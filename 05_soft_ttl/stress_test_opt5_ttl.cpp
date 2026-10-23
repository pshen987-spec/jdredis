#define OPT_CLIENT_CACHE 0
#define OPT_BIGKEY_GUARD 0
#define OPT_MGET_PIPELINE 0
#define OPT_SOFT_TTL 1
//============================================================================
// stress_test_v2.cpp
// 京东Redis "高并发 + 大Value + 热Key + 恶意APPEND" 问题复现压测程序(修订版)
//
// 相对 v1 的关键修复:
//   1) 预热数据: 40w基础数据，严格按场景全部使用 EX 1；约61GB是累计写入量。
//   2) 修复 pipeline 刷新死锁: 攒满 PIPELINE 才统一读回复, 末尾 flush 剩余;
//      (v1 在 i=0 时只追加 1 条命令就 getReply 1000 次, 主线程永久阻塞)
//   3) 热Key 增加"生命周期维护线程", 每 200ms 重置 EX 1, 保证阶段3始终命中;
//      (v1 热Key 只有 1s TTL, 1 秒后 40 个读线程全部 MISS, 压测立刻失效)
//   4) 40w数据(80%大key 200KB + 20%小key 1KB)约需>=80G内存, 148(1.5TiB)可装;
//   5) 客户端读超时: 阶段2/3用250ms观测"Command timed out"; 预热大pipeline单独
//      用20s大超时(否则250ms必然超时导致预热自断退出, 已实测复现);
//   6) 统一所有规模参数(宏), 支持 精简便/完整档 一键切换。
//
// 编译(压测机需装有 hiredis):
//   g++ -O3 -std=c++11 stress_test_opt5_ttl.cpp -o stress_test_opt5_ttl -pthread -lhiredis
// 运行:
//   ./stress_test_opt5_ttl <redis_ip> <redis_port>
//====================================================================
#include <hiredis/hiredis.h>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <algorithm>
#include <cstring>
#include <mutex>
#include <random>
#include <string>
#include <thread>
#include <vector>

using namespace std;
using std::chrono::milliseconds;
using std::chrono::microseconds;
using std::chrono::seconds;
using std::chrono::steady_clock;
using std::chrono::system_clock;
using std::chrono::duration_cast;

//------------------------- 可调规模参数 ------------------------------
static const int   WARM_KEYS     = 400000;  // 40w基础数据(须能被5整除)
static const size_t BIG_SIZE     = 200 * 1024; // 大key 200KB
static const size_t SMALL_SIZE   = 1 * 1024;   // 小key 1KB
static const int   PIPELINE      = 1000;    // 预热pipeline批量(严格按文档: 每1000条刷新一次)
static const int   WARM_TTL      = 1;        // 预热TTL=EX 1 (严格按文档 line58/266: 预热阶段也带1s强硬过期)
static const int   WRITE_TTL     = 1;       // 压测SET TTL, 模拟真实1s特性
static const int   STAGE2_SEC    = 30;      // 阶段2时长(文档: 主线程静默等待30秒)
static const int   STAGE3_SEC    = 60;      // 阶段3时长(文档: 压爆分片并持续60秒)

static const int   MGET_THREADS  = 20;      // 阶段2 MGET线程数(严格按文档: 20个)
static const int   MGET_KEYS     = 100;     // 单次MGET聚合key数(本轮统一口径100)
static const int   MGET_GAP_US   = 0;       // 文档MGET线程无间隔疯狂循环 -> 0=不限速
static const int   SET_THREADS   = 20;      // 阶段2 SET线程数(严格按文档: 20个)
static const int   SET_GAP_US    = 2000;    // SET间隔(文档: 每2ms覆盖写200KB)
static const int   HOT_THREADS   = 40;      // 阶段3热Key读线程(严格按文档: 40个)
static const int   APPEND_THREADS= 5;
static const int   APPEND_TIMES  = 5;       // 连续APPEND次数: 200KB -> 1.2MB
static const long  READ_TIMEOUT_MS      = 100;  // 网络读保护超时；保留完整回复，避免5ms时反复断线
static const long long RTT_TIMEOUT_US   = 5000; // 业务验收口径：单条命令端到端RTT>5ms
static const long  WARM_READ_TIMEOUT_MS = 20000; // 预热大pipeline专用读超时(250ms必然自断)

static const char* KEY_HOT = "test:hot:activity:global";


//--------------------- 可独立开关的优化实验 --------------------------
#ifndef OPT_CLIENT_CACHE
#define OPT_CLIENT_CACHE 0
#endif
#ifndef OPT_BIGKEY_GUARD
#define OPT_BIGKEY_GUARD 0
#endif
#ifndef OPT_MGET_PIPELINE
#define OPT_MGET_PIPELINE 0
#endif
#ifndef OPT_SOFT_TTL
#define OPT_SOFT_TTL 0
#endif

static const int LOCAL_CACHE_TTL_MS = 500;
static const size_t MAX_VALUE_SIZE = 300 * 1024;
static const int APPEND_GLOBAL_LIMIT_PER_SEC = 20;
static const int MGET_BATCH_SIZE = 10;
static const int MGET_PIPELINE_DEPTH = 8;
static const int SOFT_LOGICAL_TTL_MS = 1000;
static const int SOFT_REDIS_TTL_SEC = 10;
static const char* KEY_HOT_REFRESH_LOCK = "test:hot:activity:refresh_lock";

static atomic<long long> stat_cache_hit(0), stat_cache_miss(0), stat_cache_wait(0);
static atomic<long long> stat_append_requested(0), stat_append_rate_limited(0), stat_append_size_rejected(0);
static atomic<long long> stat_mget_logical(0), stat_mget_logical_timeout(0);
static atomic<long long> stat_soft_refresh(0), stat_soft_stale(0), stat_soft_lock_failed(0);

static mutex hot_cache_mu;
static string hot_cache_value;
static steady_clock::time_point hot_cache_expire = steady_clock::time_point::min();
static bool hot_cache_refreshing = false;

static mutex append_limit_mu;
static long long append_limit_second = -1;
static int append_tokens_used = 0;

static long long epoch_ms() {
    return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

static string make_soft_value(const string& payload) {
    return to_string(epoch_ms() + SOFT_LOGICAL_TTL_MS) + "|" + payload;
}

static bool soft_value_expired(const redisReply* r) {
    if (!r || !r->str || r->len < 2) return true;
    const char* sep = (const char*)memchr(r->str, '|', r->len);
    if (!sep) return true;
    string stamp(r->str, sep - r->str);
    return atoll(stamp.c_str()) < epoch_ms();
}

static bool allow_append_now() {
    lock_guard<mutex> lock(append_limit_mu);
    long long sec = epoch_ms() / 1000;
    if (sec != append_limit_second) {
        append_limit_second = sec;
        append_tokens_used = 0;
    }
    if (append_tokens_used >= APPEND_GLOBAL_LIMIT_PER_SEC) return false;
    ++append_tokens_used;
    return true;
}
//---------------------------------------------------------------------

// 线程放大系数 THREAD_SCALE 固定为 1(严格按文档), 不再提供命令行参数
// (该系数仅放大阶段2/3线程数, 不改预热与各命令规模; 如需更大压力改这里并重编译)
static int THREAD_SCALE = 1;
//---------------------------------------------------------------------

static atomic<bool> stop_signal(false);
static atomic<bool> phase3_signal(false);

static atomic<long long> stat_ok(0), stat_fail(0), stat_timeout(0);
static atomic<long long> stat_mget(0), stat_set(0), stat_get(0), stat_append(0);

// RTT 统计: 0=MGET 1=SET 2=GET 3=APPEND; 阶段2结束做快照, 阶段3 = 全程-快照
static const int RTT_MGET=0, RTT_SET=1, RTT_GET=2, RTT_APPEND=3;
static atomic<long long> rtt_sum[4];
static atomic<long long> rtt_n[4];
static atomic<long long> rtt_max[4];    // 全程最大(us)
static atomic<long long> ph3_max[4];    // 阶段3期间最大(us, phase3_signal 后才更新)
static long long s2_sum[4]={0}, s2_n[4]={0}, s2_max[4]={0};   // 阶段2结束快照

// key 命名与预热阶段保持一致:
//   i%5==0 -> test:data:small:<idx>   (占 1/5)
//   其它    -> test:data:normal:<idx> (占 4/5), 两侧 ID 从 0 连续递增对齐
static const int NORMAL_NUM = WARM_KEYS * 4 / 5;
static const int SMALL_NUM  = WARM_KEYS / 5;
static inline string k_normal(int i) { return "test:data:normal:" + to_string(i); }
static inline string k_small (int i) { return "test:data:small:"  + to_string(i); }
static inline int    pick_full(mt19937& g) { return uniform_int_distribution<int>(0, WARM_KEYS - 1)(g); } // 严格按文档 line146/191/236: dis(0,399999) 全区间

static redisContext* connect_redis(const string& host, int port, long read_timeout_ms) {
    struct timeval ctv = {0, 500000};          // 连接超时 500ms
    redisContext* c = redisConnectWithTimeout(host.c_str(), port, ctv);
    if (!c || c->err) {
        if (c) cerr << "[connect] " << c->errstr << endl;
        if (c) redisFree(c);
        return nullptr;
    }
    if (read_timeout_ms > 0) {
        struct timeval rtv = {read_timeout_ms / 1000, (read_timeout_ms % 1000) * 1000};
        redisSetTimeout(c, rtv);
    }
    return c;
}

// 读取并释放 n 条 pipeline 回复; 返回是否全部成功
static bool flush_replies(redisContext* c, int n) {
    for (int j = 0; j < n; ++j) {
        redisReply* r = nullptr;
        if (redisGetReply(c, (void**)&r) != REDIS_OK) {
            cerr << "[flush] " << c->errstr << endl;
            return false;
        }
        if (r) freeReplyObject(r);
    }
    return true;
}

// 统一处理一条命令的结果: 命令总耗时>RTT_TIMEOUT_US 计业务超时(命令级判定,替代"连续无字节"语义);
//   断连时重置连接以便重连; 成功时累计RTT(客户端发出->收到回复)
static inline void handle_reply(redisContext*& c, redisReply* r,
                                atomic<long long>& tag, int kind,
                                steady_clock::time_point t0) {
    if (r) {
        long long dt = (long long)duration_cast<microseconds>(steady_clock::now() - t0).count();
        if (dt > RTT_TIMEOUT_US) stat_timeout.fetch_add(1);   // 命令级超时: 总耗时>5ms 计超时
        freeReplyObject(r);
        stat_ok.fetch_add(1);
        tag.fetch_add(1);
        rtt_sum[kind].fetch_add(dt);
        rtt_n[kind].fetch_add(1);
        long long cur = rtt_max[kind].load();
        while (dt > cur && !rtt_max[kind].compare_exchange_weak(cur, dt)) {}
        if (phase3_signal.load()) {                       // 阶段3期间单独记最大
            cur = ph3_max[kind].load();
            while (dt > cur && !ph3_max[kind].compare_exchange_weak(cur, dt)) {}
        }
    } else {
        long long dt = (long long)duration_cast<microseconds>(steady_clock::now() - t0).count();
        bool socket_timeout = c && c->err == REDIS_ERR_IO &&
            (errno == EAGAIN || errno == EWOULDBLOCK || errno == ETIMEDOUT);
        if (socket_timeout || dt > RTT_TIMEOUT_US) stat_timeout.fetch_add(1);
        stat_fail.fetch_add(1);
        if (c && c->err) {
            cerr << "  [cmd] " << c->errstr << endl;
        }
        if (c) { redisFree(c); c = nullptr; }
    }
}

// 生成指定大小二进制数据
static string generate_data(size_t size) {
    string data;
    data.reserve(size);
    static const char alpha[] =
        "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    mt19937 g(random_device{}());
    uniform_int_distribution<int> d(0, (int)sizeof(alpha) - 2);
    for (size_t i = 0; i < size; ++i) data.push_back(alpha[d(g)]);
    return data;
}

static void dump_stats(const char* tag) {
    long long m = stat_mget.load(), s = stat_set.load(),
              g = stat_get.load(), a = stat_append.load(),
              ok = stat_ok.load(), f = stat_fail.load(), t = stat_timeout.load();
    cout << "[" << tag << "] MGET=" << m << " SET=" << s
         << " GET=" << g << " APPEND=" << a
         << " 成功=" << ok << " 失败=" << f << " 超时=" << t << endl;
    if (OPT_CLIENT_CACHE)
        cout << "[本地缓存] 命中=" << stat_cache_hit.load() << " 未命中=" << stat_cache_miss.load()
             << " 等待刷新=" << stat_cache_wait.load() << endl;
    if (OPT_BIGKEY_GUARD)
        cout << "[APPEND保护] 请求=" << stat_append_requested.load()
             << " 限流拒绝=" << stat_append_rate_limited.load()
             << " 大小拒绝=" << stat_append_size_rejected.load() << endl;
    if (OPT_MGET_PIPELINE)
        cout << "[MGET逻辑请求] 完成=" << stat_mget_logical.load()
             << " RTT>5ms=" << stat_mget_logical_timeout.load() << endl;
    if (OPT_SOFT_TTL)
        cout << "[逻辑TTL] 刷新=" << stat_soft_refresh.load()
             << " 返回过期旧值=" << stat_soft_stale.load()
             << " 未获得刷新锁=" << stat_soft_lock_failed.load() << endl;
}

// 打印分阶段 RTT(us): 平均/最大, n=0(该阶段无此命令)显示 -1
static void dump_rtt_ph(const char* tag, const long long sum[], const long long n[], const long long mx[]) {
    printf("[%s RTT(us)] MGET avg=%lld max=%lld | SET avg=%lld max=%lld | GET avg=%lld max=%lld | APPEND avg=%lld max=%lld\n",
        tag,
        n[RTT_MGET]  ? sum[RTT_MGET]/n[RTT_MGET]  : -1, n[RTT_MGET]  ? mx[RTT_MGET]  : -1,
        n[RTT_SET]   ? sum[RTT_SET] /n[RTT_SET]   : -1, n[RTT_SET]   ? mx[RTT_SET]   : -1,
        n[RTT_GET]   ? sum[RTT_GET] /n[RTT_GET]   : -1, n[RTT_GET]   ? mx[RTT_GET]   : -1,
        n[RTT_APPEND]? sum[RTT_APPEND]/n[RTT_APPEND]: -1, n[RTT_APPEND] ? mx[RTT_APPEND] : -1);
}

// 阶段标记: 把当前所处阶段写入Redis(供185采样按phase分桶统计CPU/网卡/指标)
//   marker=0 预热/收尾  1未用  2=阶段2  3=阶段3
static void mark_phase(const string& host, int port, const char* val) {
    redisContext* pc = connect_redis(host, port, WARM_READ_TIMEOUT_MS);
    if (!pc) return;
    redisReply* r = (redisReply*)redisCommand(
        pc, "SET %s %s", "test:phase:marker", val);
    if (r) freeReplyObject(r);
    redisFree(pc);
}

//---------------------- 阶段2: 日常 MGET 流量 --------------------------
static void read_worker_mget(string host, int port) {
    redisContext* c = connect_redis(host, port, READ_TIMEOUT_MS);
    mt19937 g(random_device{}());

    while (!stop_signal.load()) {
        if (!c) { c = connect_redis(host, port, READ_TIMEOUT_MS); if (!c) { this_thread::sleep_for(milliseconds(200)); continue; } }
#if OPT_MGET_PIPELINE
        const int total_batches = (MGET_KEYS + MGET_BATCH_SIZE - 1) / MGET_BATCH_SIZE;
        int completed = 0;
        auto logical_t0 = steady_clock::now();
        for (int first = 0; first < MGET_KEYS && c; first += MGET_BATCH_SIZE) {
            int pending = 0;
            vector<steady_clock::time_point> sent_at;
            for (; pending < MGET_PIPELINE_DEPTH && first < MGET_KEYS && c; ++pending, first += MGET_BATCH_SIZE) {
                vector<string> keys;
                vector<const char*> argv;
                vector<size_t> argvlen;
                int count = min(MGET_BATCH_SIZE, MGET_KEYS - first);
                keys.reserve(count);
                argv.push_back("MGET"); argvlen.push_back(4);
                for (int k = 0; k < count; ++k) {
                    int logical_index = first + k;
                    int idx = pick_full(g);
                    keys.push_back((logical_index % 5 == 0) ? k_small(idx) : k_normal(idx));
                    argv.push_back(keys.back().c_str());
                    argvlen.push_back(keys.back().size());
                }
                sent_at.push_back(steady_clock::now());
                if (redisAppendCommandArgv(c, (int)argv.size(), &argv[0], &argvlen[0]) != REDIS_OK) {
                    cerr << "[MGET pipeline append] " << c->errstr << endl;
                    redisFree(c); c = nullptr; break;
                }
            }
            first -= MGET_BATCH_SIZE; // 抵消内层for最后一次自增，交给外层for推进
            for (int i = 0; i < pending && c; ++i) {
                redisReply* r = nullptr;
                if (redisGetReply(c, (void**)&r) != REDIS_OK) r = nullptr;
                handle_reply(c, r, stat_mget, RTT_MGET, sent_at[i]);
                if (c) ++completed;
            }
        }
        if (completed == total_batches) {
            long long logical_us = duration_cast<microseconds>(steady_clock::now() - logical_t0).count();
            stat_mget_logical.fetch_add(1);
            if (logical_us > RTT_TIMEOUT_US) stat_mget_logical_timeout.fetch_add(1);
        }
#else
        vector<const char*> argv;
        vector<size_t> argvlen;
        vector<string> keys;
        keys.reserve(MGET_KEYS);
        argv.push_back("MGET"); argvlen.push_back(4);
        for (int k = 0; k < MGET_KEYS; ++k) {
            int idx = pick_full(g);
            keys.push_back((k % 5 == 0) ? k_small(idx) : k_normal(idx));
            argv.push_back(keys.back().c_str());
            argvlen.push_back(keys.back().size());
        }
        auto t0 = steady_clock::now();
        redisReply* r = (redisReply*)redisCommandArgv(c, (int)argv.size(), &argv[0], &argvlen[0]);
        handle_reply(c, r, stat_mget, RTT_MGET, t0);
#endif
        this_thread::sleep_for(microseconds(MGET_GAP_US));
    }
    if (c) redisFree(c);
}
//---------------- 阶段2: 日常 SET 覆盖写(带1s TTL) ----------------------
static void write_worker(string host, int port, const string& big) {
    redisContext* c = connect_redis(host, port, READ_TIMEOUT_MS);
    mt19937 g(random_device{}());

    while (!stop_signal.load()) {
        if (!c) { c = connect_redis(host, port, READ_TIMEOUT_MS); if (!c) { this_thread::sleep_for(milliseconds(200)); continue; } }

        string key = k_normal(pick_full(g));   // 严格按文档 line194-195: normal:dis(0,399999)
        auto t0 = steady_clock::now();
        redisReply* r = (redisReply*)redisCommand(
            c, "SET %s %b EX %d", key.c_str(), big.data(), big.size(), WRITE_TTL);
        handle_reply(c, r, stat_set, RTT_SET, t0);

        this_thread::sleep_for(microseconds(SET_GAP_US));
    }
    if (c) redisFree(c);
}

//---------------- 阶段3: 热Key 暴风雪读 ---------------------------------
static void hotkey_worker(string host, int port) {
    redisContext* c = connect_redis(host, port, READ_TIMEOUT_MS);
    while (!phase3_signal.load() && !stop_signal.load())
        this_thread::sleep_for(milliseconds(100));

    while (!stop_signal.load()) {
#if OPT_CLIENT_CACHE
        bool local_hit = false, refresh_owner = false;
        {
            lock_guard<mutex> lock(hot_cache_mu);
            auto now = steady_clock::now();
            if (!hot_cache_value.empty() && now < hot_cache_expire) {
                local_hit = true;
            } else if (hot_cache_refreshing) {
                if (!hot_cache_value.empty()) local_hit = true; // stale-while-revalidate
                else stat_cache_wait.fetch_add(1);
            } else {
                hot_cache_refreshing = true;
                refresh_owner = true;
            }
        }
        if (local_hit) {
            stat_cache_hit.fetch_add(1);
            this_thread::yield();
            continue;
        }
        if (!refresh_owner) {
            this_thread::sleep_for(milliseconds(1));
            continue;
        }
#endif
        if (!c) { c = connect_redis(host, port, READ_TIMEOUT_MS); if (!c) {
#if OPT_CLIENT_CACHE
            lock_guard<mutex> lock(hot_cache_mu); hot_cache_refreshing = false;
#endif
            this_thread::sleep_for(milliseconds(200)); continue; } }
        auto t0 = steady_clock::now();
        redisReply* r = (redisReply*)redisCommand(c, "GET %s", KEY_HOT);
#if OPT_SOFT_TTL
        if (r && soft_value_expired(r)) stat_soft_stale.fetch_add(1);
#endif
#if OPT_CLIENT_CACHE
        if (r && r->type == REDIS_REPLY_STRING && r->str) {
            lock_guard<mutex> lock(hot_cache_mu);
            hot_cache_value.assign(r->str, r->len);
            hot_cache_expire = steady_clock::now() + milliseconds(LOCAL_CACHE_TTL_MS);
            hot_cache_refreshing = false;
            stat_cache_miss.fetch_add(1);
        } else {
            lock_guard<mutex> lock(hot_cache_mu); hot_cache_refreshing = false;
        }
#endif
        handle_reply(c, r, stat_get, RTT_GET, t0);
    }
    if (c) redisFree(c);
}
//------------- 阶段3: 热Key 生命周期维护(让REX始终命中) ----------------
static void hotkey_refresher(string host, int port, const string& big) {
    redisContext* c = connect_redis(host, port, READ_TIMEOUT_MS);
    while (!phase3_signal.load() && !stop_signal.load())
        this_thread::sleep_for(milliseconds(100));

    while (!stop_signal.load()) {
        if (!c) { c = connect_redis(host, port, READ_TIMEOUT_MS); if (!c) { this_thread::sleep_for(milliseconds(200)); continue; } }
#if OPT_SOFT_TTL
        string token = to_string(epoch_ms()) + "-" + to_string((uintptr_t)&c);
        redisReply* lock_reply = (redisReply*)redisCommand(c, "SET %s %s NX PX 3000", KEY_HOT_REFRESH_LOCK, token.c_str());
        bool locked = lock_reply && lock_reply->type == REDIS_REPLY_STATUS;
        if (lock_reply) freeReplyObject(lock_reply);
        if (locked) {
            string soft = make_soft_value(big);
            auto t0 = steady_clock::now();
            redisReply* r = (redisReply*)redisCommand(c, "SET %s %b EX %d", KEY_HOT, soft.data(), soft.size(), SOFT_REDIS_TTL_SEC);
            handle_reply(c, r, stat_set, RTT_SET, t0);
            if (c) {
                redisReply* unlock = (redisReply*)redisCommand(c, "DEL %s", KEY_HOT_REFRESH_LOCK);
                if (unlock) freeReplyObject(unlock);
            }
            stat_soft_refresh.fetch_add(1);
        } else {
            stat_soft_lock_failed.fetch_add(1);
        }
        this_thread::sleep_for(milliseconds(SOFT_LOGICAL_TTL_MS));
#else
        auto t0 = steady_clock::now();
        string initial_hot = OPT_SOFT_TTL ? make_soft_value(big) : big;
        int hot_ttl = OPT_SOFT_TTL ? SOFT_REDIS_TTL_SEC : 1;
        redisReply* r = (redisReply*)redisCommand(c, "SET %s %b EX %d", KEY_HOT, initial_hot.data(), initial_hot.size(), hot_ttl);
        handle_reply(c, r, stat_set, RTT_SET, t0);
        this_thread::sleep_for(milliseconds(200));
#endif
    }
    if (c) redisFree(c);
}
//--------------- 阶段3: 恶意 APPEND 造超大Key (200KB->1.2MB) -----------
static void append_worker(string host, int port, const string& big) {
    redisContext* c = connect_redis(host, port, READ_TIMEOUT_MS);
    mt19937 g(random_device{}());
    while (!phase3_signal.load() && !stop_signal.load())
        this_thread::sleep_for(milliseconds(100));

    while (!stop_signal.load()) {
        if (!c) { c = connect_redis(host, port, READ_TIMEOUT_MS); if (!c) { this_thread::sleep_for(milliseconds(200)); continue; } }
        string key = k_normal(pick_full(g)); // 严格按文档 line243: normal:dis(0,399999) 全随机
        for (int a = 0; a < APPEND_TIMES && !stop_signal.load(); ++a) {
#if OPT_BIGKEY_GUARD
            stat_append_requested.fetch_add(1);
            if (!allow_append_now()) {
                stat_append_rate_limited.fetch_add(1);
                continue;
            }
            if (BIG_SIZE + big.size() > MAX_VALUE_SIZE) {
                stat_append_size_rejected.fetch_add(1);
                continue;
            }
#endif
            auto t0 = steady_clock::now();
            redisReply* r = (redisReply*)redisCommand(
                c, "APPEND %s %b", key.c_str(), big.data(), big.size());
            handle_reply(c, r, stat_append, RTT_APPEND, t0);
        }
        this_thread::sleep_for(milliseconds(100));
    }
    if (c) redisFree(c);
}

//----------------------------------------------------------------------
int main(int argc, char** argv) {
    if (argc >= 2 && string(argv[1]) == "--identify") {
        cout << "=== 优化版本: TTL逻辑过期 ===" << endl;
        cout << "MGET总聚合=100  RTT超时阈值=5ms  网络读保护超时=100ms" << endl;
        return 0;
    }
    string redis_host = "127.0.0.1";
    int    redis_port = 6379;
    if (argc >= 2) redis_host = argv[1];
    if (argc >= 3) redis_port = atoi(argv[2]);
    THREAD_SCALE = 1;   // 固定为1(严格按文档): 忽略命令行第3参数, 测试时无需设置

    for (int i = 0; i < 4; ++i) {   // 显式归零 RTT 计数(static atomic 全局数组值需初始化)
        rtt_sum[i].store(0); rtt_n[i].store(0); rtt_max[i].store(0); ph3_max[i].store(0);
    }

    cout << "=== 优化版本: TTL逻辑过期 ===" << endl;
    cout << "MGET总聚合=100  RTT超时阈值=5ms  网络读保护超时=100ms" << endl;
    cout << "  MGET总聚合=100  RTT超时阈值=5ms"
         << " cache=" << OPT_CLIENT_CACHE
         << " bigkey_guard=" << OPT_BIGKEY_GUARD
         << " mget_pipeline=" << OPT_MGET_PIPELINE
         << " soft_ttl=" << OPT_SOFT_TTL << endl;
    cout << "=== 内存预估(约) ===" << endl;
    cout << "  预热数据: " << (double)(NORMAL_NUM * BIG_SIZE) / (1024.0*1024.0*1024.0)
         << " GB (大key) + ~" << (double)((WARM_KEYS - NORMAL_NUM) * SMALL_SIZE) / (1024.0*1024.0)
         << " MB (小key)" << endl;
    cout << "  APPEND肥大Key: ~" << (double)(APPEND_THREADS * (STAGE3_SEC * 10) * APPEND_TIMES * BIG_SIZE) / (1024.0*1024.0)
         << " MB" << endl;   // 估算: 5线程*60s*每100ms一轮*5次*200KB≈3GB 全随机增长
    cout << "  要求 Redis maxmemory > 上述总和(40w数据约需>=80G, 148可用), 且 maxmemory-policy=noeviction" << endl;
    cout << "  [线程] 阶段2/3 线程数固定为严格文档规模(THREAD_SCALE=1), 无需命令行设置" << endl << endl;

    cout << "=== 准备测试数据 ===" << endl;
    string big   = generate_data(BIG_SIZE);
    string small = generate_data(SMALL_SIZE);

    // ================= 阶段1: 按原场景灌入(每个Key使用EX 1) =================
    cout << "=== 阶段1: 灌入 " << WARM_KEYS << " 个key (TTL=" << WARM_TTL << "s) ===" << endl;
    redisContext* c = connect_redis(redis_host, redis_port, WARM_READ_TIMEOUT_MS);
    if (!c) { cerr << "预热连接失败, 退出" << endl; return 1; }

    int pending = 0;
    for (int i = 0; i < WARM_KEYS; ++i) {
        const bool is_small = (i % 5 == 0);
        const string key = is_small ? k_small(i) : k_normal(i);  // 严格按文档 line271-276: ID=原索引i
        const char* val  = is_small ? small.data() : big.data();
        size_t      vlen = is_small ? small.size() : big.size();
        if (redisAppendCommand(c, "SET %s %b EX %d", key.c_str(), val, vlen, WARM_TTL) != REDIS_OK) {
            cerr << "AppendCommand失败: " << c->errstr << endl;
            break;
        }
        if (++pending >= PIPELINE) {
            if (!flush_replies(c, pending)) goto warm_fail;
            pending = 0;
        }
    }
    if (pending > 0 && !flush_replies(c, pending)) goto warm_fail;
    redisFree(c);
    cout << "=== 预热完成 ===" << endl;
    goto phase2;

warm_fail:
    redisFree(c);
    cerr << "预热失败" << endl;
    return 2;

phase2:
    // ================= 阶段2: 日常高并发(30s) =================
    mark_phase(redis_host, redis_port, "2");  // 分阶段统计: 标记阶段2开始
    const int mget_t = MGET_THREADS * THREAD_SCALE;
    const int set_t  = SET_THREADS  * THREAD_SCALE;
    cout << "=== 阶段2: " << mget_t << " x MGET(" << MGET_KEYS << "keys) + "
         << set_t << " x SET 持续 " << STAGE2_SEC << "s ===" << endl;
    vector<thread> workers;
    for (int i = 0; i < mget_t; ++i) workers.emplace_back(read_worker_mget, redis_host, redis_port);
    for (int i = 0; i < set_t; ++i)  workers.emplace_back(write_worker, redis_host, redis_port, big);

    this_thread::sleep_for(seconds(STAGE2_SEC));
    dump_stats("阶段2结束");
    for (int k = 0; k < 4; ++k) {   // 阶段2 RTT 快照 + 打印(此刻只有 MGET/SET)
        s2_sum[k]=rtt_sum[k].load(); s2_n[k]=rtt_n[k].load(); s2_max[k]=rtt_max[k].load();
    }
    dump_rtt_ph("阶段2", s2_sum, s2_n, s2_max);

    // ================= 阶段3: 注入热Key + 恶意APPEND =================
    mark_phase(redis_host, redis_port, "3");  // 分阶段统计: 标记阶段3开始
    cout << "=== 阶段3: 注入热Key + 恶意APPEND, 持续 " << STAGE3_SEC << "s ===" << endl;
    if (c = connect_redis(redis_host, redis_port, WARM_READ_TIMEOUT_MS)) {
        string initial_hot = OPT_SOFT_TTL ? make_soft_value(big) : big;
        int hot_ttl = OPT_SOFT_TTL ? SOFT_REDIS_TTL_SEC : 1;
        redisReply* r = (redisReply*)redisCommand(c, "SET %s %b EX %d", KEY_HOT, initial_hot.data(), initial_hot.size(), hot_ttl);
        if (r) freeReplyObject(r);
        redisFree(c);
    }

    for (int i = 0; i < HOT_THREADS * THREAD_SCALE; ++i)   workers.emplace_back(hotkey_worker, redis_host, redis_port);
    for (int i = 0; i < APPEND_THREADS * THREAD_SCALE; ++i) workers.emplace_back(append_worker, redis_host, redis_port, big);
    workers.emplace_back(hotkey_refresher, redis_host, redis_port, big);

    phase3_signal.store(true);
    this_thread::sleep_for(seconds(STAGE3_SEC));

    cout << "=== 停止回收 ===" << endl;
    stop_signal.store(true);
    for (auto& t : workers) if (t.joinable()) t.join();
    dump_stats("全部结束");
    long long a3[4]={0},n3[4]={0},mx3[4]={0}, aal[4]={0},nal[4]={0},mxal[4]={0};
    for (int k = 0; k < 4; ++k) {   // 阶段3 = 全程 - 阶段2快照; max3 取阶段3期间峰值
        a3[k] = rtt_sum[k].load() - s2_sum[k];
        n3[k] = rtt_n[k].load()  - s2_n[k];
        mx3[k] = ph3_max[k].load();
        aal[k] = rtt_sum[k].load();  nal[k] = rtt_n[k].load();  mxal[k] = rtt_max[k].load();
    }
    dump_rtt_ph("阶段3", a3, n3, mx3);
    dump_rtt_ph("全程", aal, nal, mxal);
    cout << "=== 测试结束 ===" << endl;
    mark_phase(redis_host, redis_port, "0");  // 分阶段统计: 复位标记
    return 0;
}