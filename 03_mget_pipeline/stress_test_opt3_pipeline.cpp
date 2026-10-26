//============================================================================
// stress_test_opt3_pipeline.cpp
// 方案B(基线口径): 把 "1条 MGET 聚合100键" 拆成 "10批 x 10条独立GET",
//                  10批进入全局共享连接池(FIFO排队), 由池内K个常驻线程/连接逐批执行.
//
// 本文件的由来(重要):
//   以 00_baseline/stress_test_v2.cpp 为基线改写, 只替换"读请求的聚合方式与下发方式".
//   基线程序与基线结果(00_baseline/20261022_195613)保持不动、不重跑;
//   本程序用同一场景口径再跑一次, 与冻结的基线数字直接对比.
//
// 与基线相同(保证场景可比):
//   预热40w key(80% 200KiB + 20% 1KiB, 全部 EX 1)、PIPELINE=1000;
//   key 选取公式(基线 stress_test_v2.cpp:210-211: 请求内位置 k%5==0 -> small,
//   否则 normal, idx 取 [0,399999] 全区间);
//   阶段2 20读+20写(SET 每2ms 覆盖 200KiB, EX 1)、阶段3 40热读+5 APPEND×5;
//   30s+60s、RTT>5ms 判定、100ms 网络读保护超时;
//   写/热Key/APPEND 的线程数与连接数不变(各1连接).
//
// 与参考实现(redis++ std::async + pool.size=20)的对应:
//   每批一个Pipeline任务, 共享一个固定大小的连接池; 本程序用K个常驻lane线程
//   (每个lane独占1条连接)实现池, 每个lane一次最多取 PIPE_DEPTH 个批,
//   避免每请求新建线程, 也避免为了在途量把连接数/线程数放大.
//
// 与基线的已知差异(2项, 报告对照时必须都计入):
//   1) 1个逻辑请求 = 10批 x 10条GET; 基线是1条MGET装100键.
//   2) 读侧连接数 = PIPE_POOL_CONNS(默认20), 每连接深度 = PIPE_DEPTH(默认10),
//      在途批上限 = K*D = 200, 在途 key 上限 = 2000; 与基线20条读连接一致.
//      同一请求的10批是否同时下发取决于队列/深度, 逻辑RTT可能包含排队等待.
//      读侧线程 = 20 owner + K lane(基线20), 多出的owner线程只做拆分/汇总/等待.
//   另: 新增 KeyGET 单命令 RTT/计数; MGET 计数保留, 语义=逻辑100键请求数;
//   新增"逻辑请求RTT>5ms"计数; 错误回复(-OOM/-MOVED等)单独计数但不改成功/失败判定;
//   APPEND 内层循环在连接被销毁后立即退出(基线此处会解引用空指针).
//
// 编译(压测机需装有 hiredis):
//   g++ -O3 -std=c++11 stress_test_opt3_pipeline.cpp -o stress_test_opt3_pipeline -pthread -lhiredis
// 运行:
//   ./stress_test_opt3_pipeline <redis_ip> <redis_port>
//============================================================================
#include <hiredis/hiredis.h>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <deque>
#include <iostream>
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
using std::chrono::duration_cast;

//------------------------- 可调规模参数(与基线完全一致) ------------------------------
static const int   WARM_KEYS     = 400000;  // 40w基础数据(须能被5整除)
static const size_t BIG_SIZE     = 200 * 1024; // 大key 200KB
static const size_t SMALL_SIZE   = 1 * 1024;   // 小key 1KB
static const int   PIPELINE      = 1000;    // 预热pipeline批量(严格按文档: 每1000条刷新一次)
static const int   WARM_TTL      = 1;        // 预热TTL=EX 1 (严格按文档 line58/266: 预热阶段也带1s强硬过期)
static const int   WRITE_TTL     = 1;       // 压测SET TTL, 模拟真实1s特性
static const int   STAGE2_SEC    = 30;      // 阶段2时长(文档: 主线程静默等待30秒)
static const int   STAGE3_SEC    = 60;      // 阶段3时长(文档: 压爆分片并持续60秒)

static const int   MGET_THREADS  = 20;      // 阶段2 读线程数(严格按文档: 20个)
static const int   MGET_KEYS     = 100;     // 单次逻辑请求聚合key数(本轮统一口径100)
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

//------------------ 方案B新增: 拆分参数 + 共享连接池 ------------------
// 100键 = 10批 x 10条GET; 10批进入同一个全局FIFO队列, 由K个常驻lane线程
// (每个lane独占1条连接)逐批取出执行. 读侧连接数固定为K, 不随批次数膨胀.
// 每连接深度 D: 每个lane一次最多取 D 个批, append D*10 条GET, 一次flush,
// 再读回 D*10 条; 20连接 x 10批 = 200批同时在途, 不需要把K加到200.
// 参考实现: std::async 每批一个任务 + 共享连接池 pool.size=20.
// K/D可用编译期参数覆盖: g++ ... -DPIPE_POOL_CONNS=20 -DPIPE_DEPTH=10 ...
#ifndef PIPE_POOL_CONNS
#define PIPE_POOL_CONNS 20
#endif
#ifndef PIPE_DEPTH
#define PIPE_DEPTH 10
#endif
static_assert(PIPE_POOL_CONNS > 0, "PIPE_POOL_CONNS 必须大于0");
static_assert(PIPE_DEPTH > 0, "PIPE_DEPTH 必须大于0");

static const int MGET_BATCH_SIZE = 10;   // 每批GET条数
static const int MGET_BATCHES    = MGET_KEYS / MGET_BATCH_SIZE;  // 批次数(=10)
static_assert(MGET_KEYS % MGET_BATCH_SIZE == 0,
              "MGET_KEYS 必须能被 MGET_BATCH_SIZE 整除");
static_assert(MGET_BATCHES * MGET_BATCH_SIZE == MGET_KEYS,
              "拆分后必须正好覆盖全部 MGET_KEYS");

// 线程放大系数 THREAD_SCALE 固定为 1(严格按文档), 不再提供命令行参数
static int THREAD_SCALE = 1;
//---------------------------------------------------------------------

static atomic<bool> stop_signal(false);
static atomic<bool> phase3_signal(false);

static atomic<long long> stat_ok(0), stat_fail(0), stat_timeout(0);
static atomic<long long> stat_mget(0), stat_set(0), stat_get(0), stat_append(0);
// 方案B新增: KeyGET命令数(已发出)、逻辑请求失败数、逻辑请求超时数、错误回复数
static atomic<long long> stat_kget(0), stat_mfail(0), stat_mto(0), stat_err(0);

// RTT 统计: 0=MGET(逻辑100键) 1=KeyGET(单键) 2=SET 3=GET 4=APPEND
static const int RTT_MGET=0, RTT_KGET=1, RTT_SET=2, RTT_GET=3, RTT_APPEND=4;
static const int RTT_KINDS=5;
static atomic<long long> rtt_sum[RTT_KINDS];
static atomic<long long> rtt_n[RTT_KINDS];
static atomic<long long> rtt_max[RTT_KINDS];    // 全程最大(us)
static atomic<long long> ph3_max[RTT_KINDS];    // 阶段3期间最大(us, phase3_signal 后才更新)
static long long s2_sum[RTT_KINDS]={0}, s2_n[RTT_KINDS]={0}, s2_max[RTT_KINDS]={0};   // 阶段2结束快照

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

static inline void update_max(atomic<long long>& slot, long long v) {
    long long cur = slot.load();
    while (v > cur && !slot.compare_exchange_weak(cur, v)) {}
}

// 统一处理一条命令的结果: 命令总耗时>RTT_TIMEOUT_US 计业务超时(命令级判定,替代"连续无字节"语义);
//   断连时重置连接以便重连; 成功时累计RTT(客户端发出->收到回复)
static inline void handle_reply(redisContext*& c, redisReply* r,
                                atomic<long long>& tag, int kind,
                                steady_clock::time_point t0) {
    if (r) {
        long long dt = (long long)duration_cast<microseconds>(steady_clock::now() - t0).count();
        if (dt > RTT_TIMEOUT_US) stat_timeout.fetch_add(1);   // 命令级超时: 总耗时>5ms 计超时
        if (r->type == REDIS_REPLY_ERROR) stat_err.fetch_add(1);  // 只计数, 不改变成功/失败判定(与基线同义)
        freeReplyObject(r);
        stat_ok.fetch_add(1);
        tag.fetch_add(1);
        rtt_sum[kind].fetch_add(dt);
        rtt_n[kind].fetch_add(1);
        update_max(rtt_max[kind], dt);
        if (phase3_signal.load()) update_max(ph3_max[kind], dt);   // 阶段3期间单独记最大
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

// hiredis 的 redisAppendCommand 只往 c->obuf 追加, 真正落 socket 发生在
// redisGetReply/redisBufferWrite 里。这里每批显式 flush, 才能让10个批不等回复
// 连续发出; 否则10批会退化成"读第1批时才写第1批"的串行.
static bool flush_obuf(redisContext* c) {
    int done = 0, guard = 0;
    while (!done) {
        if (redisBufferWrite(c, &done) != REDIS_OK) return false;
        if (!done && ++guard > 100000) return false;   // 无进展保护(阻塞socket正常不会触发)
    }
    return true;
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
    const long long m  = stat_mget.load(),   kg = stat_kget.load(),
                    s  = stat_set.load(),    g  = stat_get.load(),
                    a  = stat_append.load(), ok = stat_ok.load(),
                    f  = stat_fail.load(),   t  = stat_timeout.load(),
                    mt = stat_mto.load(),    mf = stat_mfail.load(),
                    er = stat_err.load();
    cout << "[" << tag << "] MGET=" << m << " KeyGET=" << kg << " SET=" << s
         << " GET=" << g << " APPEND=" << a
         << " 成功=" << ok << " 失败=" << f << " 超时=" << t << endl;
    cout << "[" << tag << "-逻辑请求] 完成=" << m << " 失败=" << mf
         << " 超时=" << mt << " 错误回复=" << er << endl;
}

// 打印分阶段 RTT(us): 平均/最大, n=0(该阶段无此命令)显示 -1
static void dump_rtt_ph(const char* tag, const long long sum[], const long long n[], const long long mx[]) {
    printf("[%s RTT(us)] MGET avg=%lld max=%lld | KeyGET avg=%lld max=%lld | SET avg=%lld max=%lld | GET avg=%lld max=%lld | APPEND avg=%lld max=%lld\n",
        tag,
        n[RTT_MGET]  ? sum[RTT_MGET]/n[RTT_MGET]  : -1, n[RTT_MGET]  ? mx[RTT_MGET]  : -1,
        n[RTT_KGET]  ? sum[RTT_KGET]/n[RTT_KGET]  : -1, n[RTT_KGET]  ? mx[RTT_KGET]  : -1,
        n[RTT_SET]   ? sum[RTT_SET] /n[RTT_SET]   : -1, n[RTT_SET]   ? mx[RTT_SET]   : -1,
        n[RTT_GET]   ? sum[RTT_GET] /n[RTT_GET]   : -1, n[RTT_GET]   ? mx[RTT_GET]   : -1,
        n[RTT_APPEND]? sum[RTT_APPEND]/n[RTT_APPEND]: -1, n[RTT_APPEND] ? mx[RTT_APPEND] : -1);
}

// 阶段标记: 把当前所处阶段写入Redis(供185采样按phase分桶统计CPU/网卡/指标)
//   marker=0 预热/收尾  2=阶段2  3=阶段3
static void mark_phase(const string& host, int port, const char* val) {
    redisContext* pc = connect_redis(host, port, WARM_READ_TIMEOUT_MS);
    if (!pc) return;
    redisReply* r = (redisReply*)redisCommand(
        pc, "SET %s %s", "test:phase:marker", val);
    if (r) freeReplyObject(r);
    redisFree(pc);
}

//------------ 阶段2: 日常读流量——方案B: 全局共享连接池 ----------------
// 数据流:
//   owner(20个读worker) 生成100键 -> 切10批 -> 10个BatchTask入全局FIFO队列
//   -> K个lane线程各自从队列一次取最多D批 -> append D*10条GET -> 一次flush
//   -> 读回 D*10条 -> 标记task完成 -> owner等10批全部完成 -> 结算1个逻辑请求.
// 关键: 读侧连接数固定为K(默认20), 每连接深度D(默认10), 在途批上限=K*D=200,
//       在途key上限=2000; 不需要为了在途量把连接数放大到100/150/200.
struct BatchOwner {
    mutex mu;
    condition_variable cv;
    int pending = 0;
};

struct BatchTask {
    string keys[MGET_BATCH_SIZE];
    BatchOwner* owner = nullptr;
    bool ok = false;
    int appended = 0;
    int read_cnt = 0;
};

struct PoolLane {
    redisContext* c = nullptr;      // lane独占的连接(池内一条)
    thread th;
};

static deque<BatchTask*> batch_queue;
static mutex batch_queue_mu;
static condition_variable batch_queue_cv;
static bool pool_quit = false;
static vector<PoolLane> pool_lanes;

static void pool_lane_main(PoolLane* L, string host, int port) {
    L->c = connect_redis(host, port, READ_TIMEOUT_MS);
    if (!L->c) this_thread::sleep_for(milliseconds(200));

    for (;;) {
        // 一次最多取 PIPE_DEPTH 个批: 20连接 x 10批 = 200批同时在途,
        // 覆盖 20个owner x 10批, 不需要把 K 加到 200.
        BatchTask* group[PIPE_DEPTH];
        int n = 0;
        {
            unique_lock<mutex> lk(batch_queue_mu);
            batch_queue_cv.wait(lk, []{ return pool_quit || !batch_queue.empty(); });
            if (batch_queue.empty()) {          // 只有 pool_quit 且队列空才退出
                if (pool_quit) break;
                continue;
            }
            while (n < PIPE_DEPTH && !batch_queue.empty()) {
                group[n++] = batch_queue.front();
                batch_queue.pop_front();
            }
        }

        bool send_ok = true;
        int appended = 0, read_cnt = 0;
        if (!L->c) {
            L->c = connect_redis(host, port, READ_TIMEOUT_MS);
            if (!L->c) { send_ok = false; this_thread::sleep_for(milliseconds(200)); }
        }
        if (send_ok) {
            steady_clock::time_point sent[PIPE_DEPTH * MGET_BATCH_SIZE];  // 本组每条GET的发出时刻
            for (int t = 0; t < n; ++t) {   // 先全部清零, 避免append中途失败时后续任务残留上一轮值
                group[t]->appended = 0;
                group[t]->read_cnt = 0;
                group[t]->ok = false;
            }
            for (int t = 0; t < n && send_ok; ++t) {
                for (int k = 0; k < MGET_BATCH_SIZE; ++k) {
                    sent[t * MGET_BATCH_SIZE + k] = steady_clock::now();
                    if (redisAppendCommand(L->c, "GET %s", group[t]->keys[k].c_str()) != REDIS_OK) {
                        send_ok = false;
                        break;
                    }
                    ++appended;
                    ++group[t]->appended;
                    stat_kget.fetch_add(1);                 // 已发出的GET命令数
                }
            }
            if (send_ok) send_ok = flush_obuf(L->c);        // 本组 n*10 条一次落socket

            int fail_from_task = 0;   // 读失败时: 该任务及其后续任务失败
            if (send_ok) {
                while (read_cnt < appended) {
                    redisReply* r = nullptr;
                    if (redisGetReply(L->c, (void**)&r) != REDIS_OK) {
                        send_ok = false;
                        fail_from_task = read_cnt / MGET_BATCH_SIZE;
                        break;
                    }
                    const int idx = read_cnt;
                    const long long dt = (long long)duration_cast<microseconds>(
                        steady_clock::now() - sent[idx]).count();
                    ++read_cnt;
                    if (r) {
                        if (r->type == REDIS_REPLY_ERROR) stat_err.fetch_add(1);
                        freeReplyObject(r);
                        stat_ok.fetch_add(1);
                        rtt_sum[RTT_KGET].fetch_add(dt);
                        rtt_n[RTT_KGET].fetch_add(1);
                        update_max(rtt_max[RTT_KGET], dt);
                        if (phase3_signal.load()) update_max(ph3_max[RTT_KGET], dt);
                        if (dt > RTT_TIMEOUT_US) stat_timeout.fetch_add(1);   // 单键GET命令级超时
                        ++group[idx / MGET_BATCH_SIZE]->read_cnt;
                    } else {
                        stat_fail.fetch_add(1);
                        send_ok = false;
                        fail_from_task = idx / MGET_BATCH_SIZE;
                        break;
                    }
                }
            }
            if (!send_ok) {
                stat_fail.fetch_add(appended - read_cnt);   // 已追加但未读回的命令按失败计
                if (L->c) {
                    if (L->c->err) cerr << "  [GET pipeline] " << L->c->errstr << endl;
                    redisFree(L->c);
                    L->c = nullptr;
                }
            }

            // 任务完成判定:
            //   append/flush 失败 -> 本组所有任务失败(没有完整发出/收回);
            //   读失败 -> fail_from_task 之前的任务若已收齐10条则成功, 之后失败;
            //   全部成功 -> 每个任务必须收齐10条.
            for (int t = 0; t < n; ++t) {
                bool task_ok = false;
                if (send_ok) {
                    task_ok = (group[t]->read_cnt == MGET_BATCH_SIZE);
                } else if (read_cnt > 0 && t < fail_from_task) {
                    task_ok = (group[t]->read_cnt == MGET_BATCH_SIZE);
                }
                group[t]->ok = task_ok;
            }
        } else {
            for (int t = 0; t < n; ++t) {
                group[t]->appended = 0;
                group[t]->read_cnt = 0;
                group[t]->ok = false;
            }
        }

        for (int t = 0; t < n; ++t) {
            BatchTask* task = group[t];
            lock_guard<mutex> lk(task->owner->mu);
            --task->owner->pending;
            if (task->owner->pending == 0) task->owner->cv.notify_one();
        }
    }
    if (L->c) { redisFree(L->c); L->c = nullptr; }
}

static void start_pool(const string& host, int port) {
    {
        lock_guard<mutex> lk(batch_queue_mu);
        pool_quit = false;
        batch_queue.clear();
    }
    pool_lanes.clear();
    pool_lanes.resize(PIPE_POOL_CONNS);
    for (int i = 0; i < PIPE_POOL_CONNS; ++i)
        pool_lanes[i].th = thread(pool_lane_main, &pool_lanes[i], host, port);
}

static void stop_pool() {
    {
        lock_guard<mutex> lk(batch_queue_mu);
        pool_quit = true;
    }
    batch_queue_cv.notify_all();
    for (auto& L : pool_lanes) if (L.th.joinable()) L.th.join();
    pool_lanes.clear();
}

static void read_worker_pipeline() {
    mt19937 g(random_device{}());
    vector<string> keys(MGET_KEYS);                     // 预先按基线规则生成100键

    while (!stop_signal.load()) {
        // 与基线 stress_test_v2.cpp:210-211 逐字一致的选取规则(按请求内位置 pos%5)
        for (int pos = 0; pos < MGET_KEYS; ++pos) {
            const int idx = pick_full(g);
            keys[pos] = (pos % 5 == 0) ? k_small(idx) : k_normal(idx);
        }

        // --- 拆: 100键 -> 10批, 准备10个BatchTask(客户端侧开销, 不计入逻辑RTT) ---
        BatchOwner owner;
        BatchTask tasks[MGET_BATCHES];
        {
            lock_guard<mutex> lk(owner.mu);
            owner.pending = MGET_BATCHES;
        }
        for (int b = 0; b < MGET_BATCHES; ++b) {
            for (int k = 0; k < MGET_BATCH_SIZE; ++k)
                tasks[b].keys[k] = keys[b * MGET_BATCH_SIZE + k];
            tasks[b].owner = &owner;
        }

        auto t_req = steady_clock::now();               // 逻辑请求起点(=基线MGET的t0, 入队前)

        {
            lock_guard<mutex> lk(batch_queue_mu);
            for (int b = 0; b < MGET_BATCHES; ++b)
                batch_queue.push_back(&tasks[b]);
        }
        const int notify_n = (MGET_BATCHES + PIPE_DEPTH - 1) / PIPE_DEPTH;
        for (int i = 0; i < notify_n; ++i)
            batch_queue_cv.notify_one();                // D=10时只需唤醒1个lane取走10批

        // --- 收: 等10批全部完成(由池内lane线程逐批执行, 可能排队) ---
        bool req_ok = true;
        int total_read = 0;
        {
            unique_lock<mutex> lk(owner.mu);
            owner.cv.wait(lk, [&owner]{ return owner.pending == 0; });
            for (int b = 0; b < MGET_BATCHES; ++b) {
                total_read += tasks[b].read_cnt;
                if (!tasks[b].ok) req_ok = false;
            }
        }
        if (total_read != MGET_KEYS) req_ok = false;    // 未凑齐100条回复

        // --- 逻辑请求: 与基线 MGET 同义(100键整体端到端; 含批次排队时间) ---
        const long long ldt =
            (long long)duration_cast<microseconds>(steady_clock::now() - t_req).count();
        if (req_ok) {
            stat_mget.fetch_add(1);
            rtt_sum[RTT_MGET].fetch_add(ldt);
            rtt_n[RTT_MGET].fetch_add(1);
            update_max(rtt_max[RTT_MGET], ldt);
            if (phase3_signal.load()) update_max(ph3_max[RTT_MGET], ldt);
            if (ldt > RTT_TIMEOUT_US) stat_mto.fetch_add(1);          // 逻辑请求RTT>5ms
        } else {
            stat_mfail.fetch_add(1);                                  // 逻辑请求失败(不计RTT样本)
            if (ldt > RTT_TIMEOUT_US) stat_mto.fetch_add(1);
        }

        this_thread::sleep_for(microseconds(MGET_GAP_US));
    }
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
        if (!c) { c = connect_redis(host, port, READ_TIMEOUT_MS); if (!c) { this_thread::sleep_for(milliseconds(200)); continue; } }
        auto t0 = steady_clock::now();
        redisReply* r = (redisReply*)redisCommand(c, "GET %s", KEY_HOT);
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
        redisReply* r = (redisReply*)redisCommand(
            c, "SET %s %b EX 1", KEY_HOT, big.data(), big.size());
        handle_reply(c, r, stat_set, RTT_SET, steady_clock::now());
        this_thread::sleep_for(milliseconds(200));   // EX 1 内持续刷新
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
            auto t0 = steady_clock::now();
            redisReply* r = (redisReply*)redisCommand(
                c, "APPEND %s %b", key.c_str(), big.data(), big.size());
            handle_reply(c, r, stat_append, RTT_APPEND, t0);
            if (!c) break;   // 断连时handle_reply已销毁连接; 必须退出内层循环, 否则下一条命令解引用空指针
        }
        this_thread::sleep_for(milliseconds(100));
    }
    if (c) redisFree(c);
}

//----------------------------------------------------------------------
int main(int argc, char** argv) {
    if (argc >= 2 && string(argv[1]) == "--identify") {
        cout << "=== 测试版本: 方案B (MGET拆10批x10 GET Pipeline) ===" << endl;
        cout << "MGET总聚合=100  RTT超时阈值=5ms  网络读保护超时=100ms" << endl;
        cout << "实现方式: 方案B 每请求100xGET(10批x10, 全局共享连接池, 批次FIFO排队)" << endl;
        return 0;
    }
    cout << "=== 测试版本: 方案B (MGET拆10批x10 GET Pipeline) ===" << endl;
    cout << "MGET总聚合=100  RTT超时阈值=5ms  网络读保护超时=100ms" << endl;
    cout << "实现方式: 方案B 每请求100xGET(10批x10, 全局共享连接池, 批次FIFO排队)" << endl;
    cout << "[运行配置] 预热TTL=" << WARM_TTL << "s 写TTL=" << WRITE_TTL
         << "s 大key=" << (BIG_SIZE/1024) << "KiB 小key=" << (SMALL_SIZE/1024) << "KiB"
         << " 预热key=" << WARM_KEYS << " 逻辑请求=" << MGET_KEYS << "键"
         << " 拆分=" << MGET_BATCHES << "批x" << MGET_BATCH_SIZE << "GET"
         << " 连接池=" << PIPE_POOL_CONNS << "(读侧连接固定)"
         << " 深度=" << PIPE_DEPTH << "(每连接在途批)"
         << " 读侧线程=" << (MGET_THREADS * THREAD_SCALE) << "owner+"
         << PIPE_POOL_CONNS << "lane"
         << " 线程=" << MGET_THREADS << "读/" << SET_THREADS << "写/"
         << HOT_THREADS << "热/" << APPEND_THREADS << "APPEND"
         << " 阶段=" << STAGE2_SEC << "s+" << STAGE3_SEC << "s" << endl;
    cout << "[变量] 读侧连接=" << PIPE_POOL_CONNS << "(全局共享池, 基线="
         << (MGET_THREADS * THREAD_SCALE) << "); 每连接深度=" << PIPE_DEPTH
         << " 在途批上限=" << (PIPE_POOL_CONNS * PIPE_DEPTH)
         << " 在途key上限=" << (PIPE_POOL_CONNS * PIPE_DEPTH * MGET_BATCH_SIZE)
         << "; 同一请求的10批可能排队执行, 逻辑RTT含排队等待(共享池的代价)" << endl;
    cout << "[口径] MGET=逻辑100键请求数; KeyGET=其内部的单键GET命令数(逐条计RTT/超时);"
            " 成功/失败/超时=命令级; 逻辑请求行单独报100键整体RTT>5ms的个数" << endl;

    string redis_host = "127.0.0.1";
    int    redis_port = 6379;
    if (argc >= 2) redis_host = argv[1];
    if (argc >= 3) redis_port = atoi(argv[2]);
    THREAD_SCALE = 1;   // 固定为1(严格按文档): 忽略命令行第3参数

    for (int i = 0; i < RTT_KINDS; ++i) {   // 显式归零 RTT 计数(static atomic 全局数组值需初始化)
        rtt_sum[i].store(0); rtt_n[i].store(0); rtt_max[i].store(0); ph3_max[i].store(0);
    }

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
    start_pool(redis_host, redis_port);
    cout << "[连接池] 读侧 " << PIPE_POOL_CONNS << " 条连接/线程已启动 (基线"
         << (MGET_THREADS * THREAD_SCALE) << "条)" << endl;
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
    cout << "=== 阶段2: " << mget_t << " x 逻辑请求(" << MGET_KEYS << "键="
         << MGET_BATCHES << "批x" << MGET_BATCH_SIZE << "GET, 共享池"
         << PIPE_POOL_CONNS << "连接) + "
         << set_t << " x SET 持续 " << STAGE2_SEC << "s ===" << endl;
    vector<thread> workers;
    for (int i = 0; i < mget_t; ++i) workers.emplace_back(read_worker_pipeline);
    for (int i = 0; i < set_t; ++i)  workers.emplace_back(write_worker, redis_host, redis_port, big);

    this_thread::sleep_for(seconds(STAGE2_SEC));
    dump_stats("阶段2结束");
    for (int k = 0; k < RTT_KINDS; ++k) {   // 阶段2 RTT 快照 + 打印(此刻只有 MGET/KeyGET/SET)
        s2_sum[k]=rtt_sum[k].load(); s2_n[k]=rtt_n[k].load(); s2_max[k]=rtt_max[k].load();
    }
    dump_rtt_ph("阶段2", s2_sum, s2_n, s2_max);

    // ================= 阶段3: 注入热Key + 恶意APPEND =================
    mark_phase(redis_host, redis_port, "3");  // 分阶段统计: 标记阶段3开始
    cout << "=== 阶段3: 注入热Key + 恶意APPEND, 持续 " << STAGE3_SEC << "s ===" << endl;
    if (c = connect_redis(redis_host, redis_port, WARM_READ_TIMEOUT_MS)) {
        redisReply* r = (redisReply*)redisCommand(c, "SET %s %b EX 1", KEY_HOT, big.data(), big.size());
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
    stop_pool();
    dump_stats("全部结束");
    long long a3[RTT_KINDS]={0},n3[RTT_KINDS]={0},mx3[RTT_KINDS]={0}, aal[RTT_KINDS]={0},nal[RTT_KINDS]={0},mxal[RTT_KINDS]={0};
    for (int k = 0; k < RTT_KINDS; ++k) {   // 阶段3 = 全程 - 阶段2快照; max3 取阶段3期间峰值
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
