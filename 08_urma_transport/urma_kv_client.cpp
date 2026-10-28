//============================================================================
// urma_kv_client.cpp —— 方案4 URMA 客户端库实现
//
// 数据路径(与 urma_proto.h / Redis 侧 Agent 对齐):
//   MGET: 1 条命令(WRITE_IMM 到服务端命令环) -> 服务端回复描述符(WRITE_IMM 到回复环)
//         -> 客户端对每个 HIT 发单边 READ(读 slot_hdr+value) -> 校验 version/expire
//   SET : 单边 WRITE 到服务端授予的写槽(lease) -> 发 SET 提交命令
//   APPEND: 同 SET，opcode=APPEND，服务端在槽尾原地追加或扩容
//
// 注意: 本文件依赖 liburma.so 与 /usr/include/ub/umdk/urma 头文件；
//       端到端运行还需要 Redis 侧 Agent(见 08 目录操作手册 P3)。
//============================================================================
#include "urma_kv_client.h"
#include "urma_proto.h"

#include <urma_api.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <mutex>
#include <thread>
#include <vector>

namespace {

constexpr size_t kArenaSize = 8ULL << 30;   // 8GB 共享注册区
constexpr uint32_t kToken    = 0x5A5A1234u;
constexpr int kPollBatch     = 32;

struct Env {
    urma_context_t*   ctx = nullptr;
    urma_target_seg_t* seg = nullptr;
    char*             arena = nullptr;
    size_t            size = 0;
    std::atomic<size_t> off{0};
    bool ok = false;
};

Env g_env;
std::once_flag g_env_once;

bool env_init(const char* dev_name)
{
    std::call_once(g_env_once, [&]() {
        urma_init_attr_t init_attr;
        memset(&init_attr, 0, sizeof(init_attr));
        if (urma_init(&init_attr) != URMA_SUCCESS) {
            fprintf(stderr, "[urma] urma_init failed\n");
            return;
        }

        urma_device_t* dev = urma_get_device_by_name(const_cast<char*>(dev_name));
        if (dev == nullptr) {
            fprintf(stderr, "[urma] device %s not found\n", dev_name);
            return;
        }

        uint32_t eid_cnt = 0;
        urma_eid_info_t* eids = urma_get_eid_list(dev, &eid_cnt);
        if (eids == nullptr || eid_cnt == 0) {
            fprintf(stderr, "[urma] no eid on %s\n", dev_name);
            return;
        }
        uint32_t eid_index = eids[0].eid_index;
        urma_free_eid_list(eids);

        g_env.ctx = urma_create_context(dev, eid_index);
        if (g_env.ctx == nullptr) {
            fprintf(stderr, "[urma] create_context failed\n");
            return;
        }

        g_env.size = kArenaSize;
        g_env.arena = static_cast<char*>(mmap(nullptr, g_env.size, PROT_READ | PROT_WRITE,
                                              MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB, -1, 0));
        if (g_env.arena == MAP_FAILED) {
            g_env.arena = static_cast<char*>(mmap(nullptr, g_env.size, PROT_READ | PROT_WRITE,
                                                  MAP_PRIVATE | MAP_ANONYMOUS, -1, 0));
        }
        if (g_env.arena == MAP_FAILED) {
            fprintf(stderr, "[urma] arena mmap failed\n");
            g_env.arena = nullptr;
            return;
        }

        urma_reg_seg_flag_t seg_flag;
        memset(&seg_flag, 0, sizeof(seg_flag));
        seg_flag.bs.token_policy = URMA_TOKEN_NONE;
        seg_flag.bs.cacheable    = URMA_NON_CACHEABLE;
        seg_flag.bs.access       = URMA_ACCESS_READ | URMA_ACCESS_WRITE | URMA_ACCESS_ATOMIC;

        urma_seg_cfg_t seg_cfg;
        memset(&seg_cfg, 0, sizeof(seg_cfg));
        seg_cfg.va          = reinterpret_cast<uint64_t>(g_env.arena);
        seg_cfg.len         = g_env.size;
        seg_cfg.token_value.token = kToken;
        seg_cfg.flag        = seg_flag;

        g_env.seg = urma_register_seg(g_env.ctx, &seg_cfg);
        if (g_env.seg == nullptr) {
            fprintf(stderr, "[urma] register_seg failed\n");
            return;
        }
        g_env.ok = true;
    });
    return g_env.ok;
}

void* env_alloc(size_t len)
{
    size_t cur = g_env.off.fetch_add(len, std::memory_order_relaxed);
    if (cur + len > g_env.size) {
        return nullptr;
    }
    return g_env.arena + cur;
}

bool send_all(int fd, const void* buf, size_t len)
{
    const char* p = static_cast<const char*>(buf);
    while (len > 0) {
        ssize_t n = ::send(fd, p, len, 0);
        if (n <= 0) {
            return false;
        }
        p += n;
        len -= static_cast<size_t>(n);
    }
    return true;
}

bool recv_all(int fd, void* buf, size_t len)
{
    char* p = static_cast<char*>(buf);
    while (len > 0) {
        ssize_t n = ::recv(fd, p, len, 0);
        if (n <= 0) {
            return false;
        }
        p += n;
        len -= static_cast<size_t>(n);
    }
    return true;
}

uint64_t now_ms()
{
    using namespace std::chrono;
    return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

} // namespace

struct UrmaKV::Impl {
    urma_jfce_t*          jfce = nullptr;
    urma_jfc_t*           jfc = nullptr;
    urma_jfr_t*           jfr = nullptr;
    urma_jetty_t*         jetty = nullptr;
    urma_target_jetty_t*  tjetty = nullptr;
    urma_target_seg_t*    srv_seg = nullptr;
    int                   fd = -1;

    urp_hello_t srv;
    char*  reply_ring = nullptr;
    char*  cmd_staging = nullptr;
    char*  read_pool = nullptr;
    size_t read_capacity = 0;

    uint64_t req_seq = 0;
    uint32_t lease_seq = 0;
    UrmaKVStats stats;

    std::vector<urma_jfs_wr_t> wrs;
    std::vector<urma_sge_t>    sges;

    struct ReadReq {
        uint64_t remote_ubva;
        uint32_t len;
        char*    local;
        uint32_t idx;
    };
    std::vector<ReadReq> reads;
};

UrmaKV::UrmaKV() : impl_(new Impl()) {}

UrmaKV::~UrmaKV()
{
    destroy();
    delete impl_;
    impl_ = nullptr;
}

const UrmaKVStats& UrmaKV::stats() const
{
    return impl_->stats;
}

UrmaKV* UrmaKV::create(const char* server_ip, int ctrl_port, const char* dev_name,
                       size_t read_capacity)
{
    if (!env_init(dev_name)) {
        return nullptr;
    }

    UrmaKV* kv = new UrmaKV();
    Impl* s = kv->impl_;

    s->jfce = urma_create_jfce(g_env.ctx);
    if (s->jfce == nullptr) {
        fprintf(stderr, "[urma] create_jfce failed\n");
        delete kv;
        return nullptr;
    }

    urma_jfc_cfg_t jfc_cfg;
    memset(&jfc_cfg, 0, sizeof(jfc_cfg));
    jfc_cfg.depth    = 4096;
    jfc_cfg.jfce     = s->jfce;
    jfc_cfg.user_ctx = reinterpret_cast<uint64_t>(s);
    s->jfc = urma_create_jfc(g_env.ctx, &jfc_cfg);
    if (s->jfc == nullptr) {
        fprintf(stderr, "[urma] create_jfc failed\n");
        delete kv;
        return nullptr;
    }

    urma_jfr_cfg_t jfr_cfg;
    memset(&jfr_cfg, 0, sizeof(jfr_cfg));
    jfr_cfg.depth         = 2048;
    jfr_cfg.trans_mode    = URMA_TM_RC;
    jfr_cfg.max_sge       = 1;
    jfr_cfg.min_rnr_timer = URMA_TYPICAL_MIN_RNR_TIMER;
    jfr_cfg.jfc           = s->jfc;
    jfr_cfg.token_value.token = kToken;
    s->jfr = urma_create_jfr(g_env.ctx, &jfr_cfg);
    if (s->jfr == nullptr) {
        fprintf(stderr, "[urma] create_jfr failed\n");
        delete kv;
        return nullptr;
    }

    urma_jfs_cfg_t jfs_cfg;
    memset(&jfs_cfg, 0, sizeof(jfs_cfg));
    jfs_cfg.depth         = 4096;
    jfs_cfg.trans_mode    = URMA_TM_RC;
    jfs_cfg.priority      = URMA_MAX_PRIORITY;
    jfs_cfg.max_sge       = 2;
    jfs_cfg.max_inline_data = 0;
    jfs_cfg.rnr_retry     = URMA_TYPICAL_RNR_RETRY;
    jfs_cfg.err_timeout   = URMA_TYPICAL_ERR_TIMEOUT;
    jfs_cfg.jfc           = s->jfc;

    urma_jetty_cfg_t jetty_cfg;
    memset(&jetty_cfg, 0, sizeof(jetty_cfg));
    jetty_cfg.flag.bs.share_jfr = 1;
    jetty_cfg.jfs_cfg           = jfs_cfg;
    jetty_cfg.shared.jfr        = s->jfr;
    s->jetty = urma_create_jetty(g_env.ctx, &jetty_cfg);
    if (s->jetty == nullptr) {
        fprintf(stderr, "[urma] create_jetty failed\n");
        delete kv;
        return nullptr;
    }

    s->reply_ring = static_cast<char*>(env_alloc(URP_RING_SLOTS * URP_RSP_SLOT_SZ));
    s->cmd_staging = static_cast<char*>(env_alloc(URP_CMD_SLOT_SZ));
    s->read_capacity = read_capacity;
    s->read_pool = static_cast<char*>(env_alloc(read_capacity));
    if (s->reply_ring == nullptr || s->cmd_staging == nullptr || s->read_pool == nullptr) {
        fprintf(stderr, "[urma] local arena exhausted\n");
        delete kv;
        return nullptr;
    }

    /* 带外 TCP 握手 */
    s->fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (s->fd < 0) {
        fprintf(stderr, "[urma] control socket failed\n");
        delete kv;
        return nullptr;
    }
    struct timeval tv;
    tv.tv_sec = 3;
    tv.tv_usec = 0;
    setsockopt(s->fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
    setsockopt(s->fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port   = htons(static_cast<uint16_t>(ctrl_port));
    if (inet_pton(AF_INET, server_ip, &addr.sin_addr) != 1 ||
        ::connect(s->fd, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) != 0) {
        fprintf(stderr, "[urma] control connect %s:%d failed\n", server_ip, ctrl_port);
        delete kv;
        return nullptr;
    }

    urp_hello_t hello;
    memset(&hello, 0, sizeof(hello));
    hello.magic       = URP_MAGIC;
    hello.version     = URP_VERSION;
    hello.role        = URP_ROLE_CLIENT;
    memcpy(hello.eid, &s->jetty->jetty_id.eid, sizeof(hello.eid));
    hello.uasid       = s->jetty->jetty_id.uasid;
    hello.jetty_id    = s->jetty->jetty_id.id;
    hello.seg_va      = g_env.seg->seg.ubva.va;
    hello.seg_len     = g_env.seg->seg.len;
    hello.seg_attr    = g_env.seg->seg.attr.value;
    hello.token_id    = g_env.seg->seg.token_id;
    hello.token_value = kToken;
    hello.ring_off    = static_cast<uint64_t>(s->reply_ring - g_env.arena);
    hello.ring_slots  = URP_RING_SLOTS;
    hello.ring_slot_size = URP_RSP_SLOT_SZ;

    if (!send_all(s->fd, &hello, sizeof(hello)) ||
        !recv_all(s->fd, &s->srv, sizeof(s->srv)) ||
        s->srv.magic != URP_MAGIC || s->srv.role != URP_ROLE_SERVER) {
        fprintf(stderr, "[urma] handshake failed\n");
        delete kv;
        return nullptr;
    }

    /* 导入服务端 Arena 与 jetty */
    urma_seg_t rseg;
    memset(&rseg, 0, sizeof(rseg));
    memcpy(&rseg.ubva.eid, s->srv.eid, sizeof(s->srv.eid));
    rseg.ubva.uasid = s->srv.uasid;
    rseg.ubva.va    = s->srv.seg_va;
    rseg.len        = s->srv.seg_len;
    rseg.attr.value = s->srv.seg_attr;
    rseg.token_id   = s->srv.token_id;

    urma_token_t token;
    token.token = s->srv.token_value;

    urma_import_seg_flag_t import_flag;
    memset(&import_flag, 0, sizeof(import_flag));
    import_flag.bs.cacheable = URMA_NON_CACHEABLE;
    import_flag.bs.access    = URMA_ACCESS_READ | URMA_ACCESS_WRITE | URMA_ACCESS_ATOMIC;
    import_flag.bs.mapping   = URMA_SEG_NOMAP;
    s->srv_seg = urma_import_seg(g_env.ctx, &rseg, &token, 0, import_flag);
    if (s->srv_seg == nullptr) {
        fprintf(stderr, "[urma] import_seg failed\n");
        delete kv;
        return nullptr;
    }

    urma_rjetty_t rjetty;
    memset(&rjetty, 0, sizeof(rjetty));
    memcpy(&rjetty.jetty_id.eid, s->srv.eid, sizeof(s->srv.eid));
    rjetty.jetty_id.uasid = s->srv.uasid;
    rjetty.jetty_id.id    = s->srv.jetty_id;
    rjetty.trans_mode     = URMA_TM_RC;
    rjetty.type           = URMA_JETTY;
    rjetty.tp_type        = URMA_RTP;
    rjetty.policy         = URMA_JETTY_GRP_POLICY_RR;

    s->tjetty = urma_import_jetty(g_env.ctx, &rjetty, &token);
    if (s->tjetty == nullptr) {
        fprintf(stderr, "[urma] import_jetty failed\n");
        delete kv;
        return nullptr;
    }
    if (urma_bind_jetty(s->jetty, s->tjetty) != URMA_SUCCESS) {
        fprintf(stderr, "[urma] bind_jetty failed\n");
        delete kv;
        return nullptr;
    }

    return kv;
}

void UrmaKV::destroy()
{
    if (impl_ == nullptr) {
        return;
    }
    Impl* s = impl_;
    if (s->fd >= 0) {
        ::close(s->fd);
        s->fd = -1;
    }
    if (s->tjetty != nullptr) {
        urma_unbind_jetty(s->jetty);
        urma_unimport_jetty(s->tjetty);
        s->tjetty = nullptr;
    }
    if (s->srv_seg != nullptr) {
        urma_unimport_seg(s->srv_seg);
        s->srv_seg = nullptr;
    }
    if (s->jetty != nullptr) {
        urma_delete_jetty(s->jetty);
        s->jetty = nullptr;
    }
    if (s->jfr != nullptr) {
        urma_delete_jfr(s->jfr);
        s->jfr = nullptr;
    }
    if (s->jfc != nullptr) {
        urma_delete_jfc(s->jfc);
        s->jfc = nullptr;
    }
    if (s->jfce != nullptr) {
        urma_delete_jfce(s->jfce);
        s->jfce = nullptr;
    }
}

/* ------------------------------ 数据面原语 ------------------------------ */

static bool post_write(UrmaKV::Impl* s, uint64_t dst_ubva, const void* src, uint32_t len,
                       bool with_imm, uint64_t imm, uint64_t user_ctx)
{
    urma_sge_t src_sge;
    memset(&src_sge, 0, sizeof(src_sge));
    src_sge.addr = reinterpret_cast<uint64_t>(src);
    src_sge.len  = len;
    src_sge.tseg = g_env.seg;

    urma_sge_t dst_sge;
    memset(&dst_sge, 0, sizeof(dst_sge));
    dst_sge.addr = dst_ubva;
    dst_sge.len  = len;
    dst_sge.tseg = s->srv_seg;

    urma_sg_t src_sg;
    src_sg.sge = &src_sge;
    src_sg.num_sge = 1;
    urma_sg_t dst_sg;
    dst_sg.sge = &dst_sge;
    dst_sg.num_sge = 1;

    urma_jfs_wr_t wr;
    memset(&wr, 0, sizeof(wr));
    wr.opcode = with_imm ? URMA_OPC_WRITE_IMM : URMA_OPC_WRITE;
    wr.flag.bs.complete_enable = 1;
    wr.tjetty  = s->tjetty;
    wr.user_ctx = user_ctx;
    wr.rw.src  = src_sg;
    wr.rw.dst  = dst_sg;
    if (with_imm) {
        wr.rw.notify_data = imm;
    }

    urma_jfs_wr_t* bad = nullptr;
    if (urma_post_jetty_send_wr(s->jetty, &wr, &bad) != URMA_SUCCESS) {
        s->stats.cr_err++;
        return false;
    }
    if (with_imm) {
        s->stats.cmd_wr++;
    } else {
        s->stats.lease_wr++;
    }
    return true;
}

static bool post_reads(UrmaKV::Impl* s, uint64_t req_id)
{
    size_t n = s->reads.size();
    if (n == 0) {
        return true;
    }
    s->wrs.resize(n);
    s->sges.resize(n * 2);

    for (size_t i = 0; i < n; ++i) {
        const auto& r = s->reads[i];
        urma_sge_t* rs = &s->sges[i * 2];
        urma_sge_t* rd = &s->sges[i * 2 + 1];
        memset(rs, 0, sizeof(*rs));
        memset(rd, 0, sizeof(*rd));
        rs->addr = r.remote_ubva;
        rs->len  = r.len;
        rs->tseg = s->srv_seg;
        rd->addr = reinterpret_cast<uint64_t>(r.local);
        rd->len  = r.len;
        rd->tseg = g_env.seg;

        urma_jfs_wr_t& wr = s->wrs[i];
        memset(&wr, 0, sizeof(wr));
        wr.opcode = URMA_OPC_READ;
        wr.flag.bs.complete_enable = 1;
        wr.tjetty = s->tjetty;
        wr.user_ctx = (req_id << 16) | r.idx;
        wr.rw.src.sge = rs;
        wr.rw.src.num_sge = 1;
        wr.rw.dst.sge = rd;
        wr.rw.dst.num_sge = 1;
        wr.next = (i + 1 < n) ? &s->wrs[i + 1] : nullptr;
    }

    urma_jfs_wr_t* bad = nullptr;
    if (urma_post_jetty_send_wr(s->jetty, &s->wrs[0], &bad) != URMA_SUCCESS) {
        s->stats.cr_err++;
        return false;
    }
    s->stats.read_wr += n;
    for (size_t i = 0; i < n; ++i) {
        s->stats.read_bytes += s->reads[i].len;
    }
    return true;
}

static bool wait_reply(UrmaKV::Impl* s, uint64_t req_id, int timeout_ms)
{
    using namespace std::chrono;
    auto deadline = steady_clock::now() + milliseconds(timeout_ms);
    urma_cr_t crs[kPollBatch];

    while (steady_clock::now() < deadline) {
        int n = urma_poll_jfc(s->jfc, kPollBatch, crs);
        for (int i = 0; i < n; ++i) {
            if (crs[i].status != URMA_CR_SUCCESS) {
                s->stats.cr_err++;
                continue;
            }
            if (crs[i].flag.bs.s_r && crs[i].imm_data == req_id) {
                s->stats.cmd_cr++;
                return true;
            }
        }
        if (n == 0) {
            std::this_thread::yield();
        }
    }
    s->stats.timeouts++;
    return false;
}

static bool wait_reads(UrmaKV::Impl* s, uint64_t req_id, size_t nread, int timeout_ms)
{
    using namespace std::chrono;
    auto deadline = steady_clock::now() + milliseconds(timeout_ms);
    urma_cr_t crs[kPollBatch];
    size_t done = 0;

    while (done < nread && steady_clock::now() < deadline) {
        int n = urma_poll_jfc(s->jfc, kPollBatch, crs);
        for (int i = 0; i < n; ++i) {
            if (crs[i].status != URMA_CR_SUCCESS) {
                s->stats.cr_err++;
                continue;
            }
            if (!crs[i].flag.bs.s_r && (crs[i].user_ctx >> 16) == req_id) {
                ++done;
            }
        }
        if (n == 0) {
            std::this_thread::yield();
        }
    }
    s->stats.read_cr += done;
    if (done < nread) {
        s->stats.timeouts++;
        return false;
    }
    return true;
}

/* ------------------------------ 命令构造 ------------------------------ */

static size_t build_cmd(UrmaKV::Impl* s, uint16_t opcode, const std::vector<std::string>& keys,
                        uint32_t ttl_ms, uint32_t payload_len, uint32_t lease_id,
                        uint64_t req_id, std::vector<char>& out)
{
    size_t key_bytes = 0;
    for (size_t i = 0; i < keys.size(); ++i) {
        key_bytes += 2 + keys[i].size();
    }
    out.resize(sizeof(urp_cmd_hdr_t) + key_bytes);

    urp_cmd_hdr_t* h = reinterpret_cast<urp_cmd_hdr_t*>(out.data());
    memset(h, 0, sizeof(*h));
    h->magic      = URP_MAGIC;
    h->version    = URP_VERSION;
    h->opcode     = opcode;
    h->key_count  = static_cast<uint32_t>(keys.size());
    h->ttl_ms     = ttl_ms;
    h->payload_len = payload_len;
    h->lease_id   = lease_id;
    h->req_id     = req_id;
    h->key_bytes  = static_cast<uint32_t>(key_bytes);

    char* p = out.data() + sizeof(urp_cmd_hdr_t);
    for (size_t i = 0; i < keys.size(); ++i) {
        uint16_t kl = static_cast<uint16_t>(keys[i].size());
        memcpy(p, &kl, sizeof(kl));
        p += sizeof(kl);
        memcpy(p, keys[i].data(), kl);
        p += kl;
    }
    return out.size();
}

bool UrmaKV::mget(const std::vector<std::string>& keys, std::vector<UrmaKVItem>* out,
                  bool want_values)
{
    Impl* s = impl_;
    if (s == nullptr || keys.empty() || keys.size() > URP_MAX_KEYS) {
        return false;
    }

    uint64_t req_id = ++s->req_seq;
    uint32_t slot = static_cast<uint32_t>(req_id % URP_RING_SLOTS);
    std::vector<char> cmd;
    size_t cmd_len = build_cmd(s, keys.size() == 1 ? URP_OP_GET : URP_OP_MGET,
                               keys, 0, 0, 0, req_id, cmd);

    uint64_t cmd_ubva = s->srv.seg_va + s->srv.cmd_ring_off +
                        static_cast<uint64_t>(slot) * URP_CMD_SLOT_SZ;
    if (!post_write(s, cmd_ubva, cmd.data(), static_cast<uint32_t>(cmd_len),
                    true, req_id, ~0ULL)) {
        return false;
    }
    if (!wait_reply(s, req_id, 100)) {
        return false;
    }

    char* rsp = s->reply_ring + static_cast<size_t>(slot) * URP_RSP_SLOT_SZ;
    urp_rsp_hdr_t* rh = reinterpret_cast<urp_rsp_hdr_t*>(rsp);
    if (rh->magic != URP_MAGIC || rh->req_id != req_id || rh->status != 0 ||
        rh->item_count != keys.size()) {
        s->stats.cr_err++;
        return false;
    }

    urp_item_desc_t* desc = reinterpret_cast<urp_item_desc_t*>(rsp + sizeof(urp_rsp_hdr_t));
    if (out != nullptr) {
        out->assign(keys.size(), UrmaKVItem());
    }

    s->reads.clear();
    size_t pool_off = 0;
    for (size_t i = 0; i < keys.size(); ++i) {
        if (desc[i].status != URP_ST_HIT) {
            continue;
        }
        size_t need = static_cast<size_t>(desc[i].len) + URP_SLOT_HDR_SZ;
        need = (need + 63) & ~static_cast<size_t>(63);
        if (pool_off + need > s->read_capacity) {
            s->stats.retry++;
            return false;
        }
        Impl::ReadReq r;
        r.remote_ubva = desc[i].ubva;
        r.len = static_cast<uint32_t>(desc[i].len + URP_SLOT_HDR_SZ);
        r.local = s->read_pool + pool_off;
        r.idx = static_cast<uint32_t>(i);
        s->reads.push_back(r);
        pool_off += need;
    }

    if (!post_reads(s, req_id)) {
        return false;
    }
    if (!wait_reads(s, req_id, s->reads.size(), 100)) {
        return false;
    }

    uint64_t server_now = rh->server_now_ms;
    for (size_t r = 0; r < s->reads.size(); ++r) {
        const Impl::ReadReq& rr = s->reads[r];
        urp_slot_hdr_t* sh = reinterpret_cast<urp_slot_hdr_t*>(rr.local);
        const urp_item_desc_t& d = desc[rr.idx];
        if (sh->version != d.version || sh->len != d.len ||
            (sh->expire_ms != 0 && sh->expire_ms <= server_now)) {
            s->stats.retry++;
            return false;
        }
        if (out != nullptr) {
            (*out)[rr.idx].hit = true;
            (*out)[rr.idx].expire_ms = sh->expire_ms;
            if (want_values) {
                (*out)[rr.idx].value.assign(rr.local + URP_SLOT_HDR_SZ, d.len);
            }
        }
    }
    return true;
}

bool UrmaKV::get(const std::string& key, UrmaKVItem* out, bool want_values)
{
    std::vector<std::string> keys(1, key);
    std::vector<UrmaKVItem> items;
    if (!mget(keys, &items, want_values)) {
        return false;
    }
    if (out != nullptr) {
        *out = items[0];
    }
    return true;
}

bool UrmaKV::set(const std::string& key, const std::string& val, int ttl_ms)
{
    Impl* s = impl_;
    if (s == nullptr || s->srv.lease_slots == 0 ||
        val.size() > s->srv.lease_slot_size) {
        return false;
    }

    uint64_t req_id = ++s->req_seq;
    uint32_t lease = s->lease_seq++ % s->srv.lease_slots;
    uint64_t lease_ubva = s->srv.seg_va + s->srv.lease_base_off +
                          static_cast<uint64_t>(lease) * s->srv.lease_slot_size;

    /* 先把 value 单边写进租约槽，再发提交命令；RC 保序保证服务端先看到数据 */
    if (!post_write(s, lease_ubva, val.data(), static_cast<uint32_t>(val.size()),
                    false, 0, ~0ULL)) {
        return false;
    }

    std::vector<std::string> keys(1, key);
    std::vector<char> cmd;
    size_t cmd_len = build_cmd(s, URP_OP_SET, keys, static_cast<uint32_t>(ttl_ms),
                               static_cast<uint32_t>(val.size()), lease, req_id, cmd);
    uint32_t slot = static_cast<uint32_t>(req_id % URP_RING_SLOTS);
    uint64_t cmd_ubva = s->srv.seg_va + s->srv.cmd_ring_off +
                        static_cast<uint64_t>(slot) * URP_CMD_SLOT_SZ;
    if (!post_write(s, cmd_ubva, cmd.data(), static_cast<uint32_t>(cmd_len),
                    true, req_id, ~0ULL)) {
        return false;
    }
    return wait_reply(s, req_id, 100);
}

bool UrmaKV::append(const std::string& key, const std::string& val)
{
    Impl* s = impl_;
    if (s == nullptr || s->srv.lease_slots == 0 ||
        val.size() > s->srv.lease_slot_size) {
        return false;
    }

    uint64_t req_id = ++s->req_seq;
    uint32_t lease = s->lease_seq++ % s->srv.lease_slots;
    uint64_t lease_ubva = s->srv.seg_va + s->srv.lease_base_off +
                          static_cast<uint64_t>(lease) * s->srv.lease_slot_size;

    if (!post_write(s, lease_ubva, val.data(), static_cast<uint32_t>(val.size()),
                    false, 0, ~0ULL)) {
        return false;
    }

    std::vector<std::string> keys(1, key);
    std::vector<char> cmd;
    size_t cmd_len = build_cmd(s, URP_OP_APPEND, keys, 0,
                               static_cast<uint32_t>(val.size()), lease, req_id, cmd);
    uint32_t slot = static_cast<uint32_t>(req_id % URP_RING_SLOTS);
    uint64_t cmd_ubva = s->srv.seg_va + s->srv.cmd_ring_off +
                        static_cast<uint64_t>(slot) * URP_CMD_SLOT_SZ;
    if (!post_write(s, cmd_ubva, cmd.data(), static_cast<uint32_t>(cmd_len),
                    true, req_id, ~0ULL)) {
        return false;
    }
    return wait_reply(s, req_id, 100);
}
