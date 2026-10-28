//============================================================================
// urma_kv_client.h —— 方案4 URMA 客户端库(供 stress_test_opt8_urma 使用)
//
// 线程模型: 一个 UrmaKV 实例绑定一个 worker 线程(独立 jetty/JFC/回复环/读缓冲).
// 与 baseline 对齐: 20 MGET + 20 SET + 40 GET + 5 APPEND 各建一个实例。
//
// 当前状态:
//   - 客户端侧已按 urma_proto.h 实现命令环/回复环/描述符 READ/写槽租约
//   - 需要 Redis 侧 URMA Agent(08 目录操作手册 P3)配合才能端到端跑通
//   - tcp/ums 传输不依赖本库，可直接跑基线
//============================================================================
#ifndef URMA_KV_CLIENT_H
#define URMA_KV_CLIENT_H

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

struct UrmaKVItem {
    bool     hit = false;
    uint64_t expire_ms = 0;   // 0=不过期
    std::string value;        // 仅 want_values=true 时填充
};

struct UrmaKVStats {
    uint64_t cmd_wr = 0;      // 命令 WRITE_IMM 次数
    uint64_t cmd_cr = 0;      // 命令完成次数
    uint64_t read_wr = 0;     // 单边 READ 次数
    uint64_t read_cr = 0;     // READ 完成次数
    uint64_t read_bytes = 0;  // READ 字节数
    uint64_t lease_wr = 0;    // SET/APPEND 写槽 WRITE 次数
    uint64_t cr_err = 0;      // 完成记录错误数
    uint64_t retry = 0;       // seqlock/过期重试数
    uint64_t timeouts = 0;    // 等待超时数
};

class UrmaKV {
public:
    struct Impl;   // 供 urma_kv_client.cpp 内的数据面原语使用

    // server_ip: 185 业务网 IP；ctrl_port: 带外控制面端口(默认 URP_CTRL_PORT)
    // dev_name: bonding_dev_0 / udmac0d1e2 ...
    // read_capacity: 每线程预注册读缓冲(字节)；MGET 线程建议 24MB，GET 线程 1MB
    static UrmaKV* create(const char* server_ip, int ctrl_port, const char* dev_name,
                          size_t read_capacity);
    ~UrmaKV();

    // 一条逻辑 MGET(最多 100 key)。want_values=false 时不把 value 拷贝到 string，
    // 只做 READ + 版本校验(压测客户端只关心成功/时延)。
    bool mget(const std::vector<std::string>& keys, std::vector<UrmaKVItem>* out,
              bool want_values = false);
    bool get(const std::string& key, UrmaKVItem* out, bool want_values = false);
    bool set(const std::string& key, const std::string& val, int ttl_ms);
    bool append(const std::string& key, const std::string& val);

    void destroy();

    const UrmaKVStats& stats() const;

private:
    UrmaKV();
    UrmaKV(const UrmaKV&) = delete;
    UrmaKV& operator=(const UrmaKV&) = delete;

    Impl* impl_;
};

#endif /* URMA_KV_CLIENT_H */
