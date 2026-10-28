//============================================================================
// urma_proto.h —— 方案4(URMA 数据面)客户端/服务端共享线协议
//
// 设计口径(与 baseline stress_test_v2 对齐):
//   - 一条逻辑 MGET = 1 条命令 + 100 个 key，最多 100 个描述符 + 100 个单边 READ
//   - 控制面: 单边 WRITE_IMM 写命令环/回复环；数据面: 单边 READ/WRITE
//   - 服务端 Arena 中每个 value 前有 64B slot_hdr，客户端 READ 时一并取回做版本校验
//   - SET/APPEND: 客户端先单边 WRITE 到服务端授予的写槽(lease)，再发提交命令
//
// 注意: 本文件是客户端与未来 Redis 侧 URMA Agent 的共同契约，字段变更必须两端同步。
//============================================================================
#ifndef URP_PROTO_H
#define URP_PROTO_H

#include <stdint.h>

#define URP_MAGIC        0x414D5255u   /* "URMA" little-endian */
#define URP_VERSION      1u

#define URP_CTRL_PORT    16379          /* 带外 TCP 控制面端口(与 Redis 6379/6380 区分) */
#define URP_MAX_KEYS     100            /* 与 baseline MGET_KEYS 对齐 */
#define URP_RING_SLOTS   64             /* 命令环/回复环槽位 */
#define URP_CMD_SLOT_SZ  4096           /* 100 key 的命令块约 2.5KB */
#define URP_RSP_SLOT_SZ  8192           /* 100 个描述符约 4.8KB */
#define URP_SLOT_HDR_SZ  64             /* Arena 中 value 前的槽头 */
#define URP_LEASE_SLOTS  32             /* 每客户端写槽租约数 */
#define URP_LEASE_SLOT_SZ (256u * 1024) /* 覆盖 200KB value */

/* 命令 opcode */
enum {
    URP_OP_GET    = 1,
    URP_OP_MGET   = 2,
    URP_OP_SET    = 3,
    URP_OP_APPEND = 4,
    URP_OP_PING   = 5,
};

/* 单个 key 的返回状态 */
enum {
    URP_ST_MISS = 0,
    URP_ST_HIT  = 1,
    URP_ST_ERR  = 2,
};

/* 握手角色 */
enum {
    URP_ROLE_CLIENT = 1,
    URP_ROLE_SERVER = 2,
};

/* 命令头(64B) */
typedef struct __attribute__((packed)) {
    uint32_t magic;
    uint16_t version;
    uint16_t opcode;
    uint32_t flags;
    uint32_t key_count;
    uint32_t ttl_ms;
    uint32_t payload_len;
    uint32_t lease_id;
    uint64_t req_id;
    uint32_t key_bytes;
    uint32_t client_id;
    uint32_t reserved[3];
} urp_cmd_hdr_t;

/* 回复头(64B) */
typedef struct __attribute__((packed)) {
    uint32_t magic;
    uint32_t status;      /* 0=成功 */
    uint64_t req_id;
    uint32_t item_count;
    uint32_t reserved0;
    uint64_t total_len;
    uint64_t server_now_ms;
    uint64_t reserved1[3];
} urp_rsp_hdr_t;

/* 单 key 描述(48B) */
typedef struct __attribute__((packed)) {
    uint32_t status;      /* URP_ST_* */
    uint32_t len;         /* value 长度 */
    uint32_t capacity;    /* 槽容量 */
    uint32_t reserved0;
    uint64_t version;     /* seqlock 版本，偶数稳定 */
    uint64_t expire_ms;   /* 0=不过期 */
    uint64_t ubva;        /* 槽头 UBVA(value 在 +64) */
    uint64_t reserved1;
} urp_item_desc_t;

/* Arena 槽头(64B)，与 value 一起被单边 READ */
typedef struct __attribute__((packed)) {
    uint64_t version;     /* 偶数稳定，奇数写入中 */
    uint32_t len;
    uint32_t capacity;
    uint64_t expire_ms;
    uint32_t owner;       /* lease 归属 client_id */
    uint32_t flags;       /* LIVE/GRANT/RECV/ZOMBIE */
    uint64_t epoch;
    uint64_t key_hash;
    uint64_t reserved;
} urp_slot_hdr_t;

/* 握手报文(112B)，TCP 带外交换 */
typedef struct __attribute__((packed)) {
    uint32_t magic;
    uint32_t version;
    uint32_t role;
    uint32_t client_id;
    uint8_t  eid[16];
    uint32_t uasid;
    uint32_t jetty_id;
    uint64_t seg_va;          /* 本端 Arena 的 UBVA va */
    uint64_t seg_len;
    uint32_t seg_attr;
    uint32_t token_id;
    uint32_t token_value;
    uint32_t reserved0;
    uint64_t ring_off;        /* client: 回复环在 client Arena 的偏移 */
    uint32_t ring_slots;
    uint32_t ring_slot_size;
    uint64_t cmd_ring_off;    /* server: 命令环在 server Arena 的偏移 */
    uint64_t lease_base_off;  /* server: 写槽租约在 server Arena 的偏移 */
    uint32_t lease_slots;
    uint32_t lease_slot_size;
} urp_hello_t;

#endif /* URP_PROTO_H */
