#!/usr/bin/env bash
#============================================================================
# collect_opt3_metrics.sh —— 方案B(MGET拆10批x10GET Pipeline) 一键运行+验收采集
#
#   与 00_baseline/collect_baseline_metrics.sh 同一套采样/分阶段/报告口径,
#   只做三处增量:
#     1) 身份门禁多查"实现方式=方案B"和"预热TTL=1s"(防止拿基线/别的二进制冒充)
#     2) 客户端统计同时报 命令级(KeyGET逐条) 与 逻辑请求级(100键整体)
#     3) 新增 [8] 与冻结基线(20261022_195613) 逐项对照 + [9] 方案B自检判定
#
#   基线不重跑: 对照值来自 00_baseline/20261022_195613, 找不到目录时退回内置锚点.
#
# 用法:
#   bash collect_opt3_metrics.sh             # 自动(sshpass 登录186)
#   bash collect_opt3_metrics.sh --manual    # 手动(打印186上要跑的命令)
#   bash collect_opt3_metrics.sh --check     # 只做预检, 不重建Redis
#   bash collect_opt3_metrics.sh --selftest  # 只解析冻结基线锚点并打印, 验证对照链路
#
# 环境变量: BASELINE_DIR=/path/to/20261022_195613 可指定冻结基线目录
#           UPLOAD_RESULTS=0 不上传; 其余同基线采集脚本
#============================================================================
set -uo pipefail

PROFILE="opt3_mget_pipeline"
EXPECTED_BANNER='=== 测试版本: 方案B (MGET拆10批x10 GET Pipeline) ==='
EXPECTED_PARAMS='MGET总聚合=100  RTT超时阈值=5ms  网络读保护超时=100ms'
EXPECTED_MODE='实现方式: 方案B 每请求100xGET(10批x10, 全局共享连接池, 批次FIFO排队)'
EXPECTED_CALIBER='[运行配置] 预热TTL=1s 写TTL=1s'
MASTER_SCRIPT="185_master_test_opt3_pipeline.sh"
SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
if [ -f "$SCRIPT_DIR/$MASTER_SCRIPT" ]; then
    # 支持采集脚本和 master 脚本都放在 /home/s30083118/jdredis/。
    MASTER_PATH="$SCRIPT_DIR/$MASTER_SCRIPT"
else
    # 保留 03_mget_pipeline/ 的目录结构时，master脚本位于项目根目录。
    MASTER_PATH="$(cd "$SCRIPT_DIR/../.." && pwd)/$MASTER_SCRIPT"
fi
CLIENT_HOST="${CLIENT_HOST:-20.20.21.186}"
REDIS_PORT="${REDIS_PORT:-6379}"
REPLICA_PORT="${REPLICA_PORT:-6380}"
SAMPLE_SEC="${SAMPLE_SEC:-2}"
UPLOAD_RESULTS="${UPLOAD_RESULTS:-1}"
JUMP_HOST="${JUMP_HOST:-123.60.114.206}"
JUMP_USER="${JUMP_USER:-root}"
JUMP_PORT="${JUMP_PORT:-10004}"
JUMP_DIR="${JUMP_DIR:-/home/s30083118/jdredis/acceptance_data}"
# 默认密码包含字符 '$'，必须使用单引号，避免被 Bash 展开。
if [ -z "${JUMP_PASS+x}" ]; then
    JUMP_PASS='openGauss1#2_3$00'
fi
RUN_ID=$(date +%Y%m%d_%H%M%S)
RUN_ROOT="$SCRIPT_DIR/acceptance_data/$PROFILE/$RUN_ID"
LOG_DIR_RUN="$RUN_ROOT/logs"
RESULT_DIR_RUN="$RUN_ROOT/results"
SAMPLE_CSV="$RUN_ROOT/acceptance_samples.csv"
SUMMARY="$RUN_ROOT/acceptance_summary.txt"

# 冻结基线定位: 先看显式 BASELINE_DIR, 再按目录结构猜, 找不到用内置锚点
BASELINE_DIR="${BASELINE_DIR:-}"
if [ -z "$BASELINE_DIR" ]; then
    for d in "$SCRIPT_DIR/acceptance_data/20261022_195613" \
             "$SCRIPT_DIR/../00_baseline/20261022_195613" \
             "$SCRIPT_DIR/../../00_baseline/20261022_195613"; do
        if [ -d "$d" ]; then BASELINE_DIR="$d"; break; fi
    done
fi

MODE_ARG="${1:-}"
SELFTEST=0
case "$MODE_ARG" in
    ""|--manual) ;;
    --check)
        exec bash "$MASTER_PATH" --check
        ;;
    --selftest) SELFTEST=1 ;;
    *)
        echo "用法: bash collect_opt3_metrics.sh [--manual|--check|--selftest]"
        exit 2
        ;;
esac

#---------------------------------------------------------------------------
# 取数小工具: 全部只读日志文本, 不依赖人眼看 ps/proc
#---------------------------------------------------------------------------
# 取 "token=N" 的数值; 取不到输出 "-"
ctok() {
    local v
    v=$(grep -F "[$2]" "$1" 2>/dev/null | tail -1 | grep -oE "[^A-Za-z]$3=[0-9]+" | head -1 | cut -d= -f2)
    printf '%s' "${v:--}"
}
# 取某阶段某命令 RTT 的 avg/max (先去掉行首的 [阶段N RTT(us)] 段, 再按 | 分列)
crtt() {
    grep -F "[$2 RTT(us)]" "$1" 2>/dev/null | tail -1 | sed 's/^\[[^]]*\]//' | tr '|' '\n' | \
        awk -v k="$3" -v f="$4" '$1==k { for(i=2;i<=NF;i++){ split($i,a,"="); if(a[1]==f){print a[2]; exit} } }'
}
# 非数字/取不到 -> 默认 0, 供算术与百分比使用
num() { case "${1:-}" in ''|*[!0-9.-]*) printf '%s' "${2:-0}";; *) printf '%s' "$1";; esac; }
# 变化率 (b-a)/a
pct() { awk -v a="$(num "$1")" -v b="$(num "$2")" 'BEGIN{ if(a+0>0) printf "%+.1f%%", 100*(b-a)/a; else printf "n/a" }'; }
# 倍数 b/a
times() { awk -v a="$(num "$1")" -v b="$(num "$2")" 'BEGIN{ if(a+0>0) printf "x%.2f", b/a; else printf "n/a" }'; }
# 服务端单命令 usec/call: 用与 master 报告同一算法(INFO commandstats 采样差分)
#   $1=redis_info日志 $2=phase(2/3) $3=命令名(mget/set/get/append)
usec_of() {
    [ -n "${1:-}" ] || { printf '%s' "-"; return; }
    awk -v want_ph="$2" -v want_cm="$3" '
        $2 ~ /^phase=[23]$/ && $3 ~ /^cmdstat_(mget|set|get|append):calls=/ {
            split($2,p,"="); ph=p[2]
            split($3,a,","); split(a[1],c,":"); cm=c[1]; sub(/^cmdstat_/,"",cm)
            split(c[2],d,"="); calls=d[2]+0
            split(a[2],u,"="); usec=u[2]+0
            split($1,t,":"); sec=t[1]*3600+t[2]*60+t[3]
            k=cm ph
            if(!(k in has)){ fcall[k]=calls; fsec[k]=sec; fuse[k]=usec; has[k]=1 }
            lcall[k]=calls; lsec[k]=sec; luse[k]=usec
        }
        END{
            k=want_cm want_ph
            if(k in has){ inc=lcall[k]-fcall[k]; if(inc>0) printf "%.3f", (luse[k]-fuse[k])/inc; else printf "-" }
            else printf "-"
        }' "$1"
}

#---------------------------------------------------------------------------
# 冻结基线锚点: 有目录就从日志解析, 没目录用 20261022_195613 的冻结值兜底
#   全部变量在解析失败时保持内置值, 打印时标明来源, 不静默使用旧数
#---------------------------------------------------------------------------
load_frozen() {
    BASE_SOURCE="内置常量(00_baseline/20261022_195613)"
    # 客户端(命令级)
    B2M=166191; BALLM=343983; B2TO=16772; BALTO=587818
    B2_AVG=3595; B2_MAX=19234; B3_AVG=6736; B3_MAX=80862
    # 服务端 result
    B2_CPU_MAX=96; B2_CPU_AVG=95.0055; B3_CPU_MAX=98; B3_CPU_AVG=93.0045
    B2_RX=944061; B2_TX=1943668; B3_RX=650918; B3_TX=2468994
    B2_HIT=0.85; B3_HIT=2.74
    B2_MEM_S=1336270720; B2_MEM_E=1221779168
    B3_MEM_S=1003384288; B3_MEM_E=4791431408
    # 服务端 usec/call
    B2_US=20.835; B3_US=31.263; B2_SET_US=1.224; B3_GET_US=6.712

    if [ -z "$BASELINE_DIR" ] || [ ! -d "$BASELINE_DIR" ]; then
        BASELINE_DIR=""
        return 0
    fi
    local bc br bi bs v
    bc=$(ls -t "$BASELINE_DIR"/logs/client_out_*.log 2>/dev/null | head -1)
    br=$(ls -t "$BASELINE_DIR"/results/result_*.txt 2>/dev/null | head -1)
    bi=$(ls -t "$BASELINE_DIR"/logs/redis_info_*.log 2>/dev/null | head -1)
    bs="$BASELINE_DIR/acceptance_summary.txt"
    local parsed=0
    if [ -n "$bc" ] && [ -s "$bc" ]; then
        parsed=1
        v=$(grep '^\[阶段2结束\]' "$bc" | tail -1 | sed -nE 's/.*MGET=([0-9]+).*/\1/p'); [ -n "$v" ] && B2M=$v
        v=$(grep '^\[全部结束\]' "$bc" | tail -1 | sed -nE 's/.*MGET=([0-9]+).*/\1/p'); [ -n "$v" ] && BALLM=$v
        v=$(grep '^\[阶段2结束\]' "$bc" | tail -1 | sed -nE 's/.*超时=([0-9]+).*/\1/p'); [ -n "$v" ] && B2TO=$v
        v=$(grep '^\[全部结束\]' "$bc" | tail -1 | sed -nE 's/.*超时=([0-9]+).*/\1/p'); [ -n "$v" ] && BALTO=$v
        v=$(crtt "$bc" 阶段2 MGET avg); [ -n "$v" ] && [ "$v" != "-1" ] && B2_AVG=$v
        v=$(crtt "$bc" 阶段2 MGET max); [ -n "$v" ] && [ "$v" != "-1" ] && B2_MAX=$v
        v=$(crtt "$bc" 阶段3 MGET avg); [ -n "$v" ] && [ "$v" != "-1" ] && B3_AVG=$v
        v=$(crtt "$bc" 阶段3 MGET max); [ -n "$v" ] && [ "$v" != "-1" ] && B3_MAX=$v
    fi
    if [ -n "$br" ] && [ -s "$br" ]; then
        parsed=1
        v=$(grep -F '[阶段2] CPU' "$br" | tail -1 | sed -nE 's/.*最高单线程=([0-9.]+)%.*/\1/p'); [ -n "$v" ] && B2_CPU_MAX=$v
        v=$(grep -F '[阶段2] CPU' "$br" | tail -1 | sed -nE 's/.*平均=([0-9.]+)%.*/\1/p'); [ -n "$v" ] && B2_CPU_AVG=$v
        v=$(grep -F '[阶段3] CPU' "$br" | tail -1 | sed -nE 's/.*最高单线程=([0-9.]+)%.*/\1/p'); [ -n "$v" ] && B3_CPU_MAX=$v
        v=$(grep -F '[阶段3] CPU' "$br" | tail -1 | sed -nE 's/.*平均=([0-9.]+)%.*/\1/p'); [ -n "$v" ] && B3_CPU_AVG=$v
        v=$(grep -F '[阶段2] 网卡' "$br" | tail -1 | sed -nE 's/.*峰值RX=([0-9]+)kB\/s.*/\1/p'); [ -n "$v" ] && B2_RX=$v
        v=$(grep -F '[阶段2] 网卡' "$br" | tail -1 | sed -nE 's/.*峰值TX=([0-9]+)kB\/s.*/\1/p'); [ -n "$v" ] && B2_TX=$v
        v=$(grep -F '[阶段3] 网卡' "$br" | tail -1 | sed -nE 's/.*峰值RX=([0-9]+)kB\/s.*/\1/p'); [ -n "$v" ] && B3_RX=$v
        v=$(grep -F '[阶段3] 网卡' "$br" | tail -1 | sed -nE 's/.*峰值TX=([0-9]+)kB\/s.*/\1/p'); [ -n "$v" ] && B3_TX=$v
        v=$(grep -F '[阶段2] OPS峰值' "$br" | tail -1 | sed -nE 's/.*命中率=([0-9.]+)%.*/\1/p'); [ -n "$v" ] && B2_HIT=$v
        v=$(grep -F '[阶段3] OPS峰值' "$br" | tail -1 | sed -nE 's/.*命中率=([0-9.]+)%.*/\1/p'); [ -n "$v" ] && B3_HIT=$v
    fi
    if [ -n "$bi" ] && [ -s "$bi" ]; then
        parsed=1
        v=$(usec_of "$bi" 2 mget); [ -n "$v" ] && [ "$v" != "-" ] && B2_US=$v
        v=$(usec_of "$bi" 3 mget); [ -n "$v" ] && [ "$v" != "-" ] && B3_US=$v
        v=$(usec_of "$bi" 2 set);  [ -n "$v" ] && [ "$v" != "-" ] && B2_SET_US=$v
        v=$(usec_of "$bi" 3 get);  [ -n "$v" ] && [ "$v" != "-" ] && B3_GET_US=$v
    fi
    if [ -s "$bs" ]; then
        v=$(grep -F '阶段2: 内存开始=' "$bs" | tail -1 | sed -nE 's/.*开始=(-?[0-9]+) 结束=(-?[0-9]+).*/\1 \2/p')
        if [ -n "$v" ]; then set -- $v; B2_MEM_S=$1; B2_MEM_E=$2; parsed=1; fi
        v=$(grep -F '阶段3: 内存开始=' "$bs" | tail -1 | sed -nE 's/.*开始=(-?[0-9]+) 结束=(-?[0-9]+).*/\1 \2/p')
        if [ -n "$v" ]; then set -- $v; B3_MEM_S=$1; B3_MEM_E=$2; parsed=1; fi
    fi
    [ "$parsed" = 1 ] && BASE_SOURCE="$BASELINE_DIR"
    return 0
}
load_frozen

if [ "$SELFTEST" = 1 ]; then
    echo "===== 冻结基线锚点自检 (源: $BASE_SOURCE) ====="
    echo "  阶段2: MGET=$B2M (@${B2M}/30s=$(( B2M / 30 ))/s) MGET_usec/call=$B2_US SET_usec/call=$B2_SET_US"
    echo "  阶段2: RTT avg/max=${B2_AVG}/${B2_MAX}us CPU=${B2_CPU_MAX}/${B2_CPU_AVG}% NIC=${B2_RX}/${B2_TX}kB/s 命中率=${B2_HIT}%"
    echo "  阶段3: RTT avg/max=${B3_AVG}/${B3_MAX}us CPU=${B3_CPU_MAX}/${B3_CPU_AVG}% NIC=${B3_RX}/${B3_TX}kB/s 命中率=${B3_HIT}% MGET_usec/call=$B3_US GET_usec/call=$B3_GET_US"
    echo "  内存 阶段2: ${B2_MEM_S}->${B2_MEM_E}  阶段3: ${B3_MEM_S}->${B3_MEM_E}"
    echo "  命令级RTT>5ms: 阶段2=$B2TO 全程=$BALTO"
    case "$BASE_SOURCE" in
        内置*) echo "  判定: 未找到冻结基线目录(可用 BASELINE_DIR=... 指定), 对照段将使用内置锚点" ;;
        *)     echo "  判定: 已从冻结基线目录解析(非内置常量), 对照链路可用" ;;
    esac
    exit 0
fi

command -v redis-cli >/dev/null 2>&1 || { echo "[!!] 缺 redis-cli"; exit 1; }
[ -f "$MASTER_PATH" ] || { echo "[!!] 找不到 $MASTER_PATH"; exit 1; }

mkdir -p "$LOG_DIR_RUN" "$RESULT_DIR_RUN"

IFACE=$(ip route get "$CLIENT_HOST" 2>/dev/null |
    awk '{for(i=1;i<=NF;i++) if($i=="dev"){print $(i+1); exit}}')
if [ -z "$IFACE" ]; then
    IFACE=$(ls /sys/class/net 2>/dev/null | grep -v '^lo$' | head -1)
fi

read_field() {
    local text="$1" key="$2"
    printf '%s\n' "$text" | awk -F: -v k="$key" '$1==k {gsub(/[ \r]/,"",$2); print $2; exit}'
}

net_stat() {
    local key="$1"
    local path="/sys/class/net/$IFACE/statistics/$key"
    [ -r "$path" ] && cat "$path" || echo 0
}

sampler() {
    echo 'timestamp,phase,used_memory,used_memory_peak,used_memory_rss,fragmentation,master_offset,slave_offset,repl_link,rx_bytes,tx_bytes,rx_dropped,tx_dropped,rx_errors,tx_errors' > "$SAMPLE_CSV"
    while true; do
        local now phase mem repl_master repl_slave used peak rss frag mo so link
        now=$(date +%H:%M:%S)
        phase=$(redis-cli -p "$REDIS_PORT" GET test:phase:marker 2>/dev/null | tr -d '\r')
        [ -n "$phase" ] || phase=0
        mem=$(redis-cli -p "$REDIS_PORT" INFO memory 2>/dev/null)
        repl_master=$(redis-cli -p "$REDIS_PORT" INFO replication 2>/dev/null)
        repl_slave=$(redis-cli -p "$REPLICA_PORT" INFO replication 2>/dev/null)
        used=$(read_field "$mem" used_memory); [ -n "$used" ] || used=0
        peak=$(read_field "$mem" used_memory_peak); [ -n "$peak" ] || peak=0
        rss=$(read_field "$mem" used_memory_rss); [ -n "$rss" ] || rss=0
        frag=$(read_field "$mem" mem_fragmentation_ratio); [ -n "$frag" ] || frag=0
        mo=$(read_field "$repl_master" master_repl_offset); [ -n "$mo" ] || mo=0
        so=$(read_field "$repl_slave" slave_repl_offset); [ -n "$so" ] || so=0
        link=$(read_field "$repl_slave" master_link_status); [ -n "$link" ] || link=unknown
        echo "$now,$phase,$used,$peak,$rss,$frag,$mo,$so,$link,$(net_stat rx_bytes),$(net_stat tx_bytes),$(net_stat rx_dropped),$(net_stat tx_dropped),$(net_stat rx_errors),$(net_stat tx_errors)" >> "$SAMPLE_CSV"
        sleep "$SAMPLE_SEC"
    done
}

upload_run() {
    [ "$UPLOAD_RESULTS" = "1" ] || {
        echo "[i] UPLOAD_RESULTS=$UPLOAD_RESULTS，跳过上传"
        return 0
    }

    if ! command -v sshpass >/dev/null 2>&1; then
        echo "[!!] 本机缺少 sshpass，无法自动上传到206"
        echo "     本地数据保留在: $RUN_ROOT"
        return 1
    fi

    local target="${JUMP_USER}@${JUMP_HOST}"
    local remote_profile="$JUMP_DIR/$PROFILE"
    local remote_profile_q remote_run_q
    printf -v remote_profile_q '%q' "$remote_profile"
    printf -v remote_run_q '%q' "$remote_profile/$RUN_ID"

    echo "===== 上传本次验收数据到206 ====="
    echo "远端目录: $target:$remote_profile/$RUN_ID/"
    if ! SSHPASS="$JUMP_PASS" sshpass -e ssh \
        -p "$JUMP_PORT" -o StrictHostKeyChecking=no -o ConnectTimeout=10 \
        "$target" "mkdir -p $remote_profile_q"; then
        echo "[!!] 无法连接206或创建远端目录"
        echo "     本地数据保留在: $RUN_ROOT"
        return 1
    fi

    if ! SSHPASS="$JUMP_PASS" sshpass -e scp -r \
        -P "$JUMP_PORT" -o StrictHostKeyChecking=no -o ConnectTimeout=10 \
        "$RUN_ROOT" "$target:$remote_profile/"; then
        echo "[!!] 上传失败，本地数据仍保留在: $RUN_ROOT"
        return 1
    fi

    if ! SSHPASS="$JUMP_PASS" sshpass -e ssh \
        -p "$JUMP_PORT" -o StrictHostKeyChecking=no -o ConnectTimeout=10 \
        "$target" "test -s $remote_run_q/acceptance_summary.txt && test -s $remote_run_q/acceptance_samples.csv"; then
        echo "[!!] 上传结束，但远端完整性检查未通过"
        return 1
    fi

    echo "[ok] 本次完整验收目录已上传并校验"
    return 0
}
SAMPLER_PID=""
stop_sampler() {
    if [ -n "$SAMPLER_PID" ]; then
        kill "$SAMPLER_PID" 2>/dev/null || true
        wait "$SAMPLER_PID" 2>/dev/null || true
        SAMPLER_PID=""
    fi
}
trap stop_sampler EXIT INT TERM

echo "===== $PROFILE 验收采集开始 ====="
echo "运行目录: $RUN_ROOT"
echo "采样网卡: ${IFACE:-未识别}  间隔: ${SAMPLE_SEC}s"
echo "冻结基线: ${BASE_SOURCE}  (不重跑)"
sampler &
SAMPLER_PID=$!

MASTER_RC=0
if [ "$MODE_ARG" = "--manual" ]; then
    LOG_DIR="$LOG_DIR_RUN" RESULT_DIR="$RESULT_DIR_RUN" bash "$MASTER_PATH" --manual || MASTER_RC=$?
else
    LOG_DIR="$LOG_DIR_RUN" RESULT_DIR="$RESULT_DIR_RUN" bash "$MASTER_PATH" || MASTER_RC=$?
fi
sleep 2
stop_sampler

RESULT_FILE=$(ls -t "$RESULT_DIR_RUN"/result_*.txt 2>/dev/null | head -1)
CLIENT_FILE=$(ls -t "$LOG_DIR_RUN"/client_out_* 2>/dev/null | head -1)

#---------------------------------------------------------------------------
# 上传前身份门禁: 五查(标识/参数/实现方式/基线口径/拆分形状), 任一不过拒绝归档
#---------------------------------------------------------------------------
IDENTITY_RC=0
if [ -z "$CLIENT_FILE" ] || [ ! -s "$CLIENT_FILE" ]; then
    echo "[!!] 未找到本轮客户端输出，拒绝上传，避免把空数据归档到 $PROFILE"
    echo "     判定: LOG_DIR 里没有 client_out_*, 本轮未真正跑压测或回显未落盘"
    IDENTITY_RC=21
elif ! grep -Fq "$EXPECTED_BANNER" "$CLIENT_FILE"; then
    echo "[!!] 压测程序版本不匹配，拒绝上传到 $PROFILE"
    echo "     期望标识: $EXPECTED_BANNER"
    echo "     判定: 二进制不是本目录方案B源码编译的版本(可能还是基线/旧方案)"
    IDENTITY_RC=22
elif ! grep -Fq "$EXPECTED_PARAMS" "$CLIENT_FILE"; then
    echo "[!!] 客户端参数口径不是 MGET=100 / RTT阈值=5ms / 读保护=100ms，拒绝上传"
    echo "     期望: $EXPECTED_PARAMS"
    echo "     判定: 二进制与冻结基线口径不一致, 数字无法对照"
    IDENTITY_RC=23
elif ! grep -Fq "$EXPECTED_MODE" "$CLIENT_FILE"; then
    echo "[!!] 压测程序不是方案B实现，拒绝上传到 $PROFILE"
    echo "     期望: $EXPECTED_MODE"
    echo "     判定: 缺少方案B实现方式行 -> 跑的是基线或别的优化版本"
    IDENTITY_RC=24
elif ! grep -Fq "$EXPECTED_CALIBER" "$CLIENT_FILE"; then
    echo "[!!] 运行配置不是基线口径(预热TTL=1s/写TTL=1s)，拒绝上传"
    echo "     期望: $EXPECTED_CALIBER"
    echo "     判定: [运行配置]行缺失或TTL口径被改 -> 不能与冻结基线对照"
    IDENTITY_RC=25
elif ! grep -Fq '逻辑请求=100键' "$CLIENT_FILE" || ! grep -Fq '拆分=10批x10GET' "$CLIENT_FILE" \
     || ! grep -Fq '连接池=' "$CLIENT_FILE"; then
    echo "[!!] 拆分形状不是 100键=10批x10GET(全局共享连接池)，拒绝上传"
    echo "     判定: 方案B定义是10批x10GET进入全局共享连接池(K默认20)、批次FIFO排队; 形状不符则发送方式已变"
    IDENTITY_RC=26
else
    echo "[ok] 压测程序身份校验通过: 方案B + 基线口径 + 10批x10GET + 全局共享连接池"
fi

{
    echo "============================================================"
    echo "优化验收报告: $PROFILE (方案B: MGET -> 10批x10 GET Pipeline)"
    echo "运行时间: $RUN_ID"
    echo "主控返回码: $MASTER_RC"
    echo "采样网卡: ${IFACE:-未识别}"
    echo "对照基线: ${BASE_SOURCE} (冻结, 本轮未重跑)"
    echo "============================================================"
    echo
    echo "[1] CPU峰值和阶段平均值"
    if [ -n "$RESULT_FILE" ]; then
        grep -E '\[阶段[23]\] CPU' "$RESULT_FILE" || echo "未解析到阶段CPU"
    else
        echo "未找到result文件"
    fi
    echo
    echo "[2] RX/TX峰值"
    if [ -n "$RESULT_FILE" ]; then
        grep -E '\[阶段[23]\] 网卡' "$RESULT_FILE" || echo "未解析到阶段网络峰值"
    else
        echo "未找到result文件"
    fi
    echo
    echo "[3] RTT(us): MGET列=100键逻辑请求, KeyGET列=内部单键GET; SET/GET/APPEND为命令级"
    if [ -n "$CLIENT_FILE" ]; then
        grep -E '\[[^]]*RTT\(us\)\]' "$CLIENT_FILE" | tail -3 || echo "未解析到RTT"
    else
        echo "未找到client_out文件"
    fi
    echo
    echo "[4] RTT超过5ms的数量 (命令级=基线口径; 逻辑请求级=100键整体)"
    if [ -n "$CLIENT_FILE" ]; then
        s2=$(ctok "$CLIENT_FILE" "阶段2结束" 超时); all=$(ctok "$CLIENT_FILE" "全部结束" 超时)
        l2=$(ctok "$CLIENT_FILE" "阶段2结束-逻辑请求" 超时); la=$(ctok "$CLIENT_FILE" "全部结束-逻辑请求" 超时)
        s2=$(num "$s2"); all=$(num "$all"); l2=$(num "$l2"); la=$(num "$la")
        echo "命令级:     阶段2=$s2  阶段3=$((all-s2))  全程=$all"
        echo "逻辑请求级: 阶段2=$l2  阶段3=$((la-l2))  全程=$la"
        echo "对照冻结基线(基线命令级=1条MGET/请求; 本轮命令级=单键GET, 口径不同, 对照只看逻辑请求级): 阶段2=$B2TO  阶段3=$((BALTO-B2TO))  全程=$BALTO"
        echo "优化特有统计:"
        grep -E '\[本地缓存\]|\[APPEND保护\]|\[MGET逻辑请求\]|\[逻辑TTL\]' "$CLIENT_FILE" | tail -8 || true
    else
        echo "未找到client_out文件"
    fi
    echo
    echo "[5] 分阶段内存、复制差值和网卡丢包增量"
    if [ -s "$SAMPLE_CSV" ]; then
        awk -F, '
            NR==1 {next}
            $2==2 || $2==3 {
                p=$2
                if(!(p in seen)) {
                    seen[p]=1; first_mem[p]=$3; first_rxdrop[p]=$12; first_txdrop[p]=$13
                    first_rxerr[p]=$14; first_txerr[p]=$15
                }
                last_mem[p]=$3; last_peak[p]=$4; last_frag[p]=$6
                last_rxdrop[p]=$12; last_txdrop[p]=$13; last_rxerr[p]=$14; last_txerr[p]=$15
                lag=$7-$8; if(lag<0)lag=-lag; if(lag>max_lag[p])max_lag[p]=lag
                if($9!="up") bad_link[p]++
            }
            END {
                for(p=2;p<=3;p++) {
                    if(!seen[p]) { printf "阶段%d: 无采样\n",p; continue }
                    printf "阶段%d: 内存开始=%d 结束=%d 增长=%d bytes 峰值=%d 碎片率末值=%s\n", p,first_mem[p],last_mem[p],last_mem[p]-first_mem[p],last_peak[p],last_frag[p]
                    printf "阶段%d: 最大主从offset差=%d bytes 复制链路异常采样=%d\n",p,max_lag[p],bad_link[p]+0
                    printf "阶段%d: RX丢包增量=%d TX丢包增量=%d RX错误增量=%d TX错误增量=%d\n",p,last_rxdrop[p]-first_rxdrop[p],last_txdrop[p]-first_txdrop[p],last_rxerr[p]-first_rxerr[p],last_txerr[p]-first_txerr[p]
                }
            }
        ' "$SAMPLE_CSV"
    else
        echo "采样文件为空"
    fi
    echo
    echo "[6] 测试结束后的主从状态"
    redis-cli -p "$REDIS_PORT" INFO replication 2>/dev/null | grep -E 'role:|connected_slaves:|master_repl_offset:' || true
    redis-cli -p "$REPLICA_PORT" INFO replication 2>/dev/null | grep -E 'role:|master_link_status:|slave_repl_offset:' || true
    echo
    echo "[7] 服务端配置证据 (方案B只改业务层, 服务端应与基线一致)"
    for k in maxmemory maxmemory-policy io-threads io-threads-do-reads appendonly save; do
        echo -n "  $k="
        redis-cli -p "$REDIS_PORT" CONFIG GET "$k" 2>/dev/null | tail -1
    done

    #=======================================================================
    # [8] 与冻结基线逐项对照 —— 基线不重跑, 数字全部来自 20261022_195613
    #=======================================================================
    echo
    echo "[8] 与冻结基线对比 (基线=${BASE_SOURCE}; 差值=方案B-基线; 基线未重跑)"
    echo "    注: 方案B读侧连接=全局共享池(默认20, 与基线20条读连接一致), 批次FIFO排队;"
    echo "        连接数不再是自变量, 但同一请求的10批不再同时下发, 逻辑RTT含排队等待; 吞吐/尾延迟差值归因于拆分+排队"
    if [ -n "$CLIENT_FILE" ]; then
        # 本轮客户端指标
        c2m=$(num "$(ctok "$CLIENT_FILE" 阶段2结束 MGET)")
        c3m=$(num "$(ctok "$CLIENT_FILE" 全部结束 MGET)")
        c2_lps=$(( c2m / 30 )); c3_lps=$(( (c3m - c2m) / 60 ))
        c2_avg=$(crtt "$CLIENT_FILE" 阶段2 MGET avg); c2_max=$(crtt "$CLIENT_FILE" 阶段2 MGET max)
        c3_avg=$(crtt "$CLIENT_FILE" 阶段3 MGET avg); c3_max=$(crtt "$CLIENT_FILE" 阶段3 MGET max)
        have_rtt=0
        case "${c2_max:-}" in ''|'-1'|'-') c2_max="-" ;; *) have_rtt=1 ;; esac
        case "${c2_avg:-}" in ''|'-1'|'-') c2_avg="-" ;; esac
        case "${c3_max:-}" in ''|'-1'|'-') c3_max="-" ;; esac
        case "${c3_avg:-}" in ''|'-1'|'-') c3_avg="-" ;; esac
        b2_lps=$(( B2M / 30 )); b3_lps=$(( (BALLM - B2M) / 60 ))

        # 本轮服务端 usec/call (与基线同一算法: 3s 采样差分, 窗口≈27s/55s)
        have_us=0
        c2_us="-"; c3_us="-"; c2_set_us="-"; c2_cost="-"; c3_cost="-"
        f_info=$(ls -t "$LOG_DIR_RUN"/redis_info_*.log 2>/dev/null | head -1)
        if [ -n "$f_info" ]; then
            c2_us=$(usec_of "$f_info" 2 get); c3_us=$(usec_of "$f_info" 3 get)
            c2_set_us=$(usec_of "$f_info" 2 set)
            [ "$c2_us" != "-" ] && [ "$c3_us" != "-" ] && have_us=1
            c2_cost=$(awk -v u="$(num "$c2_us")" 'BEGIN{ printf "%.1f", u*100 }')
            c3_cost=$(awk -v u="$(num "$c3_us")" 'BEGIN{ printf "%.1f", u*100 }')
        else
            echo "  !! 未找到 redis_info_*.log, 服务端 usec/call 列缺失(成本维不可判)"
        fi

        # 本轮 result / CSV
        c2_cpu_max=0; c2_cpu_avg=0; c3_cpu_max=0; c3_cpu_avg=0
        c2_rx=0; c2_tx=0; c3_rx=0; c3_tx=0; c2_hit="-"; c3_hit="-"
        c2_mem_s=0; c2_mem_e=0; c3_mem_s=0; c3_mem_e=0
        if [ -n "$RESULT_FILE" ]; then
            v=$(grep -F '[阶段2] CPU' "$RESULT_FILE" | tail -1 | sed -nE 's/.*最高单线程=([0-9.]+)%.*/\1/p'); c2_cpu_max=$(num "$v")
            v=$(grep -F '[阶段2] CPU' "$RESULT_FILE" | tail -1 | sed -nE 's/.*平均=([0-9.]+)%.*/\1/p'); c2_cpu_avg=$(num "$v")
            v=$(grep -F '[阶段3] CPU' "$RESULT_FILE" | tail -1 | sed -nE 's/.*最高单线程=([0-9.]+)%.*/\1/p'); c3_cpu_max=$(num "$v")
            v=$(grep -F '[阶段3] CPU' "$RESULT_FILE" | tail -1 | sed -nE 's/.*平均=([0-9.]+)%.*/\1/p'); c3_cpu_avg=$(num "$v")
            v=$(grep -F '[阶段2] 网卡' "$RESULT_FILE" | tail -1 | sed -nE 's/.*峰值RX=([0-9]+)kB\/s.*/\1/p'); c2_rx=$(num "$v")
            v=$(grep -F '[阶段2] 网卡' "$RESULT_FILE" | tail -1 | sed -nE 's/.*峰值TX=([0-9]+)kB\/s.*/\1/p'); c2_tx=$(num "$v")
            v=$(grep -F '[阶段3] 网卡' "$RESULT_FILE" | tail -1 | sed -nE 's/.*峰值RX=([0-9]+)kB\/s.*/\1/p'); c3_rx=$(num "$v")
            v=$(grep -F '[阶段3] 网卡' "$RESULT_FILE" | tail -1 | sed -nE 's/.*峰值TX=([0-9]+)kB\/s.*/\1/p'); c3_tx=$(num "$v")
            v=$(grep -F '[阶段2] OPS峰值' "$RESULT_FILE" | tail -1 | sed -nE 's/.*命中率=([0-9.]+)%.*/\1/p'); [ -n "$v" ] && c2_hit=$v
            v=$(grep -F '[阶段3] OPS峰值' "$RESULT_FILE" | tail -1 | sed -nE 's/.*命中率=([0-9.]+)%.*/\1/p'); [ -n "$v" ] && c3_hit=$v
        fi
        if [ -s "$SAMPLE_CSV" ]; then
            read -r c2_mem_s c2_mem_e < <(awk -F, '$2==2{if(!s)s=$3; e=$3} END{if(s!="")print s,e; else print 0,0}' "$SAMPLE_CSV")
            read -r c3_mem_s c3_mem_e < <(awk -F, '$2==3{if(!s)s=$3; e=$3} END{if(s!="")print s,e; else print 0,0}' "$SAMPLE_CSV")
        fi

        echo "  基线吞吐(阶段2 MGET/30s)=${B2M}->${b2_lps}/s  方案B(阶段2 逻辑请求/30s)=${c2m}->${c2_lps}/s  (相对 ${b2_lps}/s: $(pct "$b2_lps" "$c2_lps"))"
        echo "  基线吞吐(阶段3 MGET/60s)=$((BALLM-B2M))->${b3_lps}/s  方案B(阶段3 逻辑请求/60s)=$((c3m-c2m))->${c3_lps}/s  (相对 ${b3_lps}/s: $(pct "$b3_lps" "$c3_lps"))"
        echo "  服务端单命令成本(阶段2): 基线 mget=${B2_US} set=${B2_SET_US}us/call -> 方案B get=${c2_us} set=${c2_set_us}us/call"
        echo "  服务端单逻辑请求成本(阶段2): 基线 mget=${B2_US}us/call -> 方案B get=${c2_us}us/call x100=${c2_cost}us (${B2_US}us 的 $(times "$B2_US" "$c2_cost"))"
        echo "  服务端单逻辑请求成本(阶段3): 基线 mget=${B3_US}us/call -> 方案B get=${c3_us}us/call x100=${c3_cost}us (${B3_US}us 的 $(times "$B3_US" "$c3_cost"))"
        echo "        注: 阶段3的 cmdstat_get 混入40个热Key GET线程, 该行只作参考; 成本判定统一用阶段2(读线程只有pipeline GET)"
        echo "  逻辑请求RTT(阶段2) avg/max: 基线 ${B2_AVG}/${B2_MAX}us -> 方案B ${c2_avg}/${c2_max}us  (max 相对 $(pct "$B2_MAX" "$c2_max"))"
        echo "  逻辑请求RTT(阶段3) avg/max: 基线 ${B3_AVG}/${B3_MAX}us -> 方案B ${c3_avg}/${c3_max}us  (max 相对 $(pct "$B3_MAX" "$c3_max"))"
        echo "  CPU 最高/平均(阶段2): 基线 ${B2_CPU_MAX}/${B2_CPU_AVG}% -> 方案B ${c2_cpu_max}/${c2_cpu_avg}%"
        echo "  CPU 最高/平均(阶段3): 基线 ${B3_CPU_MAX}/${B3_CPU_AVG}% -> 方案B ${c3_cpu_max}/${c3_cpu_avg}%"
        echo "  网卡峰值RX/TX(阶段2): 基线 ${B2_RX}/${B2_TX}kB/s -> 方案B ${c2_rx}/${c2_tx}kB/s"
        echo "  网卡峰值RX/TX(阶段3): 基线 ${B3_RX}/${B3_TX}kB/s -> 方案B ${c3_rx}/${c3_tx}kB/s"
        echo "  命中率(阶段2/阶段3): 基线 ${B2_HIT}%/${B3_HIT}% -> 方案B ${c2_hit}%/${c3_hit}%"
        echo "  内存起止(阶段2): 基线 ${B2_MEM_S}->${B2_MEM_E} -> 方案B ${c2_mem_s}->${c2_mem_e}"
        echo "  内存起止(阶段3): 基线 ${B3_MEM_S}->${B3_MEM_E} -> 方案B ${c3_mem_s}->${c3_mem_e}"

        # 预登记判据 + 自动判定(打印原始量, 结论只由这三项算出)
        cost_up=$(awk -v a="$(num "$B2_US")" -v b="$(num "$c2_cost")" 'BEGIN{print (b>a)?1:0}')
        tp_down=$(awk -v a="$b2_lps" -v b="$c2_lps" 'BEGIN{print (b<a)?1:0}')
        tail_down=$(awk -v a="$(num "$B2_MAX")" -v b="$(num "$c2_max")" 'BEGIN{print (b<a)?1:0}')
        echo "  [8.4] 预登记判据(跑前锁定):"
        echo "        成本比 = 方案B get_usec/call x100 / 基线 mget_usec/call  (>1 更贵)"
        echo "        吞吐比 = 方案B 逻辑请求/s / 基线 MGET/s              (<1 更慢)"
        echo "        尾延迟比 = 方案B 逻辑RTT max / 基线 MGET max          (<1 削峰)"
        echo "        服务端单逻辑请求成本比=$( [ "$have_us" = 1 ] && times "$B2_US" "$c2_cost" || echo n/a )  逻辑吞吐比=$(times "$b2_lps" "$c2_lps")  尾延迟比=$( [ "$have_rtt" = 1 ] && times "$B2_MAX" "$c2_max" || echo n/a )"
        if [ "$have_us" = 0 ] || [ "$have_rtt" = 0 ]; then
            echo "  !!  维度缺失(成本可判=$([ "$have_us" = 1 ] && echo 是 || echo 否), 尾延迟可判=$([ "$have_rtt" = 1 ] && echo 是 || echo 否)) -> 不套用预登记决策树, 先修日志缺失"
        elif [ "$cost_up" = 1 ] && [ "$tail_down" = 0 ]; then
            echo "        判定: 成本上升且尾延迟未降 -> 本口径拆分无净收益。原因是基线口径 MGET 回复几乎全 NIL"
            echo "              (基线阶段2命中率 ${B2_HIT}%, 阶段2内存 1.34GB->1.22GB 说明预热数据已过期, 20MB 聚合根本没发生);"
            echo "              拆分收益必须在“数据常驻”口径下才可验证, 而那需要重跑基线(本轮明确不做)。"
        elif [ "$cost_up" = 1 ] && [ "$tail_down" = 1 ]; then
            echo "        判定: 成本上升但尾延迟下降 -> 用吞吐/CPU换尾延迟。若目标是消掉单条 MGET 独占主线程/20MB回包尖峰, 方向有效; 若目标是吞吐, 不合算。"
        elif [ "$cost_up" = 0 ] && [ "$tp_down" = 0 ]; then
            echo "        判定: 成本未升且吞吐未降 -> 本口径拆分无副作用, 但也没有收益证据(20MB未出现)。"
        else
            echo "        判定: 成本未升但吞吐下降 -> 先看[9]的 KeyGET/逻辑请求是否≈100(批次是否真按10条下发), 再核对共享池K(默认20)是否过小导致排队"
        fi
    else
        echo "  未找到client_out, 无法做逐项对照"
    fi

    #=======================================================================
    # [9] 方案B自检: 本轮数字能不能用, 每条都给出判定+原因
    #=======================================================================
    echo
    echo "[9] 方案B自检判定 (任一项 !! 则该轮数字不可用于对照)"
    if [ -n "$CLIENT_FILE" ]; then
        ok=$(num "$(ctok "$CLIENT_FILE" 全部结束 成功)"); f=$(num "$(ctok "$CLIENT_FILE" 全部结束 失败)")
        kt=$(num "$(ctok "$CLIENT_FILE" 全部结束 KeyGET)"); st=$(num "$(ctok "$CLIENT_FILE" 全部结束 SET)")
        gt=$(num "$(ctok "$CLIENT_FILE" 全部结束 GET)"); at=$(num "$(ctok "$CLIENT_FILE" 全部结束 APPEND)")
        mt=$(num "$(ctok "$CLIENT_FILE" 全部结束 MGET)"); mf=$(num "$(ctok "$CLIENT_FILE" 全部结束-逻辑请求 失败)")
        er=$(num "$(ctok "$CLIENT_FILE" 全部结束-逻辑请求 错误回复)"); mtto=$(num "$(ctok "$CLIENT_FILE" 全部结束-逻辑请求 超时)")
        lg=$(( mt + mf ))
        if [ "$kt" -gt 0 ] && [ "$lg" -gt 0 ] && [ $(( kt % 100 )) -eq 0 ] && [ $(( kt / 100 )) -eq "$lg" ]; then
            echo "  ok  KeyGET = 100 x 逻辑请求: ${kt} = 100 x ${lg}"
        elif [ "$mf" -gt 0 ] && [ "$kt" -le $(( 100 * lg )) ]; then
            echo "  判定 KeyGET(${kt}) < 100 x 逻辑请求(${lg}=完成${mt}+失败${mf}): 失败请求里有$(( 100 * lg - kt ))条GET因断连未能append, 属失败路径解释项; 但失败请求本身已使本轮不可用于对账(见逻辑请求失败一行)"
        else
            echo "  !!  KeyGET(${kt}) 与 100 x 逻辑请求(${lg}) 不相等且逻辑失败=0 -> 拆分计数有漏记/重复, 本轮不可用于对账"
        fi
        # 阶段2快照时读线程仍在跑(基线同语义), 允许差值<=100x20读线程 + 100x已失败逻辑请求
        c2k=$(num "$(ctok "$CLIENT_FILE" 阶段2结束 KeyGET)"); c2m=$(num "$(ctok "$CLIENT_FILE" 阶段2结束 MGET)")
        c2mf=$(num "$(ctok "$CLIENT_FILE" 阶段2结束-逻辑请求 失败)")
        d2=$(( c2k - 100 * c2m )); lim=$(( 100 * 20 + 100 * c2mf ))
        if [ "$d2" -ge 0 ] && [ "$d2" -le "$lim" ]; then
            echo "  ok  阶段2快照差值=${d2} (KeyGET-100x逻辑请求), 在途上限=${lim}=100x20读线程$( [ "$c2mf" -gt 0 ] && echo "+100x${c2mf}失败请求" ): 快照时读线程仍在跑, 属正常"
        else
            echo "  !!  阶段2快照差值=${d2} 超出[0,${lim}] -> 阶段2计数异常, 阶段2吞吐不可信"
        fi
        csum=$(( kt + st + gt + at )); resid=$(( ok + f - csum ))
        if [ "$resid" -eq 0 ]; then
            echo "  ok  命令守恒: 成功(${ok}) + 失败(${f}) = KeyGET+SET+GET+APPEND(${csum})"
        elif [ "$resid" -gt 0 ]; then
            echo "  判定 命令守恒残差=${resid}: 即非GET类(SET/热GET/APPEND)写失败的命令数(基线口径下失败命令不计数), 属解释项"
        else
            echo "  !!  命令守恒为负(${resid}) -> 计数逻辑有bug, 本轮数据不可信"
        fi
        if [ "$mf" -eq 0 ]; then echo "  ok  逻辑请求失败=0 (100条GET全部拿到正常回复)"; else echo "  !!  逻辑请求失败=${mf} -> 有请求未凑齐100条回复, 吞吐/部分RTT被低估"; fi
        if [ "$er" -eq 0 ]; then echo "  ok  错误回复=0 (-OOM/-MOVED等未出现)"; else echo "  !!  错误回复=${er} -> 服务端曾返回错误, 需看客户端 [redis错误回复] 原文"; fi
        if [ "$lg" -gt 0 ]; then
            ratio=$(awk -v a="$mtto" -v b="$lg" 'BEGIN{printf "%.2f", 100*a/b}')
            if awk -v r="$ratio" 'BEGIN{exit !(r<=100)}'; then
                echo "  判定 逻辑请求RTT>5ms 占比=${ratio}% (${mtto}/${lg}=完成+失败); 基线只有命令级超时=${BALTO}, 不可按命令类型拆"
            else
                echo "  !!  逻辑请求RTT>5ms=${mtto} 超过逻辑请求总数${lg} -> 数据不自洽(超时计数对成功+失败都累加, 不应超过总数)"
            fi
        fi
        echo "  判定 全量90s: KeyGET≈$(( kt / 90 ))/s, 逻辑请求≈$(( mt / 90 ))/s -> 命令数/逻辑请求=$(( mt > 0 ? kt / mt : 0 ))(理论100)"
        # 连接模型: 方案B改为全局共享池K(默认20), 与基线20条读连接对齐; 批次排队是新的代价
        cfg=$(grep -F '[运行配置]' "$CLIENT_FILE" | tail -1)
        cpool=$(printf '%s' "$cfg" | sed -nE 's/.*连接池=([0-9]+).*/\1/p')
        if [ -n "${cpool:-}" ]; then
            if [ "$cpool" = "20" ]; then
                echo "  判定 读侧连接=${cpool}(全局共享池, 基线20) -> 连接数与基线一致, 不再是自变量; 逻辑RTT含批次排队等待"
            else
                echo "  判定 读侧连接=${cpool}(全局共享池, 基线20) -> K≠20, 连接数仍是自变量, 对照时须注明"
            fi
        else
            echo "  !!  读侧连接池未解析到(期望启动行含 连接池=K) -> 可能跑的是旧的10线程并发版本"
        fi
    else
        echo "  !!  无client_out, 无法自检"
    fi
    echo
    echo "原始采样: $SAMPLE_CSV"
    echo "主控结果: ${RESULT_FILE:-无}"
    echo "客户端输出: ${CLIENT_FILE:-无}"
} | tee "$SUMMARY"

echo "===== 验收采集完成 ====="
echo "汇总报告: $SUMMARY"
echo "原始采样: $SAMPLE_CSV"

UPLOAD_RC=0
if [ "$IDENTITY_RC" -eq 0 ]; then
    upload_run || UPLOAD_RC=$?
else
    UPLOAD_RC=$IDENTITY_RC
    echo "[!!] 本地数据保留在: $RUN_ROOT"
fi
if [ "$MASTER_RC" -ne 0 ]; then
    exit "$MASTER_RC"
fi
exit "$UPLOAD_RC"
