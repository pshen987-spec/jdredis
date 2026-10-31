#!/usr/bin/env bash
# 一键运行对应优化版本，并采集CPU/网络/RTT/内存/复制/丢包验收数据。
set -uo pipefail

PROFILE="baseline_v2"
EXPECTED_BANNER='=== 测试版本: baseline_v2 ==='
EXPECTED_MODE='workload=full_pool_warm_normal_set_fixed200k_append_fixed200k_20set_20mget_40get_5append'
MASTER_SCRIPT="185_master_test_baseline_v2.sh"
SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
if [ -f "$SCRIPT_DIR/$MASTER_SCRIPT" ]; then
    # 支持采集脚本和原始master脚本都放在 /home/s30083118/jdredis/。
    MASTER_PATH="$SCRIPT_DIR/$MASTER_SCRIPT"
else
    # 保留 优化代码/00_baseline/ 的目录结构时，master脚本位于项目根目录。
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
JUMP_PASS="${JUMP_PASS:-}"
RUN_ID=$(date +%Y%m%d_%H%M%S)
RUN_ROOT="$SCRIPT_DIR/acceptance_data/$RUN_ID"
LOG_DIR_RUN="$RUN_ROOT/logs"
RESULT_DIR_RUN="$RUN_ROOT/results"
SAMPLE_CSV="$RUN_ROOT/acceptance_samples.csv"
SUMMARY="$RUN_ROOT/acceptance_summary.txt"
mkdir -p "$LOG_DIR_RUN" "$RESULT_DIR_RUN"

MODE_ARG="${1:-}"
case "$MODE_ARG" in
    ""|--manual) ;;
    --check)
        exec bash "$MASTER_PATH" --check
        ;;
    *)
        echo "用法: bash collect_baseline_v2_metrics.sh [--manual|--check]"
        exit 2
        ;;
esac

command -v redis-cli >/dev/null 2>&1 || { echo "[!!] 缺 redis-cli"; exit 1; }
[ -f "$MASTER_PATH" ] || { echo "[!!] 找不到 $MASTER_PATH"; exit 1; }

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

    local -a SSH_RUN=(ssh) SCP_RUN=(scp)
    if [ -n "$JUMP_PASS" ]; then
        if ! command -v sshpass >/dev/null 2>&1; then
            echo "[!!] 设置了 JUMP_PASS，但本机缺少 sshpass"
            return 1
        fi
        export SSHPASS="$JUMP_PASS"
        SSH_RUN=(sshpass -e ssh)
        SCP_RUN=(sshpass -e scp)
    fi

    local target="${JUMP_USER}@${JUMP_HOST}"
    local remote_profile="$JUMP_DIR/$PROFILE"
    local remote_profile_q remote_run_q
    # 三步上传复用同一个SSH连接；无密钥时仅首次连接需要输入密码。
    local mux_path="/tmp/jdredis-upload-${UID:-0}-$$"
    local -a SSH_OPTS=(-o StrictHostKeyChecking=no -o ConnectTimeout=10
        -o ControlMaster=auto -o ControlPersist=30 -o "ControlPath=$mux_path")
    printf -v remote_profile_q '%q' "$remote_profile"
    printf -v remote_run_q '%q' "$remote_profile/$RUN_ID"

    echo "===== 上传本次验收数据到206 ====="
    echo "远端目录: $target:$remote_profile/$RUN_ID/"
    if ! "${SSH_RUN[@]}" \
        -p "$JUMP_PORT" "${SSH_OPTS[@]}" \
        "$target" "mkdir -p $remote_profile_q"; then
        echo "[!!] 无法连接206或创建远端目录"
        echo "     本地数据保留在: $RUN_ROOT"
        return 1
    fi

    if ! "${SCP_RUN[@]}" -r \
        -P "$JUMP_PORT" "${SSH_OPTS[@]}" \
        "$RUN_ROOT" "$target:$remote_profile/"; then
        echo "[!!] 上传失败，本地数据仍保留在: $RUN_ROOT"
        return 1
    fi

    if ! "${SSH_RUN[@]}" \
        -p "$JUMP_PORT" "${SSH_OPTS[@]}" \
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
# 采集器早于主控启动；先清掉上一轮遗留的phase=3，避免跨轮污染本轮采样。
redis-cli -p "$REDIS_PORT" SET test:phase:marker 0 EX 120 >/dev/null 2>&1 || true
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

# 上传前确认本轮确实运行了本目录对应的压测程序，并且参数口径正确。
IDENTITY_RC=0
if [ "$MASTER_RC" -ne 0 ] || [ -z "$RESULT_FILE" ] || [ ! -s "$RESULT_FILE" ]; then
    echo "[!!] 主控未成功结束或缺少报告，拒绝上传"
    IDENTITY_RC=20
elif [ -z "$CLIENT_FILE" ] || [ ! -s "$CLIENT_FILE" ]; then
    echo "[!!] 未找到本轮客户端输出，拒绝上传，避免把空数据归档到 $PROFILE"
    IDENTITY_RC=21
elif ! grep -Fq "$EXPECTED_BANNER" "$CLIENT_FILE"; then
    echo "[!!] 压测程序版本不匹配，拒绝上传到 $PROFILE"
    echo "     期望标识: $EXPECTED_BANNER"
    echo "     请重新复制并编译本目录对应的C++文件"
    IDENTITY_RC=22
elif ! grep -Fq "$EXPECTED_MODE" "$CLIENT_FILE"; then
    echo "[!!] 客户端不是本轮线程阶梯版本，拒绝上传"
    IDENTITY_RC=25
elif ! grep -Fq 'MGET总聚合=100' "$CLIENT_FILE" || \
     ! grep -Fq 'RTT超时阈值=5ms' "$CLIENT_FILE" || \
     ! grep -Fq '网络读保护超时=100ms' "$CLIENT_FILE"; then
    echo "[!!] 客户端二进制参数不是 MGET=100、RTT阈值=5ms、读保护=100ms，拒绝上传"
    echo "     请在186重新编译本目录对应的C++文件"
    IDENTITY_RC=23
elif ! grep -Fq '[全部结束] measured_seconds=60' "$CLIENT_FILE"; then
    echo "[!!] 客户端没有完成阶段3的60秒测量，拒绝上传不完整结果"
    IDENTITY_RC=24
else
    echo "[ok] 压测程序身份及参数校验通过: $EXPECTED_BANNER"
fi

{
    echo "============================================================"
    echo "基线v2验收报告: $PROFILE"
    echo "运行时间: $RUN_ID"
    echo "主控返回码: $MASTER_RC"
    echo "采样网卡: ${IFACE:-未识别}"
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
    echo "[3] MGET/SET/GET/APPEND平均及最大RTT(us)"
    if [ -n "$CLIENT_FILE" ]; then
        grep -E '\[[^]]*RTT\(us\)\]' "$CLIENT_FILE" | tail -3 || echo "未解析到RTT"
    else
        echo "未找到client_out文件"
    fi
    echo
    echo "[4] RTT超过5ms的命令数"
    if [ -n "$CLIENT_FILE" ]; then
        for ph in 2 3; do
            awk -v p="阶段$ph" -v phase_num="$ph" '
                index($0,"[" p " ")==1 && /rtt_gt_5ms=/ {
                    split($0,a,"rtt_gt_5ms="); split(a[2],b," "); n+=b[1]
                }
                END {printf "阶段%d RTT>5ms=%d\n",phase_num,n}
            ' "$CLIENT_FILE"
        done
        echo "各命令达成OPS、命中率和返回字节:"
        grep -E '^\[阶段[23] (MGET|SET|GET|APPEND)\]' "$CLIENT_FILE" || true
    else
        echo "未找到client_out文件"
    fi
    echo
    echo "[4a] 186客户端CPU平均占用核数"
    if [ -n "$CLIENT_FILE" ]; then
        grep -F '[客户端CPU 阶段' "$CLIENT_FILE" || echo "未解析到客户端CPU"
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
    echo "[7] 本版本Redis配置证据"
    case "$PROFILE" in
        opt4_redis_tuning|opt7_all_optimized)
            redis-cli -p "$REDIS_PORT" CONFIG GET lazyfree-lazy-expire lazyfree-lazy-eviction lazyfree-lazy-server-del client-output-buffer-limit 2>/dev/null
            ;;
    esac
    case "$PROFILE" in
        opt6_io_threads|opt7_all_optimized)
            redis-cli -p "$REDIS_PORT" CONFIG GET io-threads io-threads-do-reads 2>/dev/null
            ;;
    esac
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
