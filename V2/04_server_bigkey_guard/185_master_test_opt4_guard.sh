#!/usr/bin/env bash
#============================================================================
# v2 基线主控。185运行主备Redis，186运行 stress_test_opt4_guard。
# 通过环境变量传 LOAD_MULTIPLIER、KEY_POOL、ABNORMAL_SET_SHARE 和
# RANDOM_SEED。LOAD_MULTIPLIER只放大线程数，不限制请求速率。
# CLIENT_PASS 非空时使用 sshpass；否则使用SSH密钥。--check不改Redis。
#============================================================================
set -uo pipefail

EXPECTED_CLIENT_BANNER='=== 测试版本: opt4_server_bigkey_guard_v2 ==='
EXPECTED_CLIENT_PARAMS='MGET总聚合=100  RTT超时阈值=5ms  网络读保护超时=100ms'
EXPECTED_CLIENT_MODE='workload=full_pool_warm_normal_set_fixed200k_append_fixed200k_20set_20mget_40get_5append'
SCRIPT_VERSION="2026-10-30-opt4_guard-v2"
CLIENT_HOST="${CLIENT_HOST:-20.20.21.186}"   # 默认压测机; 本机若是186(服务器)请覆盖 20.20.21.185
CLIENT_USER="${CLIENT_USER:-root}"
CLIENT_PASS="${CLIENT_PASS:-}"
CLIENT_DIR="${CLIENT_DIR:-/home/s30083118/jdredis}"
SERVER_HOST="${SERVER_HOST:-20.20.21.185}"
REDIS_PORT="${REDIS_PORT:-6379}"
REPLICA_PORT="${REPLICA_PORT:-6380}"
MAXMEM="${MAXMEM:-80g}"
REDIS_SERVER_BIN="${REDIS_SERVER_BIN:-/home/s30083118/jdredis/redis-server-opt4}"
VALUE_LIMIT_BYTES="${VALUE_LIMIT_BYTES:-512000}"
NET_SPEED="${NET_SPEED:-25g}"          # 链路口径: 25g=25G / 10g=万兆 / 1g=千兆(仅影响报告对比文本)
LOAD_MULTIPLIER="${LOAD_MULTIPLIER:-1}"
KEY_POOL="${KEY_POOL:-400000}"
ABNORMAL_SET_SHARE="${ABNORMAL_SET_SHARE:-0}"
RANDOM_SEED="${RANDOM_SEED:-20261028}"
REDIS_CPUS_MASTER="${REDIS_CPUS_MASTER:-0,1}"
REDIS_CPUS_REPLICA="${REDIS_CPUS_REPLICA:-2,3}"
WAIT_SEC="${WAIT_SEC:-180}"
STEP_SLEEP="${STEP_SLEEP:-5}"
LOG_DIR="${LOG_DIR:-./logs}"
RESULT_DIR="${RESULT_DIR:-./results}"
MY_IP="$SERVER_HOST"
RC="redis-cli -h 127.0.0.1 -p $REDIS_PORT"
RC2="redis-cli -h 127.0.0.1 -p $REPLICA_PORT"
MODE=auto
CHECK_ONLY=0
case "${1:-}" in
    "") ;;
    --manual) MODE=manual ;;
    --check) CHECK_ONLY=1 ;;
    *) echo "用法: bash 185_master_test_opt4_guard.sh [--manual|--check]"; exit 2 ;;
esac
SAMPLER_PIDS=()
TS="$(date +%Y%m%d_%H%M%S)"

need() { command -v "$1" >/dev/null 2>&1; }
cleanup() { for p in "${SAMPLER_PIDS[@]:-}"; do kill "$p" 2>/dev/null || true; done; }
trap cleanup EXIT

mkdir -p "$LOG_DIR" "$RESULT_DIR"
# 全程回显同时备份一份到日志
LOG_MASTER="$LOG_DIR/master_run_$TS.log"
exec > >(tee -a "$LOG_MASTER") 2>&1

say()  { echo "===== $* ====="; }
info() { echo "      $*"; }

# 统一 ssh 到压测机。用 SSHPASS 环境变量传密码，避免密码出现在进程参数中。
remote() {
    if [ "$MODE" = "auto" ] && need sshpass && [ -n "$CLIENT_PASS" ]; then
        SSHPASS="$CLIENT_PASS" sshpass -e ssh \
            -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
            -o ConnectTimeout=10 -o NumberOfPasswordPrompts=1 \
            "${CLIENT_USER}@${CLIENT_HOST}" "$@"
    else
        ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 "${CLIENT_USER}@${CLIENT_HOST}" "$@"
    fi
}

# 步骤间固定停顿(保证命令排队/采样稳定)
step_gap() { sleep "$STEP_SLEEP"; }

#------------------- 0. 预检 -------------------
precheck() {
    say "[0] 预检 (脚本版本 $SCRIPT_VERSION)"
    need redis-cli   || { echo "[!!] 本机缺 redis-cli"; exit 1; }
    [ -x "$REDIS_SERVER_BIN" ] || { echo "[!!] 缺少已打补丁的Redis: $REDIS_SERVER_BIN"; exit 1; }
    need taskset     || { echo "[!!] 本机缺 taskset (util-linux)"; exit 1; }
    if ! [[ "$VALUE_LIMIT_BYTES" =~ ^[0-9]+$ ]] || [ "$VALUE_LIMIT_BYTES" -lt 2 ]; then
        echo "[!!] VALUE_LIMIT_BYTES必须是大于1的整数"; return 1
    fi
    if [ "$MODE" = "auto" ]; then
        if [ -n "$CLIENT_PASS" ] && ! need sshpass; then
            echo "[!!] 设置了 CLIENT_PASS，但本机缺 sshpass"
            return 1
        fi
        local remote_check="" remote_rc=0
        if [ -z "$CLIENT_PASS" ] || need sshpass; then
            remote_check=$(remote "test -x '$CLIENT_DIR/stress_test_opt4_guard' && '$CLIENT_DIR/stress_test_opt4_guard' --identify" 2>&1) || remote_rc=$?
        else
            remote_rc=127
            remote_check="本机没有 sshpass"
        fi
        if [ "$remote_rc" -eq 0 ] && [[ "$remote_check" == *"$EXPECTED_CLIENT_BANNER"* ]] && [[ "$remote_check" == *"$EXPECTED_CLIENT_PARAMS"* ]] && [[ "$remote_check" == *"$EXPECTED_CLIENT_MODE"* ]]; then
            info "压测机连通，压测程序版本和参数正确 -> 全自动(sshpass 密码登录)"
            info "$EXPECTED_CLIENT_BANNER  $EXPECTED_CLIENT_PARAMS"
        else
            echo "[!!] 压测机自动连接失败；自动模式终止，未重建Redis"
            echo "     目标: ${CLIENT_USER}@${CLIENT_HOST}"
            echo "     检查: $CLIENT_DIR/stress_test_opt4_guard 必须存在、可执行，且为本目录源码重新编译的版本"
            echo "     ssh返回码: $remote_rc"
            echo "     期望: $EXPECTED_CLIENT_BANNER  $EXPECTED_CLIENT_PARAMS  $EXPECTED_CLIENT_MODE"
            [ -n "$remote_check" ] && echo "     ssh输出: $remote_check"
            echo "     如需明确使用手动模式，请执行: bash 185_master_test_opt4_guard.sh --manual"
            return 1
        fi
    fi
    info "Redis服务IP=$MY_IP  目标压测机=$CLIENT_HOST  模式=$MODE  MAXMEM=$MAXMEM  链路=$NET_SPEED  主库绑核=$REDIS_CPUS_MASTER  备库绑核=$REDIS_CPUS_REPLICA  THREAD_MULTIPLIER=$LOAD_MULTIPLIER KEY_POOL=$KEY_POOL"
}

#------------------- 1. 重建主备 Redis -------------------
setup_redis() {
    say "[1] 重建 Redis 主($REDIS_PORT, 绑核$REDIS_CPUS_MASTER)+备($REPLICA_PORT, 绑核$REDIS_CPUS_REPLICA)"
    local overcommit old_master old_replica new_master new_replica
    overcommit=$(sysctl -n vm.overcommit_memory 2>/dev/null || echo unknown)
    if [ "$overcommit" != "1" ]; then
        info "vm.overcommit_memory=$overcommit，设置为1以保证Redis后台保存和主从复制"
        if [ "$(id -u)" -ne 0 ] || ! sysctl -w vm.overcommit_memory=1 >/dev/null 2>&1; then
            echo "[!!] 无法设置 vm.overcommit_memory=1，请使用root运行或先手动执行: sysctl -w vm.overcommit_memory=1"
            return 1
        fi
    fi
    old_master=$($RC INFO server 2>/dev/null | awk -F: '$1=="process_id"{gsub(/\r/,"",$2);print $2;exit}')
    old_replica=$($RC2 INFO server 2>/dev/null | awk -F: '$1=="process_id"{gsub(/\r/,"",$2);print $2;exit}')
    info "停止旧Redis: 主PID=${old_master:-无} 备PID=${old_replica:-无}"
    $RC2 SHUTDOWN NOSAVE >/dev/null 2>&1 || true
    $RC SHUTDOWN NOSAVE >/dev/null 2>&1 || true
    for _ in $(seq 1 10); do
        ! $RC PING >/dev/null 2>&1 && ! $RC2 PING >/dev/null 2>&1 && break
        sleep 1
    done
    if $RC PING >/dev/null 2>&1 && [ -n "${old_master:-}" ]; then kill "$old_master" 2>/dev/null || true; fi
    if $RC2 PING >/dev/null 2>&1 && [ -n "${old_replica:-}" ]; then kill "$old_replica" 2>/dev/null || true; fi
    sleep 1
    if $RC PING >/dev/null 2>&1 || $RC2 PING >/dev/null 2>&1; then
        echo "[!!] 旧Redis未停止，拒绝继续压测"
        return 1
    fi
    env JD_VALUE_LIMIT_BYTES="$VALUE_LIMIT_BYTES" taskset -c "$REDIS_CPUS_MASTER" "$REDIS_SERVER_BIN" --port "$REDIS_PORT" --daemonize yes \
        --maxmemory "$MAXMEM" --maxmemory-policy noeviction --save "" --appendonly no \
        --bind 0.0.0.0 --protected-mode no
    sleep 1
    env JD_VALUE_LIMIT_BYTES="$VALUE_LIMIT_BYTES" taskset -c "$REDIS_CPUS_REPLICA" "$REDIS_SERVER_BIN" --port "$REPLICA_PORT" --daemonize yes \
        --maxmemory "$MAXMEM" --save "" --appendonly no \
        --bind 0.0.0.0 --protected-mode no
    sleep 1
    $RC PING >/dev/null   || { echo "[!!] 主库启动失败"; return 1; }
    $RC2 PING >/dev/null  || { echo "[!!] 备库启动失败"; return 1; }
    new_master=$($RC INFO server 2>/dev/null | awk -F: '$1=="process_id"{gsub(/\r/,"",$2);print $2;exit}')
    new_replica=$($RC2 INFO server 2>/dev/null | awk -F: '$1=="process_id"{gsub(/\r/,"",$2);print $2;exit}')
    if [ -z "$new_master" ] || [ -z "$new_replica" ] || \
       { [ -n "${old_master:-}" ] && [ "$new_master" = "$old_master" ]; } || \
       { [ -n "${old_replica:-}" ] && [ "$new_replica" = "$old_replica" ]; }; then
        echo "[!!] Redis进程未正确重建，拒绝继续压测"
        return 1
    fi
    info "新Redis已确认: 主PID=$new_master 备PID=$new_replica"
    # 运行边界自检：证明185主库确实运行了服务端补丁，不接受普通Redis冒充。
    local probe_key="test:opt4:guard_probe" probe_reply probe_len probe_ttl
    probe_reply=$(head -c "$VALUE_LIMIT_BYTES" /dev/zero | $RC -x SET "$probe_key")
    if [[ "$probe_reply" != *JD_VALUE_LIMIT* ]]; then
        echo "[!!] 服务端SET边界保护未生效: $probe_reply"; $RC DEL "$probe_key" >/dev/null; return 1
    fi
    probe_reply=$(head -c "$((VALUE_LIMIT_BYTES-1))" /dev/zero | $RC -x SET "$probe_key")
    if [ "$probe_reply" != "OK" ]; then
        echo "[!!] 服务端合法SET被拒绝: $probe_reply"; $RC DEL "$probe_key" >/dev/null; return 1
    fi
    $RC EXPIRE "$probe_key" 10 >/dev/null
    probe_reply=$(printf x | $RC -x APPEND "$probe_key")
    probe_len=$($RC STRLEN "$probe_key")
    probe_ttl=$($RC TTL "$probe_key")
    $RC DEL "$probe_key" >/dev/null
    if [[ "$probe_reply" != *JD_VALUE_LIMIT* ]] || [ "$probe_len" -ne "$((VALUE_LIMIT_BYTES-1))" ] || [ "$probe_ttl" -le 0 ]; then
        echo "[!!] 服务端APPEND边界保护或拒绝后原值/TTL校验失败"; return 1
    fi
    info "服务端SET/APPEND边界保护已验证: 阈值=$VALUE_LIMIT_BYTES bytes"
    local io_threads io_reads
    io_threads=$($RC CONFIG GET io-threads 2>/dev/null | tail -1 | tr -d '\r')
    io_reads=$($RC CONFIG GET io-threads-do-reads 2>/dev/null | tail -1 | tr -d '\r')
    if [ "$io_threads" != "1" ] || [ "$io_reads" != "no" ]; then
        echo "[!!] 基线Redis配置错误: io-threads=$io_threads io-threads-do-reads=$io_reads（期望1/no）"
        return 1
    fi
    info "基线I/O线程配置已确认: io-threads=1 io-threads-do-reads=no"
    $RC2 REPLICAOF 127.0.0.1 "$REDIS_PORT" >/dev/null
    $RC CONFIG SET slowlog-log-slower-than 1000 >/dev/null
    $RC CONFIG SET slowlog-max-len 128 >/dev/null
    systemctl stop firewalld 2>/dev/null; systemctl disable firewalld 2>/dev/null
    info "主库:"; $RC INFO replication | grep -E 'role|connected_slaves'
    info "备库:"; $RC2 INFO replication | grep -E 'role|master_link_status'
    info "等待主从链路up且全量同步完成(最多120s)..."
    local repl_info repl_link sync_state
    repl_link=""; sync_state=""
    for _ in $(seq 1 24); do
        repl_info=$($RC2 INFO replication 2>/dev/null)
        repl_link=$(printf '%s\n' "$repl_info" | awk -F: '$1=="master_link_status"{gsub(/\r/,"",$2);print $2;exit}')
        sync_state=$(printf '%s\n' "$repl_info" | awk -F: '$1=="master_sync_in_progress"{gsub(/\r/,"",$2);print $2;exit}')
        [ "$repl_link" = "up" ] && [ "$sync_state" = "0" ] && break
        sleep 5
    done
    if [ "$repl_link" != "up" ] || [ "$sync_state" != "0" ]; then
        echo "[!!] 主从复制未在120秒内就绪: link=${repl_link:-unknown} sync=${sync_state:-unknown}"
        return 1
    fi
    info "主从复制已就绪: link=up sync=0"
    $RC2 INFO replication | grep -E 'master_link_status|master_sync_in_progress|master_repl_offset'
}

#------------------- 2. 清库 -------------------
flush_all() {
    say "[2] 清库(主备 FLUSHALL)"
    $RC FLUSHALL >/dev/null; $RC2 FLUSHALL >/dev/null
    info "已清空"
}

#------------------- 3. 启动监控采样 -------------------
start_samplers() {
    say "[3] 后台监控采样($LOG_DIR)"
    local pid rpid
    # ① redis 指标(每3s): OPS/过期/命中, 带当前phase标记(分阶段统计)
    ( while true; do
        t="$(date +%H:%M:%S)"
        ph=$($RC GET test:phase:marker 2>/dev/null | tr -d '\r')   # 读压测程序写的阶段标记
        $RC info stats 2>/dev/null | awk -v ts="$t" -v p="$ph" \
            '/^instantaneous_ops_per_sec|^expired_keys|^keyspace_hits|^keyspace_misses|^total_net_output_bytes/{print ts,"phase="p,$0}' \
            >> "$LOG_DIR/redis_info_$TS.log"
        $RC info commandstats 2>/dev/null | awk -v ts="$t" -v p="$ph" \
            '/^cmdstat_(mget|set|get|append):calls/{print ts,"phase="p,$0}' \
            >> "$LOG_DIR/redis_info_$TS.log"   # 分命令OPS: 每3s记各命令calls, collect按phase切分算增量
        sleep 3
      done ) & pid=$!; SAMPLER_PIDS+=($pid)
    # ② 实时OPS(--stat)
    ( $RC --stat > "$LOG_DIR/redis_stat_$TS.log" 2>/dev/null ) & pid=$!; SAMPLER_PIDS+=($pid)
    # ③ redis 主线程 CPU%(top -H 周期快照, 单核打满看这里; 快照式每次独立输出, 中途被杀也有数据)
    rpid=$($RC INFO server 2>/dev/null | awk -F: '/^process_id/{gsub(/[ \r]/,"",$2); print $2}')   # 用 INFO 拿PID(比pgrep可靠)
    [ -n "$rpid" ] || rpid=$(pgrep -f "redis-server.*--port $REDIS_PORT" | head -1)
    if [ -n "$rpid" ]; then
        if need pidstat; then
            ( while true; do
                ph=$($RC GET test:phase:marker 2>/dev/null | tr -d '\r')
                echo "==== pidstat-snap $(date +%H:%M:%S) phase=$ph ===="
                pidstat -t -p "$rpid" 1 1 2>/dev/null
                sleep 2
              done > "$LOG_DIR/top_h_$TS.log" ) & pid=$!; SAMPLER_PIDS+=($pid)
        else
            ( while true; do
                ph=$($RC GET test:phase:marker 2>/dev/null | tr -d '\r')
                echo "==== top-snap $(date +%H:%M:%S) phase=$ph ===="
                top -H -b -n 1 -p "$rpid" 2>/dev/null
                sleep 2
              done > "$LOG_DIR/top_h_$TS.log" ) & pid=$!; SAMPLER_PIDS+=($pid)
        fi
        info "主库redis pid=$rpid, 线程CPU采样已启动(每5s一张快照)"
    else
        echo "[!!] 未定位到主库redis进程, 跳过top采样; 当前redis进程:"
        pgrep -af redis-server 2>/dev/null || echo "  (无redis进程!)"
    fi
    # ④ 网卡吞吐
    # 主用 /proc/net/dev 每1s: 格式固定, 时间戳24h(无AM/PM/日期列漂移), 报告按字节差分类比
    ( while true; do
        echo "==== $(date +%H:%M:%S) ===="
        awk -F'[: ]+' '/^[[:space:]]*[a-zA-Z][a-zA-Z0-9]*:/{print $1, $2, $10}' /proc/net/dev 2>/dev/null   # 设备 收字节 发字节
        sleep 1
      done > "$LOG_DIR/net_dev_$TS.log" ) & pid=$!; SAMPLER_PIDS+=($pid)
    if need sar; then
        ( sar -n DEV 1 > "$LOG_DIR/sar_net_$TS.log" ) & pid=$!; SAMPLER_PIDS+=($pid)   # 仅供人工旁证, 不进报告
    fi
    info "采样已启动: redis_info/redis_stat/top_h/net_dev(主)/sar_net(旁证)"
}

#------------------- 4. 触发压测 -------------------
run_test() {
    say "[4] v2基线压测: 预热40万Key + 阶段2 30s + 阶段3 60s"
    local test_rc=0
    if [ "$MODE" = "auto" ]; then
        remote "cd '$CLIENT_DIR' && ./stress_test_opt4_guard '$MY_IP' '$REDIS_PORT' '$LOAD_MULTIPLIER' '$KEY_POOL' '$ABNORMAL_SET_SHARE' '$RANDOM_SEED'" \
            2>&1 | tee "$LOG_DIR/client_out_$TS.log"
        test_rc=${PIPESTATUS[0]}
        if [ "$test_rc" -ne 0 ]; then
            echo "[!!] 远端压测异常退出，ssh/程序返回码=$test_rc"
            echo "     已保存输出: $LOG_DIR/client_out_$TS.log"
        fi
    else
        echo "--------------------------------------------------------------"
        echo " 请在压测机($CLIENT_HOST)上手动执行 (本脚本等待 $WAIT_SEC 秒后自动继续, 或按回车):"
        echo "   cd $CLIENT_DIR && ./stress_test_opt4_guard $MY_IP $REDIS_PORT $LOAD_MULTIPLIER $KEY_POOL $ABNORMAL_SET_SHARE $RANDOM_SEED"
        echo " 若要收集[客户端超时]统计, 用这条代替(保存回显到压测机):"
        echo "   cd $CLIENT_DIR && ./stress_test_opt4_guard $MY_IP $REDIS_PORT $LOAD_MULTIPLIER $KEY_POOL $ABNORMAL_SET_SHARE $RANDOM_SEED 2>&1 | tee client_out.txt"
        echo " 并按回车前把压测机上 client_out.txt 拷到本机 $LOG_DIR/ 下(否则报告无超时统计)"
        echo "--------------------------------------------------------------"
        read -r -t "$WAIT_SEC" -p "压测机跑完后按回车收集... " || echo "(等待超时, 继续收集)"
    fi
    # 客户端已结束：立即退出阶段3，避免报告把后续收尾采样算进60秒负载。
    if ! $RC SET test:phase:marker 0 EX 120 >/dev/null 2>&1; then
        echo "[!] 无法重置阶段标记，收尾采样可能继续归到阶段3"
    fi
    sleep 2
    return "$test_rc"
}

#------------------- 5. 收集判定数据 -------------------
# 从 result_*.txt 生成 Markdown 版分阶段报告(result_*.md)
gen_md() {
    local OUTF="$1"; local MDF="${OUTF%.txt}.md"
    awk '
        function v(f,  i){ i=index(f,"="); return substr(f,i+1) }   # 局部变量i必须写进参数表(否则全局污染for循环的i)
        function phof(s,  p){ p=match(s,/[23]/); return substr(s,p,1) }
        /^  \[阶段[23]\] / {
            ph=phof($1)
            for(i=2;i<=NF;i++){
                if      (index($i,"最高单线程=")) c1[ph]=v($i)
                else if (index($i,"平均="))       c2[ph]=v($i)
                else if (index($i,"峰值RX="))     rx[ph]=v($i)
                else if (index($i,"峰值TX="))     tx[ph]=v($i)
                else if (index($i,"OPS峰值="))    ops[ph]=v($i)
                else if (index($i,"过期增量="))   ep[ph]=v($i)
                else if (index($i,"命中/未命中增量=")) hm[ph]=v($i)
                else if (index($i,"命中率="))     rt[ph]=v($i)
            }
        }
        END{
            print "# Redis 压测分阶段结果"
            print ""
            print "数据来源: " FILENAME
            print ""
            print "| 指标 | 阶段2（30s） | 阶段3（60s） |"
            print "|---|---|---|"
            printf "| CPU 最高/平均 | %s / %s | %s / %s |\n", c1[2], c2[2], c1[3], c2[3]
            printf "| OPS 峰值 | %s | %s |\n", ops[2], ops[3]
            printf "| 过期增量 | %s | %s |\n", ep[2], ep[3]
            printf "| 命中/未命中增量 | %s | %s |\n", hm[2], hm[3]
            printf "| 命中率 | %s | %s |\n", rt[2], rt[3]
            printf "| 网卡 RX/TX | %s / %s | %s / %s |\n", rx[2], tx[2], rx[3], tx[3]
            print ""
            print "> 口径: 阶段3 = 全量(90s) - 阶段2; 命中率 = hits/(hits+misses); 网卡=峰值kB/s(sar)"
        }
    ' "$OUTF" > "$MDF"
    echo "  Markdown报告已生成: $MDF"
}

collect() {
    say "[5] 收判定数据"
    local OUTF="$RESULT_DIR/result_$TS.txt"
    {
    echo "==========================================================="
    echo " Redis 问题复现 — 双节点压测结果   生成: $(date)"
    echo " 本机(服务器)=$MY_IP  压测机=$CLIENT_HOST"
    echo "==========================================================="

    # --- ① CPU 单核(top_h 里 redis 主线程最高%CPU) ---
    echo "[CPU]  redis 主线程最高CPU(%), top -H 采样:"
    local f_top="$(ls -t "$LOG_DIR"/top_h_*.log 2>/dev/null | head -1)"
    if [ -n "$f_top" ]; then
        awk 'BEGIN{m=-1}
             /%CPU/ { for(i=1;i<=NF;i++) if($i=="%CPU") c=i; next }      # 表头定位 %CPU 列(兼容12h制/TGID/top -H)
             !/Average/ && /redis-server/ && c { if($c+0>m)m=$c+0 }             # 跳过Average冗余行,避免%wait/列错位
             END{ if(m>0) print "  最高单线程%CPU = "m"%"; else print "  未能解析%CPU(见 top_h 原始文件)" }' "$f_top"
        echo "  (若≈100 = 单核打满, 符合文档 line 347)"
    else
        echo "  未找到 top_h 采样"
    fi

    # --- ② 网卡峰值 ---
    echo "[网卡] 吞吐:"
    local f_nd="$(ls -t "$LOG_DIR"/net_dev_*.log 2>/dev/null | head -1)"
    if [ -n "$f_nd" ]; then
        local netref; case "$NET_SPEED" in 25g) netref="25G≈3125000kB/s";; 10g) netref="万兆≈1130000kB/s";; *) netref="千兆≈113000kB/s";; esac
        awk -v ref="$netref" '
            /^==== / { split($2,tm,":"); sec=tm[1]*3600+tm[2]*60+tm[3]; next }
            NF==3 && $1!="lo" { dev=$1
                if(dev in prx){ dt=sec-psec[dev]; if(dt>0){ r=($2-prx[dev])/1024/dt; t=($3-ptx[dev])/1024/dt; if(r<0)r=0; if(t<0)t=0; if(r>rxm)rxm=r; if(t>txm)txm=t } }
                prx[dev]=$2; ptx[dev]=$3; psec[dev]=sec }
            END{ printf "  峰值 RX(服务器收/写方向)= %d kB/s   TX(服务器发/读响应方向)= %d kB/s  (%s)\n", int(rxm), int(txm), ref }' "$f_nd"
        echo "  (按 /proc/net/dev 每1s字节差分; 原始 sar 旁证见 sar_net_*.log)"
    else
        echo "  未找到 net_dev 采样"
    fi

    # --- ③ redis 指标演变 ---
    local f_info="$(ls -t "$LOG_DIR"/redis_info_*.log 2>/dev/null | head -1)"
    echo "[Redis] 指标:"
    if [ -n "$f_info" ]; then
        local ops_max e1 e2 h1 h2 m1 m2
        ops_max=$(grep -oE 'instantaneous_ops_per_sec:[0-9]+' "$f_info" | cut -d: -f2 | sort -n | tail -1)
        e1=$(grep -oE 'expired_keys:[0-9]+' "$f_info" | head -1 | cut -d: -f2)
        e2=$(grep -oE 'expired_keys:[0-9]+' "$f_info" | tail -1 | cut -d: -f2)
        h1=$(grep -oE 'keyspace_hits:[0-9]+' "$f_info" | head -1 | cut -d: -f2)
        h2=$(grep -oE 'keyspace_hits:[0-9]+' "$f_info" | tail -1 | cut -d: -f2)
        m1=$(grep -oE 'keyspace_misses:[0-9]+' "$f_info" | head -1 | cut -d: -f2)
        m2=$(grep -oE 'keyspace_misses:[0-9]+' "$f_info" | tail -1 | cut -d: -f2)
        echo "  峰值OPS=$ops_max  过期增量(写放大)=$(( ${e2:-0}-${e1:-0} ))  命中/未命中增=$(( ${h2:-0}-${h1:-0} ))/$(( ${m2:-0}-${m1:-0} ))"
    else
        echo "  未找到 redis_info 采样"
    fi

    # --- ④ 客户端统计 (兼容 .log 与手动 scp 成的 .txt)
    echo "[客户端] 统计:"
    local f_cli="$(ls -t "$LOG_DIR"/client_out* 2>/dev/null | head -1)"
    if [ -n "$f_cli" ]; then
        grep -E '\[阶段2结束\]|\[全部结束\]' "$f_cli"
        echo "  [客户端各命令] offered/success/error、RTT、命中和返回字节:"
        grep -E '^\[阶段[23] (MGET|SET|GET|APPEND)\]' "$f_cli" || true
        echo "  [客户端RTT(us)]"
        grep -E '\[[^]]*RTT\(us\)\]' "$f_cli" | tail -2
    else
        echo "  未找到 client_out(手动模式186回显丢失; 补救: 186压测时用 tee 保存回显为 client_out_*.log 并放到本机 $LOG_DIR/)"
    fi

    # --- ⑤ 大key检出 ---
    echo "[bigkeys] 大Key:"
    $RC --bigkeys > "$LOG_DIR/bigkeys_$TS.log" 2>/dev/null
    grep -E 'Sampled|Biggest string found' "$LOG_DIR/bigkeys_$TS.log" | tail -5

    # --- ⑥ 慢日志 ---
    echo "[slowlog] 单条大命令耗时(微秒, 第3个integer):"
    $RC SLOWLOG GET 8 2>/dev/null | head -40

    # --- ⑦ 分阶段统计(phase=2/3, 供阶段对比) ---
    echo ""
    echo "[分阶段对比] 阶段2 vs 阶段3 (由压测程序写入 test:phase:marker, 采样时打 phase 标签):"
    # CPU: 从 top_h 按 phase 分桶取 redis 主线程最高/平均
    if [ -n "${f_top:-}" ]; then
        for ph in 2 3; do
            read -r p_max p_avg < <(awk -v ph="$ph" '
                BEGIN{m=-1}
                /%CPU/ { for(i=1;i<=NF;i++) if($i=="%CPU") c=i; next }      # 表头定位 %CPU 列
                /phase=[0-9]/ { for(i=1;i<=NF;i++) if($i ~ /^phase=[0-9]/){ split($i,a,"="); cur=a[2] } }
                cur==ph && !/Average/ && /redis-server/ && c {
                    if($c+0>m)m=$c+0; s+=$c; n++
                }
                END{ if(n==0){ print 0,0; exit } print m, s/n }' "$f_top")
            echo "  [阶段$ph] CPU 最高单线程=${p_max}%  平均=${p_avg}%"
        done
    fi
    # 网卡: 用 redis_info 的时间锚点从 net_dev(每1s字节差分) 切分
    if [ -n "${f_nd:-}" ]; then
        s2=0; s3=0
        if [ -n "${f_info:-}" ]; then
            t2=$(grep 'phase=2' "$f_info" | head -1 | awk '{print $1}')
            t3=$(grep 'phase=3' "$f_info" | head -1 | awk '{print $1}')
            [ -n "$t2" ] && s2=$(awk -F: -v t="$t2" 'BEGIN{split(t,a,":"); print a[1]*3600+a[2]*60+a[3]}')
            [ -n "$t3" ] && s3=$(awk -F: -v t="$t3" 'BEGIN{split(t,a,":"); print a[1]*3600+a[2]*60+a[3]}')
        fi
        for ph in 2 3; do
            if [ "$ph" = "2" ]; then sA=$s2; sB=$s3; else sA=$s3; sB=999999; fi
            read -r rx tx < <(awk -v a="$sA" -v b="$sB" '
                /^==== / { split($2,tm,":"); sec=tm[1]*3600+tm[2]*60+tm[3]; next }
                NF==3 && $1!="lo" { dev=$1
                    if(dev in prx){ dt=sec-psec[dev]; if(dt>0){ r=($2-prx[dev])/1024/dt; t=($3-ptx[dev])/1024/dt; if(r<0)r=0; if(t<0)t=0
                        if(sec>=a && (b==999999||sec<b)){ if(r>rxm)rxm=r; if(t>txm)txm=t } } }
                    prx[dev]=$2; ptx[dev]=$3; psec[dev]=sec }
                END{ printf "%d %d", int(rxm), int(txm) }' "$f_nd")
            echo "  [阶段$ph] 网卡 峰值RX=${rx:-0}kB/s  峰值TX=${tx:-0}kB/s"
        done
    fi
    # Redis指标: 按 phase 分桶取 OPS/过期/命中 增量
    if [ -n "${f_info:-}" ]; then
        for ph in 2 3; do
            seg=$(grep "phase=$ph" "$f_info")
            if [ -n "$seg" ]; then
                ops=$(echo "$seg" | grep -oE 'instantaneous_ops_per_sec:[0-9]+' | cut -d: -f2 | sort -n | tail -1)
                e1=$(echo "$seg" | grep -oE 'expired_keys:[0-9]+' | head -1 | cut -d: -f2); e2=$(echo "$seg" | grep -oE 'expired_keys:[0-9]+' | tail -1 | cut -d: -f2)
                h1=$(echo "$seg" | grep -oE 'keyspace_hits:[0-9]+' | head -1 | cut -d: -f2); h2=$(echo "$seg" | grep -oE 'keyspace_hits:[0-9]+' | tail -1 | cut -d: -f2)
                m1=$(echo "$seg" | grep -oE 'keyspace_misses:[0-9]+' | head -1 | cut -d: -f2); m2=$(echo "$seg" | grep -oE 'keyspace_misses:[0-9]+' | tail -1 | cut -d: -f2)
                hit=$(( ${h2:-0}-${h1:-0} )); mis=$(( ${m2:-0}-${m1:-0} ))
                rate=$(awk -v h="$hit" -v m="$mis" 'BEGIN{ if(h+m>0) printf "%.2f%%", 100*h/(h+m); else printf "-" }')
                echo "  [阶段$ph] OPS峰值=$ops  过期增量=$(( ${e2:-0}-${e1:-0} ))  命中/未命中增量=$hit/$mis  命中率=$rate"
            fi
        done
    fi
    # 分命令OPS: 从 commandstats(redis_info 里的 cmdstat_* 行)按 phase 切分, calls增量/阶段时长
    if [ -n "${f_info:-}" ]; then
        echo "  [分命令OPS] (INFO commandstats, 按phase切分 calls增量/时长):"
        awk '
            $2 ~ /^phase=[23]$/ && $3 ~ /^cmdstat_(mget|set|get|append):calls=/ {
                split($2,p,"="); ph=p[2]
                split($3,a,","); split(a[1],c,":"); cm=c[1]; sub(/^cmdstat_/,"",cm)
                split(c[2],d,"="); calls=d[2]+0
                split($1,t,":"); sec=t[1]*3600+t[2]*60+t[3]
                k=cm ph
                if(!(k in has)){ fcall[k]=calls; fsec[k]=sec; has[k]=1 }
                lcall[k]=calls; lsec[k]=sec
            }
            END{
                for(k in has){ ph=substr(k,length(k),1); cm=substr(k,1,length(k)-1)
                    dt=lsec[k]-fsec[k]; inc=lcall[k]-fcall[k]
                    printf "  [阶段%s] %-6s OPS≈%d (calls增量=%d)\n", ph, cm, (dt>0?inc/dt:0), inc }
            }' "$f_info" | sort -k2,3
    fi
    } > "$OUTF"
    cat "$OUTF"
    gen_md "$OUTF"
    echo "[ok] 报告已写: $OUTF"
}

#------------------- 入口(各步骤间固定 sleep + 全程回显已备份到 master_run 日志) -------------------
precheck || exit 1
if [ "$CHECK_ONLY" -eq 1 ]; then
    echo "===== 预检通过：185 可以自动登录186并找到压测程序；未修改Redis ====="
    exit 0
fi
step_gap
setup_redis || exit 1; step_gap
flush_all;  step_gap
start_samplers; step_gap
run_test || exit 1; step_gap
collect;    step_gap
cleanup
echo "===== 测试流程结束 (全程回显已备份: $LOG_MASTER) ====="
