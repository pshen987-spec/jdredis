#!/usr/bin/env bash
# 依次运行原始基线和7种优化版本的数据采集脚本。
set -uo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
MODE_ARG="${1:-}"
case "$MODE_ARG" in
    ""|--manual|--check) ;;
    *)
        echo "用法: bash collect_all_cases.sh [--manual|--check]"
        exit 2
        ;;
esac

CASES=(
    "00_baseline/collect_baseline_metrics.sh"
    "01_client_cache/collect_opt1_metrics.sh"
    "02_bigkey_guard/collect_opt2_metrics.sh"
    "03_mget_pipeline/collect_opt3_metrics.sh"
    "04_redis_tuning/collect_opt4_metrics.sh"
    "05_soft_ttl/collect_opt5_metrics.sh"
    "06_io_threads/collect_opt6_metrics.sh"
    "07_all_optimized/collect_opt7_metrics.sh"
)

for relative in "${CASES[@]}"; do
    [ -f "$SCRIPT_DIR/$relative" ] || {
        echo "[!!] 缺少采集脚本: $SCRIPT_DIR/$relative"
        exit 1
    }
done

RUN_ID=$(date +%Y%m%d_%H%M%S)
SUMMARY="$SCRIPT_DIR/all_cases_summary_$RUN_ID.txt"
FAILED=0

{
    echo "全部场景数据采集"
    echo "开始时间: $(date '+%F %T')"
    echo "模式: ${MODE_ARG:-auto}"
    echo "场景数: ${#CASES[@]}"
    echo
} | tee "$SUMMARY"

for relative in "${CASES[@]}"; do
    name=${relative%%/*}
    script="$SCRIPT_DIR/$relative"
    echo "============================================================" | tee -a "$SUMMARY"
    echo "[$name] 开始: $(date '+%F %T')" | tee -a "$SUMMARY"

    if [ -n "$MODE_ARG" ]; then
        bash "$script" "$MODE_ARG" 2>&1 | tee -a "$SUMMARY"
        rc=${PIPESTATUS[0]}
    else
        bash "$script" 2>&1 | tee -a "$SUMMARY"
        rc=${PIPESTATUS[0]}
    fi

    if [ "$rc" -eq 0 ]; then
        echo "[$name] 成功" | tee -a "$SUMMARY"
    else
        echo "[$name] 失败，返回码=$rc；继续下一个场景" | tee -a "$SUMMARY"
        FAILED=$((FAILED+1))
    fi
done

{
    echo "============================================================"
    echo "结束时间: $(date '+%F %T')"
    echo "失败场景数: $FAILED"
    echo "总汇总文件: $SUMMARY"
} | tee -a "$SUMMARY"

[ "$FAILED" -eq 0 ]
