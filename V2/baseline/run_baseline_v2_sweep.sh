#!/usr/bin/env bash
# 在185上按0.5、0.75、1.0、1.25倍逐轮测试；每轮重建Redis并单独归档。
set -uo pipefail
cd "$(dirname "$0")" || exit 1
START_MULTIPLIER="${START_MULTIPLIER:-0.5}"
STEP_MULTIPLIER="${STEP_MULTIPLIER:-0.25}"
MAX_MULTIPLIER="${MAX_MULTIPLIER:-1.25}"
awk -v a="$START_MULTIPLIER" -v b="$STEP_MULTIPLIER" -v c="$MAX_MULTIPLIER" 'BEGIN{exit !(a>0 && b>0 && c>=a)}' || {
    echo "[!!] START/STEP/MAX_MULTIPLIER非法"; exit 2;
}
i=0
while :; do
    mult=$(awk -v a="$START_MULTIPLIER" -v b="$STEP_MULTIPLIER" -v i="$i" 'BEGIN{printf "%.2f",a+b*i}')
    awk -v x="$mult" -v y="$MAX_MULTIPLIER" 'BEGIN{exit !(x<=y+0.0001)}' || break
    echo "===== 基线v2容量阶梯 x$mult ====="
    LOAD_MULTIPLIER="$mult" bash ./collect_baseline_v2_metrics.sh || {
        echo "[!!] x$mult 失败，停止后续测试；保留已有轮次的数据"; exit 1;
    }
    i=$((i+1))
done
echo "===== 线程阶梯完成。按阶段3成功OPS、错误率、RTT>5ms比例和命中率判断可持续负载。====="
