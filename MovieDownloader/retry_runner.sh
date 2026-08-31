#!/usr/bin/env bash
# =============================================================================
# retry_runner.sh —— 瞬时失败 ID 的循环重跑捞回驱动脚本（不定轮 + 真无源永久剔除）
#
# 设计要点：
#   1. 全程在独立的 retry/ 目录内工作，绝不触碰主目录的 results.jsonl / fail.txt / run.log。
#   2. 每轮一个独立子目录 round1/round2/...，各放一份 config（仅改 input/output/fail_file）
#      + 软链主脚本，靠脚本原生的 load_processed_ids 去重，实现"本轮范围内"跳过已处理。
#   3. 核心优化：每轮跑完后从本轮日志区分两类失败——
#        - 真无源（日志打 "[无源 404]"）→ 累积进 dead_ids.txt，永久排除，绝不再重抓；
#        - 瞬时错误（日志打 "attempts failed for ID"）→ 作为下一轮输入。
#      因此下一轮输入 = 本轮 fail.txt 扣除 dead_ids.txt，只保留仍可能有源的 ID。
#   4. 不定轮循环：直到某轮"瞬时失败"清零（下一轮输入为空）或达到 MAX_ROUNDS 上限。
#      终止即代表：所有有源 ID 已捞干净，剩下的全是真无源。
#   5. 结束后合并各轮 results 去重成 retry_recovered.jsonl，并打印每轮捞回数量。
#
# 用法（在 /mnt/MovieAndTVDownload/MovieDownloader/ 下）：
#   bash retry_runner.sh
#   nohup 版：nohup bash retry_runner.sh > retry_runner.out 2>&1 &
#
# 幂等：可重复运行；已完成的轮次结果不会被清空重跑（脚本去重会跳过已处理 ID）。
#   如需彻底重来，先 rm -rf retry/ 再执行。
# =============================================================================
set -euo pipefail

# ---- 路径锚定到脚本所在目录（主工作目录） ----
BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$BASE_DIR"

MAIN_SCRIPT="$BASE_DIR/tmdb_ids_to_links.py"
MAIN_CONFIG="$BASE_DIR/config.yaml"
RUN_LOG="$BASE_DIR/run.log"
PYTHON="$BASE_DIR/.venv/bin/python"
RETRY_DIR="$BASE_DIR/retry"
MAX_ROUNDS=8   # 兜底上限，防止极端情况下死循环

# ---- 前置检查 ----
for f in "$MAIN_SCRIPT" "$MAIN_CONFIG" "$RUN_LOG"; do
    [[ -f "$f" ]] || { echo "[FATAL] 缺少必需文件: $f" >&2; exit 1; }
done
[[ -x "$PYTHON" || -f "$PYTHON" ]] || { echo "[FATAL] 找不到 venv python: $PYTHON" >&2; exit 1; }

mkdir -p "$RETRY_DIR"

# ---- 全局"真无源"累积表：一旦被判定真无源，永久排除，绝不再重抓 ----
DEAD_IDS="$RETRY_DIR/dead_ids.txt"
touch "$DEAD_IDS"

# ---- 第 0 步：提取瞬时失败种子 ID（仅首轮 input 不存在时才提取，保证幂等） ----
SEED="$RETRY_DIR/round1_input.txt"
if [[ ! -s "$SEED" ]]; then
    echo "==> 从 run.log 提取瞬时失败 ID（attempts failed for ID）..."
    grep -oE "attempts failed for ID [0-9]+" "$RUN_LOG" \
        | grep -oE "[0-9]+" | sort -u > "$SEED"
fi
SEED_COUNT=$(wc -l < "$SEED" | tr -d ' ')
echo "==> 种子瞬时失败 ID 数量: $SEED_COUNT"

# ---- 生成某一轮的 config（仅改三处路径，其余原样继承主 config） ----
make_round_config() {
    local round_dir="$1" in_file="$2" out_file="$3" fail_file="$4"
    "$PYTHON" - "$MAIN_CONFIG" "$round_dir/config.yaml" \
        "$in_file" "$out_file" "$fail_file" <<'PYEOF'
import sys, yaml
src, dst, in_f, out_f, fail_f = sys.argv[1:6]
with open(src, encoding="utf-8") as f:
    cfg = yaml.safe_load(f)
sec = cfg.get("tmdb_ids_to_links", {})
sec["input"] = in_f
sec["output"] = out_f
sec["fail_file"] = fail_f
cfg["tmdb_ids_to_links"] = sec
with open(dst, "w", encoding="utf-8") as f:
    yaml.safe_dump(cfg, f, allow_unicode=True, sort_keys=False)
PYEOF
}

# ---- 逐轮循环执行 ----
prev_input="$SEED"
r=0
while :; do
    r=$((r + 1))
    if [[ "$r" -gt "$MAX_ROUNDS" ]]; then
        echo ""
        echo "==> 已达最大轮数 $MAX_ROUNDS，停止循环（剩余失败 ID 视为难以捞回，按真无源处理）。"
        break
    fi

    round_dir="$RETRY_DIR/round$r"
    mkdir -p "$round_dir"

    in_file="$round_dir/input.txt"
    out_file="$round_dir/results.jsonl"
    fail_file="$round_dir/fail.txt"

    # 本轮输入 = 上一轮筛出的"纯瞬时失败"ID（首轮=种子）
    cp "$prev_input" "$in_file"
    in_count=$(wc -l < "$in_file" | tr -d ' ')

    echo ""
    echo "======================================================================"
    echo "==> 第 $r 轮开始 | 输入 $in_count 个 ID | 目录 $round_dir"
    echo "======================================================================"

    if [[ "$in_count" -eq 0 ]]; then
        echo "==> 第 $r 轮无待处理 ID → 瞬时失败已清零，所有有源 ID 已捞干净，正常结束。"
        rmdir "$round_dir" 2>/dev/null || true
        r=$((r - 1))
        break
    fi

    # 每轮独立 config + 软链主脚本，cd 进目录跑（脚本内 config 以 __file__ 同目录为准）
    make_round_config "$round_dir" "$in_file" "$out_file" "$fail_file"
    ln -sf "$MAIN_SCRIPT" "$round_dir/tmdb_ids_to_links.py"

    (
        cd "$round_dir"
        "$PYTHON" tmdb_ids_to_links.py 2>&1 | tee "run_round$r.log"
    )

    got=$([[ -f "$out_file" ]] && wc -l < "$out_file" | tr -d ' ' || echo 0)

    # ---- 从本轮日志提取真无源 ID（[无源 404]），累积进全局 dead_ids ----
    round_log="$round_dir/run_round$r.log"
    round_dead="$round_dir/dead_this_round.txt"
    grep -oE "\[无源 404\] [0-9]+" "$round_log" 2>/dev/null \
        | grep -oE "[0-9]+" | sort -u > "$round_dead" || true
    dead_this=$(wc -l < "$round_dead" | tr -d ' ')
    # 合并进全局 dead 表（去重）
    sort -u "$DEAD_IDS" "$round_dead" -o "$DEAD_IDS"
    dead_total=$(wc -l < "$DEAD_IDS" | tr -d ' ')

    # ---- 下一轮输入 = 本轮 fail.txt 扣除所有已知真无源 ----
    next_input="$round_dir/next_input.txt"
    if [[ -f "$fail_file" ]]; then
        sort -u "$fail_file" > "$round_dir/_fail_sorted.txt"
        # comm -23：在 fail 中但不在 dead 中的 → 纯瞬时失败
        comm -23 "$round_dir/_fail_sorted.txt" "$DEAD_IDS" > "$next_input" || true
        rm -f "$round_dir/_fail_sorted.txt"
    else
        : > "$next_input"
    fi
    next_count=$(wc -l < "$next_input" | tr -d ' ')

    echo "==> 第 $r 轮完成 | 本轮捞回 $got | 本轮新增真无源 $dead_this（累计 $dead_total）| 下一轮瞬时失败 $next_count"

    prev_input="$next_input"
done

TOTAL_ROUNDS=$r

# ---- 合并所有轮成功结果，按 tmdbId 去重 ----
echo ""
echo "======================================================================"
echo "==> 合并 $TOTAL_ROUNDS 轮结果并去重..."
RECOVERED="$RETRY_DIR/retry_recovered.jsonl"
"$PYTHON" - "$RETRY_DIR" "$RECOVERED" "$TOTAL_ROUNDS" <<'PYEOF'
import sys, os, json
retry_dir, out_path, rounds = sys.argv[1], sys.argv[2], int(sys.argv[3])
seen = set()
total_lines = 0
per_round = {}
with open(out_path, "w", encoding="utf-8") as out:
    for r in range(1, rounds + 1):
        p = os.path.join(retry_dir, f"round{r}", "results.jsonl")
        cnt = 0
        if os.path.exists(p):
            with open(p, encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        obj = json.loads(line)
                    except Exception:
                        continue
                    tid = str(obj.get("tmdbId"))
                    if tid in seen:
                        continue
                    seen.add(tid)
                    out.write(line + "\n")
                    cnt += 1
        per_round[r] = cnt
        total_lines += cnt
print("==> 各轮净新增（去重后）：")
for r in range(1, rounds + 1):
    print(f"    round{r}: {per_round.get(r,0)}")
print(f"==> 合并去重后总捞回: {total_lines} 条 -> {out_path}")
PYEOF

DEAD_FINAL=$(wc -l < "$DEAD_IDS" | tr -d ' ')
echo ""
echo "==> 全部完成。共跑 $TOTAL_ROUNDS 轮。"
echo "    最终捞回文件: $RECOVERED"
echo "    累计判定真无源: $DEAD_FINAL 个 -> $DEAD_IDS"
echo "    如需并入主结果: cat \"$RECOVERED\" >> \"$BASE_DIR/results.jsonl\"  （并入前建议先备份 results.jsonl）"
