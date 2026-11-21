#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

AUTO_FORMAT="${AUTO_FORMAT:-1}"
RUN_STATIC_ANALYSIS="${RUN_STATIC_ANALYSIS:-1}"
RUN_UNIT_TESTS="${RUN_UNIT_TESTS:-1}"
RUN_SQL_CONCURRENCY="${RUN_SQL_CONCURRENCY:-1}"
RUN_TPCC="${RUN_TPCC:-0}"
TPCC_FAIL_ON_REGRESSION="${TPCC_FAIL_ON_REGRESSION:-0}"
CTEST_EXCLUDE="${CTEST_EXCLUDE:-memtracer_test|disk_log_handler_test|mvcc_trx_log_test|bplus_tree_log_test|bplus_tree_test|buffer_pool_log_test|log_file_test|record_manager_test|ob_compaction_test|ob_lsm_test}"

NPROC="$(nproc 2>/dev/null || echo 4)"
BASELINE_DIR="$ROOT_DIR/.cache/tpcc_baseline"
mkdir -p "$BASELINE_DIR"

echo "============================================================"
echo "[pre-commit] MiniOB 本地提交前门禁检查"
echo "============================================================"

# 1. Collect staged files
STAGED_FILES="$(git diff --cached --name-only --diff-filter=ACM || true)"
STAGED_CPP="$(echo "$STAGED_FILES" | grep -E '\.(h|hpp|c|cc|cpp)$' | grep -vE '^(deps/3rd/|src/observer/sql/parser/(lex|yacc)_sql\.)' || true)"
STAGED_PY="$(echo "$STAGED_FILES" | grep -E '\.py$' || true)"
STAGED_YML="$(echo "$STAGED_FILES" | grep -E '\.(yml|yaml)$' || true)"

# 2. Format (clang-format)
if [ -n "$STAGED_CPP" ]; then
  if command -v clang-format >/dev/null 2>&1; then
    echo "[1/5] 运行 clang-format 检查与格式化..."
    if [ "$AUTO_FORMAT" = "1" ]; then
      echo "$STAGED_CPP" | xargs -r clang-format -i --style=file
      echo "$STAGED_CPP" | xargs -r git add
      echo "  -> 已自动格式化并重新暂存 C/C++ 文件"
    else
      echo "$STAGED_CPP" | xargs -r clang-format --dry-run --Werror --style=file
    fi
  else
    echo "[1/5] 跳过 clang-format (未安装 clang-format)"
  fi
else
  echo "[1/5] 无暂存 C/C++ 文件，跳过 clang-format"
fi

# 3. Static checks (clang-tidy, cppcheck, py_compile, yaml)
if [ "$RUN_STATIC_ANALYSIS" = "1" ]; then
  echo "[2/5] 运行静态检查..."
  if [ -n "$STAGED_CPP" ]; then
    if command -v clang-tidy >/dev/null 2>&1 && [ -f "$ROOT_DIR/compile_commands.json" ]; then
      echo "  -> clang-tidy (compile_commands.json)"
      echo "$STAGED_CPP" | xargs -r clang-tidy -p "$ROOT_DIR" --quiet || {
        echo "ERROR: clang-tidy 静态检查未通过"
        exit 1
      }
    else
      echo "  -> 跳过 clang-tidy (未安装或缺少 compile_commands.json)"
    fi

    if command -v cppcheck >/dev/null 2>&1; then
      echo "  -> cppcheck"
      echo "$STAGED_CPP" | xargs -r cppcheck --enable=warning,performance,portability --error-exitcode=1 --inline-suppr --quiet || {
        echo "ERROR: cppcheck 静态检查未通过"
        exit 1
      }
    fi
  fi

  if [ -n "$STAGED_PY" ]; then
    echo "  -> python3 py_compile"
    echo "$STAGED_PY" | xargs -r python3 -m py_compile
  fi

  if [ -n "$STAGED_YML" ]; then
    echo "  -> YAML 语法检查"
    python3 -c "import sys, yaml; [yaml.safe_load(open(f, encoding='utf-8')) for f in sys.argv[1:]]" $STAGED_YML
  fi
else
  echo "[2/5] 已跳过静态检查 (RUN_STATIC_ANALYSIS=0)"
fi

# 4. Incremental Build + Unit Tests
if [ "$RUN_UNIT_TESTS" = "1" ]; then
  echo "[3/5] 编译并运行单元测试 (build_debug + ctest)..."
  if [ -f "$ROOT_DIR/build_debug/CMakeCache.txt" ]; then
    cmake --build "$ROOT_DIR/build_debug" -j"$NPROC"
  else
    bash "$ROOT_DIR/build.sh" debug -DCONCURRENCY=ON --make -j"$NPROC"
  fi
  ctest --test-dir "$ROOT_DIR/build_debug" -E "$CTEST_EXCLUDE" --output-on-failure
else
  echo "[3/5] 已跳过单元测试 (RUN_UNIT_TESTS=0)"
fi

# Pick observer binary for SQL & TPC-C tests (prefer Release if available, else Debug)
OBSERVER_BIN="$ROOT_DIR/build_release/bin/observer"
if [ -f "$ROOT_DIR/build_release/CMakeCache.txt" ]; then
  echo "[4/5] 增量编译 Release observer..."
  cmake --build "$ROOT_DIR/build_release" --target observer -j"$NPROC"
elif [ ! -x "$OBSERVER_BIN" ]; then
  OBSERVER_BIN="$ROOT_DIR/build_debug/bin/observer"
fi

if [ ! -x "$OBSERVER_BIN" ]; then
  echo "ERROR: 未找到可执行文件 observer ($OBSERVER_BIN)"
  exit 1
fi

# 5. Real SQL concurrency tests
if [ "$RUN_SQL_CONCURRENCY" = "1" ]; then
  echo "[4/5] 运行真实 SQL 并发测试 (mvcc_concurrency_test.py)..."
  python3 "$ROOT_DIR/test/case/concurrency/mvcc_concurrency_test.py" --observer "$OBSERVER_BIN"
else
  echo "[4/5] 已跳过并发 SQL 测试 (RUN_SQL_CONCURRENCY=0)"
fi

# 6. TPC-C correctness & performance tests
if [ "$RUN_TPCC" = "1" ]; then
  echo "[5/5] 运行 TPC-C 正确性与性能测试..."
  TMP_REPORT_DIR="$(mktemp -d)"
  trap 'rm -rf "$TMP_REPORT_DIR"' EXIT

  python3 "$ROOT_DIR/test/tpcc/miniob_tpcc.py" \
    --observer "$OBSERVER_BIN" \
    --engine lsm --trx lsm --threads 1 --transactions 200 --seed 42 \
    --output-json "$TMP_REPORT_DIR/head_det.json"

  python3 "$ROOT_DIR/test/tpcc/miniob_tpcc.py" \
    --observer "$OBSERVER_BIN" \
    --engine lsm --trx lsm --threads 4 --transactions 400 --seed 42 \
    --output-json "$TMP_REPORT_DIR/head_conc.json"

  COMPARE_EXTRA=()
  if [ "$TPCC_FAIL_ON_REGRESSION" = "1" ]; then
    COMPARE_EXTRA+=(--fail-on-regression)
  fi

  python3 "$ROOT_DIR/test/tpcc/compare_tpcc.py" \
    --head "$TMP_REPORT_DIR/head_det.json" \
    --base "$BASELINE_DIR/det.json" \
    --label "Local TPC-C Deterministic (1-Thread)" \
    --check-digest \
    "${COMPARE_EXTRA[@]}"

  python3 "$ROOT_DIR/test/tpcc/compare_tpcc.py" \
    --head "$TMP_REPORT_DIR/head_conc.json" \
    --base "$BASELINE_DIR/conc.json" \
    --label "Local TPC-C Concurrent MVCC (4-Thread)" \
    "${COMPARE_EXTRA[@]}"

  # Update local baseline after correctness checks pass
  cp "$TMP_REPORT_DIR/head_det.json" "$BASELINE_DIR/det.json"
  cp "$TMP_REPORT_DIR/head_conc.json" "$BASELINE_DIR/conc.json"
else
  echo "[5/5] 已跳过 TPC-C 测试 (默认跳过，自测可设 RUN_TPCC=1)"
fi

echo "============================================================"
echo "[pre-commit] 全部检查通过，允许提交！"
echo "============================================================"
