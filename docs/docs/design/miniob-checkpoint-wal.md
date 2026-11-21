---
title: MiniOB 检查点与 WAL 截断
---

# MiniOB 检查点与 WAL 截断

状态：**部分实现**。`Db::sync` 现在执行安全的“记录恢复起点”检查点；
**WAL 截断未实现**，所有 clog 文件保留。

## 1. 完整流程与当前实现

标准的 checkpoint 流程是：

1. 冻结/记录水位：暂停新写入（或加检查点锁），记录目标 `checkpoint_lsn`。
2. MVCC 回填与清理：`MvccGc::collect` 把已提交的 `-xid` 改写成
   `commit_seq`，清理所有活跃快照都不可见的死版本。
3. 全量刷脏页：把所有 buffer pool 脏页写入数据/索引文件。
4. 持久化屏障：对数据文件、double write buffer、元数据文件执行 `fsync`。
5. 原子更新元数据：把新 `checkpoint_lsn` 写入控制文件，`fsync` 控制文件
   及其目录。
6. 截断日志：确认元数据已持久化后，删除 `LSN < checkpoint_lsn` 的旧 WAL。

当前 `Db::sync()` 实现了 1 的一半、2、3、4 和 5；**第 6 步没有实现**。
它并不假装做了 WAL 截断：`LogFileManager` 没有 `remove/truncate` 接口，
所有日志文件都会保留。

### 1.1 冻结/水位

`MvccTrxKit` 有一把 `std::shared_mutex checkpoint_gate_`：

- `MvccTrx::start_if_need()` 持 shared lock；
- `Db::sync()` 持 unique lock，因此检查点期间不会启动新事务。

`Db::sync()` 在 unique lock 内检查 `all_trxes()`，只要还有 `started()` 的
MVCC 事务，就**刷页但不推进 checkpoint**。这样不需要无限等待长事务，也不会
把未提交事务修改过的页当作稳定检查点。非 MVCC 引擎没有这个 gate，仍依赖
调用方保证 sync 期间没有并发写。

目标 LSN 在刷页**之前**采样：

```text
target_lsn = log_handler->current_lsn()   // before flushing
flush tables -> dblwr -> fsync
wait_lsn(target_lsn)
meta.checkpoint_lsn = target_lsn
```

旧实现先刷页、再取 `current_lsn`，会把刷页期间提交的事务包含进 checkpoint，
但这些事务的页可能尚未刷出，恢复时又因 LSN 小于 checkpoint 被跳过。
先采样后刷页消除了这个窗口；target 之后提交的事务 LSN 必然更大，会在下次
恢复时重放。

### 1.2 回填与 GC

`Db::sync()` 先执行 `MvccGc::collect`：

- 已提交但仍是 `-T` 的 `begin/end` 改写成 `commit_seq`；
- `end` 为 ABORTED 的删除标记恢复为 INF（旧版本仍存活）；
- 删除所有已启动快照都不可见的版本及其索引项。

GC 与普通读写仍可并发；GC 的可见性由最老活跃快照水位保证，页面访问依赖
buffer pool 的 page latch。`Db::sync()` 有 mutex，避免多个会话同时跑 GC。

### 1.3 刷脏页与 fsync

`Table::sync()` 把数据页和索引页 flush 到全局 double write buffer；
`DiskDoubleWriteBuffer::flush_page()` 先 `fsync` dblwr，再把页写入数据文件；
随后 `BufferPoolManager::fsync_all()` 对所有数据/索引 buffer pool 文件
`fsync`；再刷新一次 dblwr 的 invalid 标记并 `fsync`。

这一步是 checkpoint 元数据之前的持久化屏障。当前仍未覆盖：
LSM 自己的 `.sst/.wal` 持久化语义、表元数据 `.table` 文件的 fsync。

### 1.4 控制文件

`Db::flush_meta()` 写 `sys.db.tmp`，`fsync` 文件，rename 为 `sys.db`，再
`fsync` 数据库目录。`sys.db` 当前是文本：

```text
<checkpoint_lsn> <persisted_commit_seq>
```

旧格式只有第一个数字，仍可读取。控制文件原子替换依赖 rename 的同目录原子性；
没有 double-buffer/控制页校验。

## 2. WAL 截断：为什么没有实现

当前 API 只提供 `list_files/last_file/next_file`，不提供删除。恢复从
`check_point_lsn` 开始重放；保留全部 WAL 能保证即使 checkpoint 元数据被误写，
仍有日志可回退重放。反过来，一旦提前删除日志，任何 checkpoint/meta 问题都会
变成不可恢复的数据丢失。

因此：

- `Db::sync()` 的注释明确写着不删除 WAL；
- `LogFileManager` 注释明确没有 remove/truncate；
- 检查点只改变恢复起点，不减少日志占用。

正确实现截断至少需要：

1. 检查点元数据已经 fsync（含目录）；
2. 数据页/索引页已经 fsync；
3. 没有活动事务的新写入可能跨越 checkpoint；
4. 只删除“最后一个 LSN < checkpoint_lsn”的完整日志文件；
5. 保留包含 `checkpoint_lsn` 的文件，绝不删除正在写的 last file；
6. 删除后执行目录 `fsync`；
7. 在“删除日志后重启恢复”的测试通过之前保持默认关闭。

建议的 `LogFileManager::remove_old_files(checkpoint_lsn)` 判据：

```text
for file in sorted(log_files):
    if file is last/active file: break
    if next_file.first_lsn <= checkpoint_lsn:
        remove(file)          # next file still covers checkpoint
```

删除前必须有进程级 checkpoint 锁，且不能在日志 writer 持文件句柄时移除当前
文件。当前没有这个接口，也没有对应故障注入测试。

## 3. 恢复保证与失败矩阵

> **关键限制**：当前 `LogFileWriter::write()` 只调用 `write(2)`，没有
> `fsync/fdatasync`；`DiskLogHandler::wait_lsn()` 等待的是“日志已写入”，
> 不是“日志已持久化”。因此下面的矩阵是**进程崩溃/同机 page cache 仍在**的
> 语义；掉电或内核缓存丢失时，尚未 fsync 的 COMMIT 日志可能丢失。
> 这也是 WAL 截断必须先解决的前置问题。

在“所有 WAL 保留”的前提下：

| 崩溃点                        | 结果                                                            |
| ----------------------------- | --------------------------------------------------------------- |
| 刷页前/刷页中                 | meta 仍是旧 checkpoint，从旧位置重放；未提交事务由 on_done 回滚 |
| dblwr 写入后、数据 fsync 前   | 页可能旧/新混合；旧 checkpoint 或 dblwr 恢复路径重做            |
| 数据 fsync 后、meta rename 前 | meta 旧；重放 target 之后的日志，已刷页重做幂等                 |
| meta rename 后、目录 fsync 前 | 可能旧 meta 或新 meta；两者都有完整 WAL，可重放                 |
| meta 持久化后                 | 数据页已 fsync，恢复从 target 开始；target 之后日志仍保留并重放 |

若以后加入 WAL 截断，上表必须改成“日志少于 target 时仍可恢复”的证明，并以
故障注入测试为准；不能只靠代码审阅。


### Db::sync() process:
```
Db::sync()
 1. 拿 sync_lock_、MVCC checkpoint_gate_（阻止新事务 start）
 2. MvccGc::collect()             // 回填 -xid、清理死版本
 3. target_lsn = current_lsn()    // 先采样
 4. 检查活跃 MVCC 事务              // 有则 can_advance_checkpoint=false
 5. 每个 Table::sync()
      -> HeapTableEngine::sync()
         -> data_buffer_pool_->flush_all_pages()
         -> index->sync()
      -> 每页 flush_page_internal()
         -> add_page() 写入 dblwr 文件
 6. dblwr_buffer_->flush_page()
      -> fsync(dblwr file)
      -> 把 dblwr 页写入数据/索引文件
      -> 标记 dblwr 页 invalid
 7. buffer_pool_manager_->fsync_all()   // fsync 所有数据/索引文件
 8. dblwr_buffer_->flush_page()         // 再 fsync invalid 标记
 9. wait_lsn(target_lsn)                // 确保 target 之前 WAL 落盘
10. check_point_lsn_ = target_lsn
11. flush_meta():
      write sys.db.tmp
      fsync(tmp fd)
      rename -> sys.db
      fsync(db directory)
```

### Recovery process:

```
Db::init()
  open_all_tables()
  init_dblwr_buffer()
    -> dblwr.load_pages()        // 读 dblwr 中 checksum 有效的页
    -> dblwr.recover() = flush_page()
        // 把 dblwr 页写回数据/索引文件
  recover()
    -> log_handler.replay(checkpoint_lsn)
        -> RecordLogReplayer:
             if page.lsn >= log_entry.lsn: skip
             else redo INIT_PAGE/INSERT/DELETE/UPDATE
        -> MvccTrxLogReplayer: 重建事务状态/操作
    -> log_handler.start()
    -> log_replayer.on_done()
        // 没有 COMMIT 的事务 rollback
    -> MvccGc: 修复/回收 -T 死版本
```
```html
<!DOCTYPE html>
<html lang="zh-CN">
<head>
<script id="artifact-state-bridge">
(function() {
'use strict';
if (window.pageVault) {
  return;
}
// Single-reporter guard: the generic form-snapshot script checks this flag and
// stays inert.
window.__GEMINI_STATE_PRESERVER_INSTALLED__ = true;

const MAX_BYTES = 64 * 1024;
const POST_INTERVAL_MS = 100;
const ECHO_MEMORY = 8;

let map = {};
let hostSeed = {};
let isReady = false;
let resolveReady = null;
const readyPromise = new Promise(function(r) {
  resolveReady = r;
});
let queued = [];
const pending = {};
const restoreCallbacks = [];
const recentReports = [];
let postTimer = null;
let postDue = false;
// Write gate. Becomes true on the first trusted user input in this frame.
// Until then nothing is posted to the host.
let userActivated = false;

function hasOwn(o, k) {
  return Object.prototype.hasOwnProperty.call(o, k);
}
function isPlainObject(v) {
  return v !== null && typeof v === 'object' && !Array.isArray(v);
}
function clone(v) {
  return v === undefined ? undefined : JSON.parse(JSON.stringify(v));
}
function makeError(code, msg) {
  const e = new Error('pageVault: ' + msg);
  e.code = code;
  return e;
}
function byteLength(s) {
  if (typeof TextEncoder === 'function') {
    return new TextEncoder().encode(s).length;
  }
  return s.length;
}
function deepEqual(a, b) {
  if (a === b) {
    return true;
  }
  if (a === null || b === null || typeof a !== 'object' ||
      typeof b !== 'object') {
    return false;
  }
  const aArr = Array.isArray(a), bArr = Array.isArray(b);
  if (aArr !== bArr) {
    return false;
  }
  if (aArr) {
    if (a.length !== b.length) {
      return false;
    }
    for (let i = 0; i < a.length; i++) {
      if (!deepEqual(a[i], b[i])) {
        return false;
      }
    }
    return true;
  }
  const ka = Object.keys(a), kb = Object.keys(b);
  if (ka.length !== kb.length) {
    return false;
  }
  for (let j = 0; j < ka.length; j++) {
    if (!hasOwn(b, ka[j]) || !deepEqual(a[ka[j]], b[ka[j]])) {
      return false;
    }
  }
  return true;
}

function toSeedMap(raw) {
  if (raw === undefined || raw === null) {
    return null;
  }
  if (typeof raw === 'string') {
    if (raw.trim() === '') {
      return {};
    }
    try {
      raw = JSON.parse(raw);
    } catch (e) {
      return {};
    }
  }
  if (!isPlainObject(raw)) {
    return {};
  }
  try {
    return JSON.parse(JSON.stringify(raw));
  } catch (e) {
    return {};
  }
}
function unwrapSnapshotEnvelope(seed) {
  if (seed && seed.version === 1 && isPlainObject(seed.forms) &&
      isPlainObject(seed.scroll) && hasOwn(seed, 'custom')) {
    return isPlainObject(seed.custom) ? seed.custom : {};
  }
  return seed;
}
function takeSeed(raw) {
  const seed = toSeedMap(raw);
  if (seed === null) {
    return;
  }
  applySeed(
      unwrapSnapshotEnvelope(seed), typeof raw === 'string' ? raw : undefined);
}
function clearConfirmedPending(seed) {
  const keys = Object.keys(pending);
  for (let i = 0; i < keys.length; i++) {
    const k = keys[i];
    if (pending[k].removed) {
      if (!hasOwn(seed, k)) {
        delete pending[k];
      }
    } else if (hasOwn(seed, k) && deepEqual(pending[k].value, seed[k])) {
      delete pending[k];
    }
  }
}
function applySeed(seed, rawString) {
  if (!isReady) {
    map = seed;
    hostSeed = clone(seed);
    isReady = true;
    drainQueue(Object.keys(seed));
    resolveReady();
    return;
  }
  if (rawString !== undefined && recentReports.indexOf(rawString) !== -1) {
    hostSeed = clone(seed);
    clearConfirmedPending(seed);
    return;
  }
  if (deepEqual(seed, hostSeed)) {
    clearConfirmedPending(seed);
    if (userActivated && Object.keys(pending).length > 0) {
      schedulePost();
    }
    return;
  }
  const next = {}, changed = [];
  let k, i;
  const seedKeys = Object.keys(seed);
  for (i = 0; i < seedKeys.length; i++) {
    k = seedKeys[i];
    if (hasOwn(pending, k)) {
      if (pending[k].removed) {
        continue;
      }
      next[k] = map[k];
      if (deepEqual(pending[k].value, seed[k])) {
        delete pending[k];
      }
    } else if (hasOwn(hostSeed, k) && deepEqual(hostSeed[k], seed[k])) {
      next[k] = hasOwn(map, k) ? map[k] : seed[k];
    } else {
      next[k] = seed[k];
      if (!hasOwn(map, k) || !deepEqual(map[k], seed[k])) {
        changed.push(k);
      }
    }
  }
  const mapKeys = Object.keys(map);
  for (i = 0; i < mapKeys.length; i++) {
    k = mapKeys[i];
    if (hasOwn(next, k) || hasOwn(seed, k)) {
      continue;
    }
    if ((hasOwn(pending, k) && !pending[k].removed) || !hasOwn(hostSeed, k)) {
      next[k] = map[k];
    } else {
      changed.push(k);
    }
  }
  const pk = Object.keys(pending);
  for (i = 0; i < pk.length; i++) {
    if (pending[pk[i]].removed && !hasOwn(seed, pk[i])) {
      delete pending[pk[i]];
    }
  }
  hostSeed = clone(seed);
  if (userActivated && Object.keys(pending).length > 0) {
    schedulePost();
  }
  if (changed.length === 0) {
    return;
  }
  map = next;
  changed.sort();
  const cbs = restoreCallbacks.slice();
  for (i = 0; i < cbs.length; i++) {
    try {
      cbs[i](changed.slice());
    } catch (e) {
      console.warn('pageVault: onRestore callback failed', e);
    }
  }
}

function normalizeValue(value) {
  let text;
  try {
    text = JSON.stringify(value);
  } catch (e) {
    text = undefined;
  }
  if (typeof text !== 'string') {
    return {
      ok: false,
      error: makeError('NOT_SERIALIZABLE', 'value is not JSON-serializable')
    };
  }
  return {ok: true, value: JSON.parse(text)};
}
function applySave(key, value) {
  if (hasOwn(map, key) && deepEqual(map[key], value)) {
    if (userActivated &&
        (!hasOwn(hostSeed, key) || !deepEqual(hostSeed[key], value)) &&
        !(hasOwn(pending, key) && !pending[key].removed &&
          deepEqual(pending[key].value, value))) {
      trackEdit(key, {value: clone(value)});
    }
    return clone(value);
  }
  const next = Object.assign({}, map);
  next[key] = value;
  if (byteLength(JSON.stringify(next)) > MAX_BYTES) {
    throw makeError('TOO_LARGE', 'saved data would exceed 64 KB');
  }
  map = next;
  trackEdit(key, {value: clone(value)});
  return clone(value);
}
function applyRemove(key) {
  if (!hasOwn(map, key)) {
    if (userActivated && hasOwn(hostSeed, key) &&
        !(hasOwn(pending, key) && pending[key].removed)) {
      trackEdit(key, {removed: true});
    }
    return false;
  }
  const next = Object.assign({}, map);
  delete next[key];
  map = next;
  trackEdit(key, {removed: true});
  return true;
}
// Marks a local edit as pending and schedules a post. Before the first
// trusted user input an edit only changes `map`: it is not pending, so a seed
// that arrives later replaces it and onRestore reports the key.
function trackEdit(key, entry) {
  if (!userActivated) {
    return;
  }
  pending[key] = entry;
  schedulePost();
}
function drainQueue(seededKeys) {
  const ops = queued;
  queued = [];
  for (let i = 0; i < ops.length; i++) {
    const op = ops[i];
    try {
      if (op.op === 'save') {
        if (seededKeys.indexOf(op.key) !== -1) {
          op.resolve(clone(map[op.key]));
        } else {
          op.resolve(applySave(op.key, op.value));
        }
      } else {
        if (seededKeys.indexOf(op.key) !== -1) {
          op.resolve(false);
        } else {
          op.resolve(applyRemove(op.key));
        }
      }
    } catch (e) {
      op.reject(e);
    }
  }
}

function schedulePost() {
  if (!isReady) {
    return;
  }
  postDue = true;
  if (postTimer !== null) {
    return;
  }
  postTimer = setTimeout(flush, POST_INTERVAL_MS);
}
function flush() {
  if (postTimer !== null) {
    clearTimeout(postTimer);
    postTimer = null;
  }
  if (!postDue || !isReady || !userActivated) {
    return;
  }
  postDue = false;
  let payload;
  try {
    payload = JSON.stringify(map);
  } catch (e) {
    return;
  }
  recentReports.push(payload);
  if (recentReports.length > ECHO_MEMORY) {
    recentReports.shift();
  }
  try {
    window.parent.postMessage(
        {
          'type': 'WIDGET_STATE_UPDATE',
          'widgetType': 'artifact',
          'payload': payload
        },
        '*');
  } catch (e) { /* host unreachable: keep the in-memory session */
  }
}
window.addEventListener('pagehide', flush);
window.addEventListener('blur', flush);
if (typeof document !== 'undefined' && document.addEventListener) {
  document.addEventListener('visibilitychange', function() {
    if (document.visibilityState === 'hidden') {
      flush();
    }
  });
}

// The write gate opens on the first trusted pointer, touch, key, wheel,
// clipboard, form input or drop event in this frame, so startup code,
// onRestore callbacks and in-memory migrations cannot post on their own.
// Capture-phase listeners on `window` run before the page's handlers, so a
// save made while handling that same input is posted. Opening the gate posts
// nothing by itself: the next save posts the whole map, including earlier
// in-memory edits.
const USER_INPUT_EVENTS = [
  'pointerdown', 'mousedown', 'touchstart', 'keydown', 'input', 'change',
  'click', 'paste', 'cut', 'drop', 'wheel'
];
function onUserInput(event) {
  if (!event.isTrusted) {
    return;
  }
  userActivated = true;
  for (let i = 0; i < USER_INPUT_EVENTS.length; i++) {
    window.removeEventListener(USER_INPUT_EVENTS[i], onUserInput, true);
  }
}
for (let i = 0; i < USER_INPUT_EVENTS.length; i++) {
  window.addEventListener(
      USER_INPUT_EVENTS[i], onUserInput, {capture: true, passive: true});
}

window.addEventListener('message', function(event) {
  if (event.source !== window.parent) {
    return;
  }
  const d = event.data;
  if (!d || typeof d !== 'object') {
    return;
  }
  if (d.type === 'SUPPLEMENTAL_DATA_UPDATE') {
    takeSeed(d.data);
  } else if (d.type === 'RESTORE_STATE') {
    takeSeed(d.state);
  }
});
window.__onRestoreState = function(state) {
  takeSeed(state);
};
window.__getWidgetState = function() {
  return clone(map);
};

const api = {
  ready: readyPromise,
  load: function(key) {
    return readyPromise.then(function() {
      return (typeof key === 'string' && hasOwn(map, key)) ? clone(map[key]) :
                                                             undefined;
    });
  },
  save: function(key, value) {
    if (typeof key !== 'string' || key === '') {
      return Promise.reject(
          makeError('INVALID_KEY', 'key must be a non-empty string'));
    }
    const n = normalizeValue(value);
    if (!n.ok) {
      return Promise.reject(n.error);
    }
    if (!isReady) {
      return new Promise(function(resolve, reject) {
        queued.push({
          op: 'save',
          key: key,
          value: n.value,
          resolve: resolve,
          reject: reject
        });
      });
    }
    try {
      return Promise.resolve(applySave(key, n.value));
    } catch (e) {
      return Promise.reject(e);
    }
  },
  remove: function(key) {
    if (typeof key !== 'string') {
      return Promise.resolve(false);
    }
    if (!isReady) {
      return new Promise(function(resolve, reject) {
        queued.push({op: 'remove', key: key, resolve: resolve, reject: reject});
      });
    }
    return Promise.resolve(applyRemove(key));
  },
  keys: function() {
    return readyPromise.then(function() {
      return Object.keys(map);
    });
  },
  onRestore: function(cb) {
    if (typeof cb !== 'function') {
      return function() {};
    }
    restoreCallbacks.push(cb);
    return function() {
      const i = restoreCallbacks.indexOf(cb);
      if (i !== -1) {
        restoreCallbacks.splice(i, 1);
      }
    };
  }
};
Object.freeze(api);
window.pageVault = api;

// `ready` resolves right away, so the page never waits on the host. A host
// that embeds the saved data before this script runs hands it over through
// `window.__artifact_seed`. Without an embedded seed the page starts from an
// empty session. Saved data that the host delivers later (through
// SUPPLEMENTAL_DATA_UPDATE or RESTORE_STATE) still arrives: applySeed merges
// it, keeps the keys the page has written since the first user input, and
// reports the changed keys through onRestore.
const embeddedSeed = window.__artifact_seed;
if (embeddedSeed !== undefined && embeddedSeed !== null) {
  takeSeed(embeddedSeed);
} else {
  takeSeed({});
}
})();

</script>
<style id="lumi-runtime">
/* luminous.css */
/*
 * Luminous runtime stylesheet — injected once into every generated widget's
 * <head> by the C++ widget agent (id="lumi-runtime"). Single Source of Truth
 * for ALL design tokens and pre-baked .lumi-* component classes. The SI prompts
 * (design.md / html_generation.jinja2 / chart.jinja2) must NOT re-declare these
 * tokens or component CSS; they only reference them.
 *
 * Sections:
 *   1. Web fonts (Google Sans Flex + Google Sans Code)
 *   2. Light token :root (from previewer tokens.css, verbatim)
 *   3. Dark token overrides — REMAPPED to :root so canvas/D3 readers of
 *      getComputedStyle(document.documentElement) resolve dark values (D3).
 *   4. Short diagram/chart aliases (bridge legacy --surface / --chart-N names).
 *   5. In-scope pre-baked .lumi-* component classes.
 */

/* ============================================================
 * 1. WEB FONTS & ICONOGRAPHY
 *    Loads Google Sans Flex, Google Sans Code, and Material Symbols Outlined.
 *    Enforces the dual-class iconography rendering protocol.
 * ============================================================ */
@import url('https://fonts.googleapis.com/css2?family=Google+Sans+Flex:ital,wght@0,300..700;1,300..700&family=Google+Sans+Code:ital,wght,MONO@0,300..700,0..1;1,300..700,0..1&family=Material+Symbols+Outlined:opsz,wght,FILL,GRAD@20..48,100..700,0..1,-50..200&display=swap');

/* 
```
## 4. 验证

- `unittest/observer/mvcc_recovery_atomic_test.cpp`
  - `checkpoint_survives_without_wal_files`：提交 → `db->sync()` →
    拷贝数据库 → 删除全部 clog 文件 → 重开仍读到已提交行；
  - 同时断言 `sync` 前后 WAL 文件数量不变（当前不截断）；
  - `active_transaction_blocks_checkpoint_advance`：有活跃 MVCC 事务时
    `sync` 成功但我刷页不推进 `checkpoint_lsn`（读控制文件验证）。
- `mvcc_xid_test / mvcc_commit_atomic_test / mvcc_update_test /
  mvcc_gc_test / occ_trx_test / trx_isolation_test`。
- SQL `update_index/update_heap/update_btree/update_lsm/trx_semantics/
  basic/mvcc_update`。
- 并发压力：`test/case/concurrency/mvcc_concurrency_test.py`。
- 全量 `mvcc_trx_log_test` 通过。

## 5. 已知边界

- 没有 WAL 截断；磁盘日志会持续增长。
- **WAL 没有 fsync**：`wait_lsn` 只保证 write，掉电可能丢未 fsync 的 COMMIT；
  没有 WAL 尾部半写检测/截断。这是当前最重要的持久化缺口。
- 没有全局 freeze；有活跃 MVCC 事务时跳过 checkpoint，而不是等待。
- 非 MVCC 引擎没有 checkpoint gate，仍依赖调用方 quiesce。
- 表元数据与 LSM 文件没有纳入统一 fsync 屏障。
- `sys.db` 没有控制页 checksum/双副本；rename 原子性依赖文件系统。
- WAL 删除 API 与“删除后恢复”故障注入测试均未实现。

## 6. 参考与技能检索

按用户要求检索了 `~/.codex/skills`：

- `.system/` 下是 skill-creator、imagegen、openai-docs、review-agent 等通用工具；
- `tom-jerr-blog-writing` 限定 `E:/blog` 的 MkDocs/Zensical 正式博客；
- `drawing-toolkit` 用于 SVG/图表。

没有 checkpoint/WAL/数据库存储相关 skill。本文按本仓库
`docs/DESIGN_RULES.md` 与现有设计文档的事实/证据格式编写，没有套用博客技能。
