# MiniOB 可视化文档

`docs/docs` 下全部 64 篇 Markdown 的静态可视化站点：每篇正文 + 离线结构导览与思维导图 + 右侧常驻目录，4 张确定性子系统架构图与 13 个关键机制的可交互 SVG 流程图，首页全站知识地图与搜索。

原始文档不被修改；本目录中的 `*.html` 与 `assets/diagrams/*.svg` 均由 `build.py` 和 `draw_diagrams.py` 生成。

## 生成与校验

```bash
pip install -r docs/visual/requirements.txt
python3 docs/visual/draw_diagrams.py
python3 docs/visual/build.py

# 使用 drawing-toolkit 校验全部生成的 SVG
for f in docs/visual/assets/diagrams/*.svg; do
  python3 ~/.codex/skills/drawing-toolkit/scripts/validate_svg.py "$f" --forbid-raster --forbid-gradients --forbid-external
done
```

## 浏览

推荐从仓库根目录启动本地服务，这样正文引用的图片可以直接访问：

```bash
python3 -m http.server 8765
# 打开 http://127.0.0.1:8765/docs/visual/
```

也可以直接打开 `docs/visual/index.html`（`file://`）：架构图、13 个交互流程图、章节结构导览卡片均为内联确定性 SVG / HTML，**完全离线可用**；Markmap 思维导图、Highlight.js、MathJax 作为可选增强从 jsDelivr 加载。

## 结构

- `draw_diagrams.py`：遵循 `~/.codex/skills/drawing-toolkit` 规范的确定性 SVG 生成器（标准库 `xml.etree.ElementTree` 实现，区分 Architecture 模式与 Pipeline 模式），输出 17 张独立 SVG 至 `assets/diagrams/` 并为 HTML 提供内联交互图层。
- `build.py`：扫描 `docs/docs/**/*.md`，生成页面、三栏阅读布局（左侧导航 + 正文 + 右侧常驻 TOC）、语义提示框、全站地图、搜索索引。
- `flows.json`：13 个流程的节点、边、步骤与文档边界说明；改完重跑 `build.py`。
- `assets/`：样式（含深浅色主题 SVG 适配）、交互脚本、主题引导脚本及 `assets/diagrams/*.svg`。
- `index.html`、`flows.html`、`design/*.html` 等：生成物，不要手改。

## 流程数据

每个流程对应一篇设计文档和其中的标题锚点：

```json
{
  "id": "wal-flush",
  "title": "流程标题",
  "source": "design/miniob-durability.md",
  "anchor": "设计与实现",
  "summary": "一句话概括",
  "nodes": [{"id": "a", "label": "节点"}],
  "edges": [["a", "b", "边标签"]],
  "steps": [{"title": "步骤", "desc": "说明", "highlight": ["a"]}],
  "uncertainties": ["文档未决点"]
}
```

流程只描述文档已经写明的行为；`uncertainties` 会原样展示在播放器底部，不把设计描述包装成已验证事实。

## 已知边界

- 三处链接指向源仓库中不存在或未生成的产物：`dev_by_gitpod.md`（源文档缺失）、`doxygen` HTML（需另行生成）。
- 架构图、13 个交互流程图与章节结构卡片均无外部运行时依赖（离线与 `file://` 直接可用）；仅可选的 Markmap 思维导图视图、代码语法高亮与 MathJax 数学公式从 CDN 加载。
- 流程是人工从文档提炼的，不是自动抽取；跨多节的流程只链接到其中一节，并在 `uncertainties` 中注明。
