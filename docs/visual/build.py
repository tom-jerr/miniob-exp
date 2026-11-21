#!/usr/bin/env python3
"""Build the visual MiniOB docs site into docs/visual.

Reads every docs/docs/**/*.md, renders it to static HTML with:
- Deterministic SVG architecture & interactive pipeline/flow diagrams generated
  by draw_diagrams.py (following ~/.codex/skills/drawing-toolkit)
- Three-column reading layout with sticky right-hand Table of Contents
- Offline-first visual section tree + optional Markmap mindmap view
- Semantic callout boxes for GitHub alerts ([!WARNING], [!NOTE], etc.)
- Dark/light theme compatibility and keyboard-accessible interactive flow player

No files under docs/docs are modified.

Usage: python3 docs/visual/build.py
"""

from __future__ import annotations

import html as html_lib
import json
import os
import posixpath
import re
from pathlib import Path

import yaml
from markdown_it import MarkdownIt

import draw_diagrams

ROOT = Path(__file__).resolve().parents[2]
DOCS = ROOT / "docs" / "docs"
OUT = Path(__file__).resolve().parent

FLOWS = json.loads((OUT / "flows.json").read_text(encoding="utf-8"))
MD_BY_NAME: dict[str, list[str]] = {}
for _path in DOCS.rglob("*.md"):
    MD_BY_NAME.setdefault(_path.name, []).append(_path.relative_to(DOCS).as_posix())

GROUPS = [
    ("", "快速开始", "编译、运行与项目入口"),
    ("design", "设计文档", "架构、SQL、存储、事务与恢复"),
    ("db_course_lab", "课程实验", "从零实现数据库的 LAB0–LAB4"),
    ("lectures", "简明教程", "数据库实现原理与实践讲义"),
    ("dev-env", "开发环境", "Docker、VS Code、调试与测试"),
    ("game", "大赛与训练", "训练营、输出约定与工具"),
    ("blog", "博客", "社区文章"),
]
GROUP_LABEL = {key: label for key, label, _ in GROUPS}
GROUP_DESC = {key: desc for key, _, desc in GROUPS}

# Map design docs to supplementary architecture diagrams from draw_diagrams.py
DOC_ARCH_DIAGRAMS: dict[str, list[tuple[str, str, str]]] = {
    "design/miniob-persistence-files.md": [
        (
            "miniob-persistence-architecture",
            "MiniOB 存储文件、缓冲池与 WAL / Double Write Buffer 恢复架构",
            "内存 Buffer Pool、WAL 缓冲、TxnStatusTable 与磁盘控制文件 sys.db、表数据 *.data、索引 *.index、双写缓冲 dblwr.db 的整体协作关系。",
        )
    ],
    "design/miniob-durability.md": [
        (
            "miniob-persistence-architecture",
            "MiniOB 持久化与日志恢复子系统架构",
            "CLog (WAL) 与 Buffer Pool、Record Manager、B+Tree、MVCC 事务状态表及磁盘文件的分层关系。",
        )
    ],
    "design/miniob-update-mvcc-occ.md": [
        (
            "miniob-mvcc-occ-architecture",
            "MiniOB UPDATE、MVCC 与 OCC 版本存储及原子提交架构",
            "PostgreSQL 风格追加写多版本元组（8 字节 begin_xid/end_xid）、二级索引保留与新增、TxnStatusTable 原子提交点与 MvccGc 水位回收。",
        )
    ],
    "design/miniob-transaction.md": [
        (
            "miniob-mvcc-occ-architecture",
            "MiniOB 事务管理（MVCC / OCC）核心架构",
            "事务快照（snapshot_seq）、可见性判定、多版本元组标记与原子提交状态表。",
        )
    ],
    "design/miniob-btree-storage-engine.md": [
        (
            "miniob-btree-clustered-architecture",
            "MiniOB B+Tree 聚簇表存储引擎与四种 Key 编码架构",
            "对比 ENGINE=btree 聚簇表（COMPOSITE / ROWID 叶子存整行）与二级索引（ATTR_MEMCMP_VALUE / ATTR_RID）的编码及回表路径。",
        )
    ],
}

GENERIC_H1 = {"背景", "简介", "介绍", "background", "overview", "introduction"}

FRONTMATTER_RE = re.compile(r"\A---[ \t]*\n(.*?)\n---[ \t]*\n", re.S)
ATTR_RE = re.compile(r"""(?<![\w-])(?P<attr>href|src)\s*=\s*(?P<q>["'])(?P<url>.*?)(?P=q)""")
SCHEME_RE = re.compile(r"^(?:[a-zA-Z][a-zA-Z0-9+.-]*:|//)")
# Accidental widget-runtime HTML dump in miniob-checkpoint-wal.md
POLLUTED_HTML_RE = re.compile(
    r"```html\s*<!DOCTYPE html>\s*<html[^>]*>\s*<head>\s*<script id=\"artifact-state-bridge\">.*?```",
    re.S,
)

md = MarkdownIt("commonmark", {"html": True}).enable(["table", "strikethrough"])


def load_nav_order() -> dict[str, int]:
    """Order pages the way mkdocs.yml does; docs absent from nav go last."""
    try:
        cfg = yaml.safe_load((ROOT / "docs" / "mkdocs.yml").read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError):
        return {}
    order: dict[str, int] = {}

    def walk(items):
        for item in items or []:
            if isinstance(item, str):
                order.setdefault(item, len(order))
            elif isinstance(item, dict):
                for value in item.values():
                    if isinstance(value, str):
                        order.setdefault(value, len(order))
                    elif isinstance(value, list):
                        walk(value)

    walk(cfg.get("nav") if isinstance(cfg, dict) else [])
    return order


NAV_ORDER = load_nav_order()


def slugify(text: str) -> str:
    text = text.strip().lower()
    text = re.sub(r"[^\w\u4e00-\u9fff\-\s]", "", text)
    return re.sub(r"[\s-]+", "-", text).strip("-") or "section"


def inline_text(token) -> str:
    parts = []
    for child in token.children or []:
        if child.type in ("text", "code_inline", "image"):
            parts.append(child.content)
        elif child.type in ("softbreak", "hardbreak"):
            parts.append(" ")
        elif child.children:
            parts.append(inline_text(child))
    return "".join(parts).strip()


def split_frontmatter(source: str) -> tuple[dict, str]:
    match = FRONTMATTER_RE.match(source)
    if not match:
        return {}, source
    meta = {}
    for line in match.group(1).splitlines():
        key, sep, value = line.partition(":")
        if sep:
            meta[key.strip()] = value.strip().strip("\"'")
    return meta, source[match.end():]


def clean_markdown_source(rel: str, body: str) -> str:
    """Clean known formatting glitches without touching docs/docs source files."""
    if "artifact-state-bridge" in body:
        body = POLLUTED_HTML_RE.sub("", body)
    return body


def site_href(page_rel: str, target_rel: str) -> str:
    return posixpath.relpath(target_rel, posixpath.dirname(page_rel) or ".")


def output_rel(rel: str) -> str:
    return "index.html" if rel == "index.md" else rel[:-3] + ".html"


def source_link(page_rel: str, rel: str) -> str:
    target = os.path.relpath(DOCS / rel, (OUT / page_rel).parent)
    return target.replace(os.sep, "/")


def arch_figure_html(
    page_rel: str,
    diag_name: str,
    title: str,
    caption: str,
    fig_id: str = "",
) -> str:
    svg_markup = draw_diagrams.inline_svg_by_name(diag_name)
    svg_href = site_href(page_rel, f"assets/diagrams/{diag_name}.svg")
    id_attr = f' id="{fig_id}"' if fig_id else ""
    return f"""
<figure class="arch-figure"{id_attr}>
  <div class="arch-figure-head">
    <div>
      <span class="arch-kicker">ARCHITECTURE DIAGRAM · DETERMINISTIC SVG</span>
      <h3>{html_lib.escape(title)}</h3>
      <p>{html_lib.escape(caption)}</p>
    </div>
    <div class="arch-actions">
      <button type="button" class="arch-zoom-btn" data-zoom-diagram aria-label="放大查看架构图">⤢ 放大检视</button>
      <a class="arch-svg-link" href="{svg_href}" target="_blank" rel="noopener">SVG 源文件 ↗</a>
    </div>
  </div>
  <div class="arch-canvas">{svg_markup}</div>
</figure>"""


def enhance_callouts(html_body: str) -> str:
    """Transform GitHub-style alerts and semantic blockquotes into callout cards."""
    alert_map = {
        "NOTE": ("note", "说明 · NOTE"),
        "TIP": ("tip", "技巧 · TIP"),
        "IMPORTANT": ("important", "重要 · IMPORTANT"),
        "WARNING": ("warning", "警告 · WARNING"),
        "CAUTION": ("warning", "注意 · CAUTION"),
    }

    def repl_gh_alert(match: re.Match) -> str:
        kind_key = match.group(1).upper()
        rest = match.group(2).lstrip()
        role, badge = alert_map.get(kind_key, ("note", kind_key))
        return (
            f'<div class="callout callout-{role}" role="note">'
            f'<div class="callout-title">{badge}</div>'
            f'<div class="callout-body"><p>{rest}</div></div>'
        )

    html_body = re.sub(
        r"<blockquote>\s*<p>\[!(NOTE|TIP|IMPORTANT|WARNING|CAUTION)\]\s*(.*?)</blockquote>",
        repl_gh_alert,
        html_body,
        flags=re.S | re.I,
    )

    # Also upgrade > Q: ... or > 重要提示：... or > 注意：... or > 关键限制：...
    def repl_semantic_quote(match: re.Match) -> str:
        inner = match.group(1).strip()
        if re.match(r"^<p>\s*(?:<strong>)?\s*(?:Q[:：]|问题[:：])", inner, re.I):
            return (
                '<div class="callout callout-question" role="note">'
                '<div class="callout-title">思考与讨论 · QUESTION</div>'
                f'<div class="callout-body">{inner}</div></div>'
            )
        if re.match(r"^<p>\s*(?:<strong>)?\s*(?:重要提示|关键限制|注意|WARNING|IMPORTANT)[:：]?", inner, re.I):
            return (
                '<div class="callout callout-warning" role="note">'
                '<div class="callout-title">关键提示 · NOTICE</div>'
                f'<div class="callout-body">{inner}</div></div>'
            )
        if re.match(r"^<p>\s*(?:<strong>)?\s*(?:NOTE[:：])", inner, re.I):
            return (
                '<div class="callout callout-note" role="note">'
                '<div class="callout-title">补充说明 · NOTE</div>'
                f'<div class="callout-body">{inner}</div></div>'
            )
        return match.group(0)

    html_body = re.sub(
        r"<blockquote>\s*(.*?)\s*</blockquote>",
        repl_semantic_quote,
        html_body,
        flags=re.S,
    )
    return html_body


class Doc:
    def __init__(self, path: Path):
        self.path = path
        self.rel = path.relative_to(DOCS).as_posix()
        self.out = output_rel(self.rel)
        raw = path.read_text(encoding="utf-8")
        self.meta, raw_body = split_frontmatter(raw)
        self.body = clean_markdown_source(self.rel, raw_body)
        self.tokens = md.parse(self.body, {})
        self.headings = []
        seen: dict[str, int] = {}
        for i, token in enumerate(self.tokens):
            if token.type != "heading_open":
                continue
            level = int(token.tag[1])
            text = inline_text(self.tokens[i + 1])
            base = slugify(text)
            seen[base] = seen.get(base, 0) + 1
            slug = base if seen[base] == 1 else f"{base}-{seen[base] - 1}"
            token.attrSet("id", slug)
            self.headings.append({"level": level, "text": text, "slug": slug})
        h1 = next((h["text"] for h in self.headings if h["level"] == 1), "")
        meta_title = self.meta.get("title", "").strip()
        if meta_title and (not h1 or h1.strip().lower() in GENERIC_H1):
            self.title = meta_title
        else:
            self.title = h1 or meta_title or path.stem
        self.group = self.rel.split("/", 1)[0] if "/" in self.rel else ""
        if self.group not in GROUP_LABEL:
            self.group = ""
        self.order = NAV_ORDER.get(self.rel, 10_000)
        self.content = self._render()
        self.plain = re.sub(
            r"\s+", " ", html_lib.unescape(re.sub(r"<[^>]+>", " ", self.content))
        ).strip()
        self.outline = self._outline()

    def anchor_for(self, heading_text: str) -> str:
        for heading in self.headings:
            if heading["text"] == heading_text:
                return heading["slug"]
        return slugify(heading_text)

    def _render(self) -> str:
        body = md.renderer.render(self.tokens, md.options, {})

        def rewrite(match: re.Match) -> str:
            url = match.group("url")
            if not url or url.startswith("#") or SCHEME_RE.match(url):
                return match.group(0)
            path, _, fragment = url.partition("#")
            query = ""
            if "?" in path:
                path, _, query = path.partition("?")
            if re.match(
                r"^(?:www\.)?[a-z0-9-]+(?:\.[a-z0-9-]+)*\.(?:com|org|net|io|cn|dev|edu)(?:[/?#]|$)",
                path,
                re.I,
            ) and not (DOCS / path).exists():
                url = "https://" + url
            else:
                target = posixpath.normpath(
                    posixpath.join(posixpath.dirname(self.rel), path)
                )
                if target.endswith(".md") and not (DOCS / target).is_file():
                    matches = MD_BY_NAME.get(posixpath.basename(target), [])
                    if len(matches) == 1:
                        target = matches[0]
                if target.endswith(".md") and (DOCS / target).is_file():
                    url = site_href(self.out, output_rel(target))
                else:
                    url = os.path.relpath(
                        DOCS / target, (OUT / self.out).parent
                    ).replace(os.sep, "/")
            if query:
                url += "?" + query
            if fragment:
                url += "#" + fragment
            return f'{match.group("attr")}={match.group("q")}{url}{match.group("q")}'

        body = ATTR_RE.sub(rewrite, body)
        body = re.sub(r"<img(?![^>]*\bloading=)", '<img loading="lazy"', body)
        body = re.sub(
            r'<a(?![^>]*\btarget=)([^>]*href="https?://)',
            r'<a target="_blank" rel="noopener"\1',
            body,
        )
        # Replace old draw.io miniob-architecture.svg with our deterministic drawing-toolkit SVG
        arch_fig = arch_figure_html(
            self.out,
            "miniob-architecture",
            "MiniOB 数据库内核总体架构图",
            "客户端、网络服务与线程模型、SQL 解析/语义/优化/执行、会话与元数据控制面、多存储引擎/MVCC/缓冲池/CLog 与磁盘文件的分层协作架构。",
            fig_id="arch-miniob-overview",
        )
        body = re.sub(
            r"<p>\s*<img[^>]*miniob-architecture\.svg[^>]*>\s*</p>",
            lambda _: arch_fig,
            body,
        )
        body = enhance_callouts(body)
        body = body.replace("<table>", '<div class="table-wrap"><table>')
        body = body.replace("</table>", "</table></div>")
        return body

    def _outline(self) -> dict:
        root = {"text": self.title, "slug": None, "children": []}
        stack = [(0, root)]
        skipped_title = False
        for heading in self.headings:
            if heading["level"] > 3:
                continue
            if (
                not skipped_title
                and heading["level"] == 1
                and heading["text"] == self.title
            ):
                skipped_title = True
                stack = [(1, root)]
                continue
            while len(stack) > 1 and stack[-1][0] >= heading["level"]:
                stack.pop()
            node = {"text": heading["text"], "slug": heading["slug"], "children": []}
            stack[-1][1]["children"].append(node)
            stack.append((heading["level"], node))
        if not root["children"]:
            root["children"] = self._fallback_outline()
        return root

    def _fallback_outline(self) -> list[dict]:
        """Some short docs use bold lines or lists instead of headings."""
        def plain(text: str, limit: int) -> str:
            text = re.sub(r"!?\[([^\]]*)\]\([^)]*\)", r"\1", text)
            text = re.sub(r"[*_`]", "", text)
            text = re.sub(r"\s+", " ", text).strip()
            return text if len(text) <= limit else text[: limit - 1] + "…"

        nodes = []
        for line in self.body.splitlines():
            match = re.match(r"^\s*\*\*(.+?)\*\*\s*$", line)
            if match:
                raw = match.group(1).strip()
                text = plain(raw, 52)
                if text and not raw.startswith("*") and len(raw) <= 80:
                    nodes.append({"text": text, "slug": None, "children": []})
        if not nodes:
            for line in self.body.splitlines():
                match = re.match(r"^(?:[-*+]|\d+\.)\s+(.+)$", line)
                if match:
                    text = plain(match.group(1), 40)
                    if len(text) >= 2:
                        nodes.append({"text": text, "slug": None, "children": []})
                if len(nodes) >= 8:
                    break
        return nodes


def escape_brackets(text: str) -> str:
    return text.replace("[", "\\[").replace("]", "\\]")


def outline_markdown(root: dict, escape: bool = True) -> str:
    def label(text: str) -> str:
        return escape_brackets(text) if escape else text

    lines = [f"# {label(root['text'])}"]

    def walk(node: dict, depth: int) -> None:
        for child in node["children"]:
            text = label(child["text"])
            if child["slug"]:
                text = f"[{text}](#{child['slug']})"
            lines.append("  " * depth + f"- {text}")
            walk(child, depth + 1)

    walk(root, 1)
    return "\n".join(lines)


def outline_visual_cards(root: dict) -> str:
    """Offline-ready, compact visual structure map for article chapters."""
    def label(node: dict, idx_str: str = "") -> str:
        text = html_lib.escape(node["text"])
        prefix = f'<span class="tree-num">{idx_str}</span>' if idx_str else ""
        if node["slug"]:
            return f'<a class="tree-link" href="#{node["slug"]}">{prefix}<span>{text}</span></a>'
        return f'<span class="tree-link">{prefix}<span>{text}</span></span>'

    cards = []
    for i, child in enumerate(root["children"], 1):
        sub_items = ""
        if child["children"]:
            lis = "".join(
                f"<li>{label(grand, f'{i}.{j}')}</li>"
                for j, grand in enumerate(child["children"], 1)
            )
            sub_items = f'<ul class="tree-sub">{lis}</ul>'
        cards.append(
            f'<div class="tree-branch">'
            f'<div class="tree-top">{label(child, str(i))}</div>'
            f"{sub_items}</div>"
        )
    return f'<div class="tree-grid">{"".join(cards)}</div>'


def page_toc_html(
    doc: Doc,
    flows: list[dict],
    arch_list: list[tuple[str, str, str]],
) -> str:
    """Build sticky right-hand Table of Contents for comfortable long-doc reading."""
    items: list[str] = []
    for diag_name, title, _ in arch_list:
        items.append(
            f'<li class="toc-item toc-special"><a href="#arch-{diag_name}">◈ {html_lib.escape(title)}</a></li>'
        )
    if "miniob-architecture.svg" in doc.body:
        items.append(
            '<li class="toc-item toc-special"><a href="#arch-miniob-overview">◈ MiniOB 总体架构图</a></li>'
        )
    for f in flows:
        items.append(
            f'<li class="toc-item toc-flow"><a href="#flow-{f["id"]}">▶ 流程：{html_lib.escape(f["title"])}</a></li>'
        )
    for h in doc.headings:
        if h["level"] == 1 and h["text"] == doc.title:
            continue
        if h["level"] > 3:
            continue
        lvl_cls = f"toc-l{h['level']}"
        items.append(
            f'<li class="toc-item {lvl_cls}"><a href="#{h["slug"]}">{html_lib.escape(h["text"])}</a></li>'
        )
    if not items:
        return ""
    return (
        '<aside class="page-toc" aria-label="本页目录">'
        '<div class="toc-inner">'
        '<p class="toc-title">本页导览</p>'
        f'<ul class="toc-list">{"".join(items)}</ul>'
        '<a class="toc-top" href="#top">↑ 回到顶部</a>'
        "</div></aside>"
    )


def nav_html(docs: list[Doc], current: str | None, page_rel: str) -> str:
    current_group = next((d.group for d in docs if d.rel == current), None)
    flow_sources = {f["source"] for f in FLOWS}
    parts = ['<nav class="site-nav" aria-label="文档目录">']
    parts.append(
        f'<a class="nav-home" href="{site_href(page_rel, "index.html")}">'
        '<span class="brand-mark">M</span><span>MiniOB 可视化文档</span></a>'
    )
    for key, label, _ in GROUPS:
        group_docs = [d for d in docs if d.group == key and d.rel != "index.md"]
        if not group_docs:
            continue
        opened = (
            " open"
            if (key == current_group or (current is None and key == "design"))
            else ""
        )
        parts.append(
            f'<details class="nav-group"{opened}><summary>{label}<span>{len(group_docs)}</span></summary><div class="nav-links">'
        )
        for doc in group_docs:
            active = ' class="active" aria-current="page"' if doc.rel == current else ""
            has_visual = (
                doc.rel in flow_sources
                or doc.rel in DOC_ARCH_DIAGRAMS
                or "miniob-architecture.svg" in doc.body
            )
            badge = '<span class="nav-badge" title="含交互流程图或架构图">图</span>' if has_visual else ""
            parts.append(
                f'<a href="{site_href(page_rel, doc.out)}"{active}>'
                f'<span class="nav-link-text">{html_lib.escape(doc.title)}</span>{badge}</a>'
            )
        parts.append("</div></details>")
    flow_active = " active" if page_rel == "flows.html" else ""
    parts.append(
        f'<a class="nav-flows{flow_active}" href="{site_href(page_rel, "flows.html")}">'
        '<span class="dot"></span>动态流程<span class="count">%d</span></a>' % len(FLOWS)
    )
    parts.append("</nav>")
    return "".join(parts)


def global_markmap(docs: list[Doc]) -> str:
    lines = ["# MiniOB 文档地图"]
    for key, label, _ in GROUPS:
        group_docs = [d for d in docs if d.group == key and d.rel != "index.md"]
        if not group_docs:
            continue
        lines.append(f"- {label}")
        for doc in group_docs:
            lines.append(f"  - [{escape_brackets(doc.title)}]({doc.out})")
    lines.append("- 动态流程")
    for flow in FLOWS:
        lines.append(f"  - [{flow['title']}](flows.html#flow-{flow['id']})")
    return "\n".join(lines)


def page(
    page_rel: str,
    title: str,
    content: str,
    *,
    current: str | None = None,
    sidebar: bool = True,
    toc_html: str = "",
    body_class: str = "",
    scripts: str = "",
    math: bool = False,
) -> str:
    css = site_href(page_rel, "assets/style.css")
    home = site_href(page_rel, "index.html")
    flows_href = site_href(page_rel, "flows.html")
    docs = PAGE_DOCS
    side = (
        f'<aside class="sidebar" id="sidebar">{nav_html(docs, current, page_rel)}</aside>'
        if sidebar
        else ""
    )
    menu = (
        '<button class="menu-btn" id="menuBtn" aria-label="切换侧栏" aria-expanded="false">☰</button>'
        if sidebar
        else ""
    )
    math_js = (
        '<script>window.MathJax={tex:{inlineMath:[["$","$"],["\\\\(","\\\\)"]]}};</script>'
        '<script defer src="https://cdn.jsdelivr.net/npm/mathjax@3/es5/tex-mml-chtml.js"></script>'
        if math
        else ""
    )
    layout_cls = "layout"
    if not sidebar:
        layout_cls += " no-side"
    if toc_html:
        layout_cls += " has-toc"
    return f"""<!doctype html>
<html lang="zh-CN" id="top">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{html_lib.escape(title)} · MiniOB 可视化文档</title>
<link rel="stylesheet" href="{css}">
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/@highlightjs/cdn-assets@11/styles/atom-one-dark.min.css">
<script src="{site_href(page_rel, "assets/theme.js")}"></script>
{math_js}
</head>
<body class="{body_class}">
<div class="reading-progress" id="readingProgress" aria-hidden="true"></div>
<header class="topbar">
  {menu}
  <a class="brand" href="{home}"><span class="brand-mark">M</span><span>MiniOB <em>可视化文档</em></span></a>
  <div class="top-actions">
    <a class="top-link{' active' if page_rel == 'flows.html' else ''}" href="{flows_href}">动态流程</a>
    <button class="theme-btn" id="themeBtn" aria-label="切换明暗主题" title="切换明暗主题">◐</button>
  </div>
</header>
<div class="{layout_cls}">
  {side}
  <main class="main">
    {content}
    <footer class="page-footer">
      <span>由 <code>docs/visual/build.py</code> &amp; <code>draw_diagrams.py</code> 从 <code>docs/docs</code> 生成</span>
      <span>MiniOB · 可视化技术文档</span>
    </footer>
  </main>
  {toc_html}
</div>
<div class="nav-scrim" id="navScrim"></div>
<div class="diagram-modal" id="diagramModal" hidden role="dialog" aria-modal="true" aria-label="图表放大检视">
  <div class="diagram-modal-bar">
    <span class="diagram-modal-title" id="diagramModalTitle">架构与流程图检视</span>
    <button type="button" class="diagram-modal-close" id="diagramModalClose" aria-label="关闭">✕ 关闭 (Esc)</button>
  </div>
  <div class="diagram-modal-body" id="diagramModalBody"></div>
</div>
<script src="{site_href(page_rel, "assets/markmap-config.js")}"></script>
<script defer src="https://cdn.jsdelivr.net/npm/markmap-autoloader@0.18"></script>
<script defer src="https://cdn.jsdelivr.net/npm/@highlightjs/cdn-assets@11/highlight.min.js"></script>
{scripts}
<script defer src="{site_href(page_rel, "assets/site.js")}"></script>
</body>
</html>
"""


def flow_html(flow: dict, page_rel: str, anchor: str = "") -> str:
    fid = flow["id"]
    doc_out = output_rel(flow["source"])
    href = site_href(page_rel, doc_out) + (f"#{anchor}" if anchor else "")
    svg_href = site_href(page_rel, f"assets/diagrams/flow-{fid}.svg")
    svg_markup = draw_diagrams.inline_svg_for_flow(flow)
    node_label_map = {n["id"]: n["label"] for n in flow["nodes"]}

    chips = "".join(
        f'<button type="button" class="step-chip{" active" if i == 0 else ""}" data-step="{i}" '
        f'title="{html_lib.escape(step["title"])}" aria-label="第 {i + 1} 步：{html_lib.escape(step["title"])}">'
        f'<span class="step-chip-num">{i + 1}</span>'
        f'<span class="step-chip-label">{html_lib.escape(step["title"])}</span></button>'
        for i, step in enumerate(flow["steps"])
    )
    first = flow["steps"][0]
    first_tags = "".join(
        f'<span class="flow-tag">{html_lib.escape(node_label_map.get(nid, nid))}</span>'
        for nid in first.get("highlight", [])
    )
    notes = ""
    if flow.get("uncertainties"):
        items = "".join(
            f"<li>{html_lib.escape(note)}</li>" for note in flow["uncertainties"]
        )
        notes = (
            f'<details class="flow-notes">'
            f'<summary><span>边界与文档未决点（{len(flow["uncertainties"])} 项事实核对）</span></summary>'
            f"<ul>{items}</ul></details>"
        )
    total_steps = len(flow["steps"])
    init_pct = round(100 / max(1, total_steps), 1)
    return f"""
<section class="flow-player" id="flow-{fid}" data-flow="{fid}" tabindex="0" aria-label="{html_lib.escape(flow["title"])} 交互流程播放器">
  <header class="flow-head">
    <div>
      <div class="flow-kicker-row">
        <span class="flow-kicker">INTERACTIVE MECHANISM · 动态机制流程</span>
        <span class="flow-step-total">共 {total_steps} 步</span>
      </div>
      <h2>{html_lib.escape(flow["title"])}</h2>
      <p class="flow-summary">{html_lib.escape(flow.get("summary", ""))}</p>
    </div>
    <div class="flow-head-actions">
      <button type="button" class="arch-zoom-btn" data-zoom-diagram aria-label="放大流程图">⤢ 放大</button>
      <a class="arch-svg-link" href="{svg_href}" target="_blank" rel="noopener" title="打开独立 SVG 矢量图">SVG ↗</a>
      <a class="flow-source" href="{href}">跳转原文 →</a>
    </div>
  </header>
  <div class="flow-legend" aria-hidden="true">
    <span><i class="leg-dot leg-active"></i>当前步骤节点与流转路径</span>
    <span><i class="leg-dot leg-visited"></i>已执行路径</span>
    <span><i class="leg-dot leg-idle"></i>待执行节点</span>
    <span class="flow-legend-tip">💡 可直接点击图中节点、下方步骤条，或使用 ← / → 键切换步骤</span>
  </div>
  <div class="flow-canvas" role="img" aria-label="{html_lib.escape(flow["title"])} 流程图">{svg_markup}</div>
  <div class="flow-progress-track" aria-hidden="true"><div class="flow-progress-bar" style="width:{init_pct}%"></div></div>
  <div class="flow-controls-row">
    <div class="flow-bar">
      <button type="button" class="flow-btn" data-act="prev" aria-label="上一步" title="上一步 (←)">← 上一步</button>
      <button type="button" class="flow-btn primary" data-act="play" aria-label="自动播放" title="播放 / 暂停 (Space)">▶ 自动播放</button>
      <button type="button" class="flow-btn" data-act="next" aria-label="下一步" title="下一步 (→)">下一步 →</button>
    </div>
    <span class="flow-count">步骤 1 / {total_steps}</span>
  </div>
  <div class="flow-steps" role="tablist" aria-label="流程步骤列表">{chips}</div>
  <div class="flow-desc" aria-live="polite">
    <div class="flow-desc-head">
      <span class="flow-desc-badge">STEP 1 / {total_steps}</span>
      <strong class="flow-desc-title">{html_lib.escape(first["title"])}</strong>
      <div class="flow-desc-tags">{first_tags}</div>
    </div>
    <p class="flow-desc-text">{html_lib.escape(first["desc"])}</p>
  </div>
  {notes}
</section>"""


PAGE_DOCS: list[Doc] = []


def doc_page(doc: Doc) -> str:
    flows = [f for f in FLOWS if f["source"] == doc.rel]
    arch_list = DOC_ARCH_DIAGRAMS.get(doc.rel, [])
    arch_blocks = "".join(
        arch_figure_html(doc.out, diag_name, title, caption, fig_id=f"arch-{diag_name}")
        for diag_name, title, caption in arch_list
    )
    flow_block = "".join(
        flow_html(f, doc.out, doc.anchor_for(f.get("anchor", ""))) for f in flows
    )
    flow_section = f'<div class="flows-inline">{arch_blocks}{flow_block}</div>' if (flows or arch_blocks) else ""
    mindmap = outline_markdown(doc.outline)
    visual_tree = outline_visual_cards(doc.outline)
    group_docs = [d for d in PAGE_DOCS if d.group == doc.group and d.rel != "index.md"]
    pos = next((i for i, d in enumerate(group_docs) if d.rel == doc.rel), -1)
    prev_doc = group_docs[pos - 1] if pos > 0 else None
    next_doc = group_docs[pos + 1] if 0 <= pos < len(group_docs) - 1 else None
    pager = '<nav class="pager" aria-label="相邻文档导航">'
    pager += (
        f'<a class="prev" href="{site_href(doc.out, prev_doc.out)}"><span>← 上一篇</span>{html_lib.escape(prev_doc.title)}</a>'
        if prev_doc
        else "<span></span>"
    )
    pager += (
        f'<a class="next" href="{site_href(doc.out, next_doc.out)}"><span>下一篇 →</span>{html_lib.escape(next_doc.title)}</a>'
        if next_doc
        else "<span></span>"
    )
    pager += "</nav>"

    read_mins = max(1, len(doc.plain) // 500)
    sec_count = max(1, len(doc.headings))
    content = f"""
<nav class="crumbs" aria-label="面包屑导航"><a href="{site_href(doc.out, "index.html")}">首页</a><span>/</span><span>{GROUP_LABEL[doc.group]}</span><span>/</span><span aria-current="page">{html_lib.escape(doc.title)}</span></nav>
<header class="doc-head">
  <div class="doc-meta-bar">
    <span class="eyebrow-pill">{GROUP_LABEL[doc.group]}</span>
    <span class="doc-meta-item">{sec_count} 个章节</span>
    <span class="doc-meta-sep">·</span>
    <span class="doc-meta-item">约 {read_mins} 分钟阅读</span>
    <span class="doc-meta-sep">·</span>
    <a class="doc-meta-source" href="{source_link(doc.out, doc.rel)}">Markdown 源文件 ↗</a>
  </div>
  <h1>{html_lib.escape(doc.title)}</h1>
</header>
<section class="panel map-panel" id="doc-outline-panel">
  <div class="panel-head">
    <div>
      <h2>章节结构导览</h2>
      <span>点击任意章节直达正文；支持结构卡片与交互思维导图切换</span>
    </div>
    <button type="button" class="map-toggle-btn" data-toggle-markmap aria-expanded="false">⊞ 切换思维导图</button>
  </div>
  <div class="tree-view">{visual_tree}</div>
  <div class="markmap-wrap" hidden>
    <div class="markmap"><script type="text/template">{mindmap}</script></div>
  </div>
</section>
{flow_section}
<article class="content">{doc.content}</article>
{pager}
<p class="source-line">源文件：<a href="{source_link(doc.out, doc.rel)}">{html_lib.escape(doc.rel)}</a></p>
"""
    flow_json = [f for f in FLOWS if f["source"] == doc.rel]
    scripts = ""
    if flow_json:
        payload = json.dumps(flow_json, ensure_ascii=False).replace("</", "<\\/")
        scripts = f'<script type="application/json" id="flow-data">{payload}</script>'
    toc = page_toc_html(doc, flows, arch_list)
    return page(
        doc.out,
        doc.title,
        content,
        current=doc.rel,
        toc_html=toc,
        scripts=scripts,
        math=bool(re.search(r"(?m)^\$\$$", doc.body)),
    )


def home_page(intro: Doc) -> str:
    flow_sources = {f["source"] for f in FLOWS}
    cards = []
    for key, label, desc in GROUPS:
        group_docs = [d for d in PAGE_DOCS if d.group == key and d.rel != "index.md"]
        if not group_docs:
            continue
        doc_cards = []
        for d in group_docs:
            has_flow = d.rel in flow_sources
            has_arch = d.rel in DOC_ARCH_DIAGRAMS or "miniob-architecture.svg" in d.body
            badges = []
            if has_flow:
                badges.append('<span class="card-tag tag-flow">交互流程</span>')
            if has_arch:
                badges.append('<span class="card-tag tag-arch">架构图</span>')
            badge_html = f'<span class="doc-card-tags">{"".join(badges)}</span>' if badges else ""
            doc_cards.append(
                f'<a class="doc-card" href="{d.out}" data-search="{html_lib.escape((d.title + " " + " ".join(h["text"] for h in d.headings)).lower())}">'
                f'<div class="doc-card-top"><span class="doc-card-title">{html_lib.escape(d.title)}</span>{badge_html}</div>'
                f'<span class="doc-card-meta">{max(1, len(d.headings))} 节 · 约 {max(1, len(d.plain) // 500)} 分钟</span>'
                f'<span class="doc-card-excerpt">{html_lib.escape(d.plain[:96])}</span></a>'
            )
        cards.append(
            f'<section class="group-block" id="group-{key or "root"}">'
            f'<div class="group-head"><div><h2>{label}</h2><p>{desc}</p></div><span>{len(group_docs)} 篇</span></div>'
            f'<div class="doc-grid">{"".join(doc_cards)}</div></section>'
        )
    flow_cards = "".join(
        f'<a class="flow-card" href="flows.html#flow-{f["id"]}">'
        f'<span class="flow-card-kicker">{html_lib.escape(next((d.title for d in PAGE_DOCS if d.rel == f["source"]), ""))}</span>'
        f'<strong>{html_lib.escape(f["title"])}</strong>'
        f'<span>{html_lib.escape(f.get("summary", ""))}</span>'
        f'<span class="flow-card-play">▶ {len(f["steps"])} 步交互演练 · 查看 SVG</span></a>'
        for f in FLOWS
    )
    words = sum(len(d.plain) for d in PAGE_DOCS)
    content = f"""
<section class="hero">
  <p class="eyebrow">MINIOB · 数据库内核可视化技术文档</p>
  <h1>把数据库内核<br>拆成<em>看得见的架构与流程</em></h1>
  <p class="hero-sub">从 SQL 解析、Cascade 优化、B+Tree 蟹行并发到 MVCC/OCC 原子提交与 WAL 崩溃恢复：{len(PAGE_DOCS)} 篇结构化文档、{len(FLOWS)} 个高保真 SVG 交互流程、4 张核心子系统架构图。</p>
  <div class="search-box">
    <span class="search-icon">⌕</span>
    <input id="docSearch" type="search" placeholder="搜索文档标题或章节，例如：WAL、B+ 树、MVCC、OCC、恢复…" aria-label="搜索文档">
    <kbd>/</kbd>
  </div>
  <div class="stat-row">
    <div><strong>{len(PAGE_DOCS)}</strong><span>篇技术文档</span></div>
    <div><strong>{len(FLOWS)}</strong><span>个交互式机制流程</span></div>
    <div><strong>17</strong><span>张确定性 SVG 架构/流程图</span></div>
    <div><strong>{words / 10000:.0f}万</strong><span>字内核详解</span></div>
  </div>
</section>
<section class="section-head"><h2>交互式核心机制流程</h2><a href="flows.html">进入流程演练中心 ({len(FLOWS)}) →</a></section>
<div class="flow-grid">{flow_cards}</div>
<div id="emptyState" class="empty-state" hidden>没有匹配的文档。</div>
<section class="section-head"><h2>按主题浏览全部文档</h2><span>共 {sum(1 for d in PAGE_DOCS if d.rel != "index.md")} 篇</span></section>
{''.join(cards)}
<section class="panel map-panel home-map">
  <div class="panel-head">
    <div><h2>全站知识地图</h2><span>可缩放、拖拽与折叠；点击节点进入对应文档</span></div>
    <button type="button" class="map-toggle-btn" data-toggle-markmap aria-expanded="true">⊟ 收起/展开导图</button>
  </div>
  <div class="markmap-wrap">
    <div class="markmap"><script type="text/template">{global_markmap(PAGE_DOCS)}</script></div>
  </div>
</section>
<article class="content home-intro">{intro.content}</article>
"""
    return page(
        "index.html",
        "首页",
        content,
        sidebar=False,
        body_class="home",
        scripts="",
    )


def flows_page() -> str:
    categories = [
        ("SQL 编译、优化与执行", ["sql-lifecycle", "add-sql-statement", "cascade-search", "thread-model"]),
        ("事务、MVCC 与 OCC 并发控制", ["trx-lifecycle", "mvcc-update-occ"]),
        ("缓冲池、B+ 树与 PAX/LSM 存储引擎", ["buffer-pool-page-lifecycle", "bplus-tree-insert-split", "bplus-tree-concurrency-latching", "pax-storage-layout", "lsm-tree-write-compaction-recovery"]),
        ("持久化、Double Write 与崩溃恢复", ["checkpoint-recovery", "double-write-buffer-flow"]),
    ]
    flow_by_id = {f["id"]: f for f in FLOWS}

    quick_nav_groups = []
    toc_items = []
    sections_html = []

    for cat_title, fids in categories:
        pills = []
        blocks = []
        for fid in fids:
            f = flow_by_id.get(fid)
            if not f:
                continue
            pills.append(
                f'<a class="flow-jump-pill" href="#flow-{fid}">'
                f'<span>{html_lib.escape(f["title"])}</span>'
                f'<em>{len(f["steps"])}步</em></a>'
            )
            toc_items.append(
                f'<li class="toc-item toc-l2"><a href="#flow-{fid}">{html_lib.escape(f["title"])}</a></li>'
            )
            anchor = next(
                (d.anchor_for(f.get("anchor", "")) for d in PAGE_DOCS if d.rel == f["source"]),
                "",
            )
            blocks.append(flow_html(f, "flows.html", anchor))
        quick_nav_groups.append(
            f'<div class="flow-index-group"><strong>{html_lib.escape(cat_title)}</strong>'
            f'<div class="flow-index-pills">{"".join(pills)}</div></div>'
        )
        sections_html.append(
            f'<section class="flow-category-block">'
            f'<h2 class="flow-category-title">{html_lib.escape(cat_title)}</h2>'
            f'{"".join(blocks)}</section>'
        )

    content = f"""
<nav class="crumbs"><a href="index.html">首页</a><span>/</span><span aria-current="page">动态流程演练中心</span></nav>
<header class="doc-head page-head">
  <div class="doc-meta-bar">
    <span class="eyebrow-pill">INTERACTIVE MECHANISMS</span>
    <span class="doc-meta-item">{len(FLOWS)} 个核心机制</span>
    <span class="doc-meta-sep">·</span>
    <span class="doc-meta-item">确定性 SVG 矢量流程图</span>
  </div>
  <h1>逐步播放，看清数据库内核每一步</h1>
  <p class="lede">每张流程图均基于 <code>drawing-toolkit</code> 规范构建：按语义划分为架构分层泳道与执行流水线阶段，支持点击图中节点直接定位步骤、键盘左右键单步调试或自动播放，并严格标注文档边界与事实依据。</p>
</header>
<section class="panel flow-index-panel" aria-label="流程快速索引">
  <div class="flow-index-grid">{"".join(quick_nav_groups)}</div>
</section>
{"".join(sections_html)}
"""
    payload = json.dumps(FLOWS, ensure_ascii=False).replace("</", "<\\/")
    scripts = f'<script type="application/json" id="flow-data">{payload}</script>'
    toc = (
        '<aside class="page-toc" aria-label="流程目录">'
        '<div class="toc-inner">'
        '<p class="toc-title">13 个核心流程</p>'
        f'<ul class="toc-list">{"".join(toc_items)}</ul>'
        '<a class="toc-top" href="#top">↑ 回到顶部</a>'
        "</div></aside>"
    )
    return page("flows.html", "动态流程", content, toc_html=toc, scripts=scripts)


def main() -> None:
    global PAGE_DOCS
    # 1. Regenerate all standalone deterministic SVG files
    svg_files = draw_diagrams.build()

    # 2. Parse markdown docs and build all HTML pages
    paths = sorted(DOCS.rglob("*.md"))
    docs = [Doc(path) for path in paths]
    docs.sort(key=lambda d: (d.group, d.order, d.rel))
    PAGE_DOCS = docs
    for stale in OUT.rglob("*.html"):
        stale.unlink()
    for doc in docs:
        if doc.rel == "index.md":
            continue
        target = OUT / doc.out
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(doc_page(doc), encoding="utf-8")
    intro = next(d for d in docs if d.rel == "index.md")
    (OUT / "index.html").write_text(home_page(intro), encoding="utf-8")
    (OUT / "flows.html").write_text(flows_page(), encoding="utf-8")
    print(
        f"generated {len(docs)} docs, {len(FLOWS)} interactive flows, "
        f"{len(svg_files)} deterministic SVGs -> {OUT.relative_to(ROOT)}"
    )


if __name__ == "__main__":
    main()
