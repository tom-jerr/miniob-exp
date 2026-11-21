#!/usr/bin/env python3
"""Deterministic SVG diagram generator for MiniOB Visual Documentation.

Follows ~/.codex/skills/drawing-toolkit:
- Routes diagrams into Architecture Mode (containment, component ownership,
  control/data-plane separation) or Pipeline Mode (dominant time/stage axis,
  phase spans, execution lanes, branch/loop routing).
- Uses only Python's standard library (xml.etree.ElementTree) and native SVG
  primitives (no embedded raster <image>, no gradients, no external hrefs).
- Automatically verifies zero bounding-box overlaps across all nodes, lane
  headers, and edge label pills.
- Emits standalone validated SVG files to docs/visual/assets/diagrams/*.svg
  and provides inline interactive SVG markup for build.py.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from xml.etree import ElementTree as ET

OUT_DIR = Path(__file__).resolve().parent / "assets" / "diagrams"
FLOWS_PATH = Path(__file__).resolve().parent / "flows.json"

FONT = '"Noto Sans SC", "PingFang SC", "Microsoft YaHei", "Segoe UI", Arial, sans-serif'
MONO = '"JetBrains Mono", "SFMono-Regular", Consolas, "Liberation Mono", monospace'

C = {
    "white": "#FFFFFF",
    "paper": "#FCFDFE",
    "band_slate": "#F4F7FB",
    "band_blue": "#F0F5FF",
    "band_amber": "#FFF9EB",
    "band_teal": "#F0FDFA",
    "band_purple": "#F6F3FF",
    "band_rose": "#FFF1F2",
    "ink": "#141E30",
    "muted": "#526077",
    "faint": "#7B89A1",
    "line": "#334155",
    "border": "#CBD5E1",
    "border_strong": "#64748B",
    "blue_fill": "#E6EFFD",
    "blue_stroke": "#2554C7",
    "blue_badge": "#1E40AF",
    "amber_fill": "#FEF0C7",
    "amber_stroke": "#B45309",
    "amber_badge": "#92400E",
    "teal_fill": "#CCFBF1",
    "teal_stroke": "#0F766E",
    "teal_badge": "#115E59",
    "purple_fill": "#EDE9FE",
    "purple_stroke": "#6D28D9",
    "purple_badge": "#5B21B6",
    "rose_fill": "#FFE4E6",
    "rose_stroke": "#BE123C",
    "rose_badge": "#9F1239",
    "gray_fill": "#F1F5F9",
    "gray_stroke": "#475569",
    "gray_badge": "#334155",
}

ROLE_STYLE = {
    "blue": (C["blue_fill"], C["blue_stroke"], C["blue_badge"]),
    "amber": (C["amber_fill"], C["amber_stroke"], C["amber_badge"]),
    "teal": (C["teal_fill"], C["teal_stroke"], C["teal_badge"]),
    "purple": (C["purple_fill"], C["purple_stroke"], C["purple_badge"]),
    "rose": (C["rose_fill"], C["rose_stroke"], C["rose_badge"]),
    "gray": (C["gray_fill"], C["gray_stroke"], C["gray_badge"]),
}

Box = tuple[float, float, float, float]


def el(parent: ET.Element, tag: str, **attrs: object) -> ET.Element:
    clean: dict[str, str] = {}
    for k, v in attrs.items():
        if v is None:
            continue
        key = "class" if k == "cls" else k.replace("_", "-")
        clean[key] = str(v)
    return ET.SubElement(parent, tag, clean)


def boxes_overlap(a: Box, b: Box, pad: float = 2.0) -> bool:
    return not (
        a[2] + pad <= b[0]
        or b[2] + pad <= a[0]
        or a[3] + pad <= b[1]
        or b[3] + pad <= a[1]
    )


def est_text_width(s: str, font_size: float) -> float:
    w = 0.0
    for ch in s:
        if ord(ch) > 127:
            w += font_size * 0.98
        elif ch in "MW@%":
            w += font_size * 0.78
        else:
            w += font_size * 0.58
    return w


def new_svg(
    w: int, h: int, title: str, desc: str, svg_id: str
) -> tuple[ET.Element, dict[str, ET.Element], list[Box]]:
    svg = ET.Element(
        "svg",
        {
            "xmlns": "http://www.w3.org/2000/svg",
            "width": str(w),
            "height": str(h),
            "viewBox": f"0 0 {w} {h}",
            "role": "img",
            "aria-labelledby": f"{svg_id}-title {svg_id}-desc",
            "class": "toolkit-diagram",
            "data-diagram-id": svg_id,
        },
    )
    t = el(svg, "title", id=f"{svg_id}-title")
    t.text = title
    d = el(svg, "desc", id=f"{svg_id}-desc")
    d.text = desc

    defs = el(svg, "defs")
    for name, color in [
        ("ink", C["line"]),
        ("blue", C["blue_stroke"]),
        ("amber", C["amber_stroke"]),
        ("teal", C["teal_stroke"]),
        ("purple", C["purple_stroke"]),
        ("rose", C["rose_stroke"]),
        ("muted", C["border_strong"]),
    ]:
        marker = el(
            defs,
            "marker",
            id=f"{svg_id}-arrow-{name}",
            viewBox="0 0 10 10",
            refX="8.6",
            refY="5",
            markerWidth="6.8",
            markerHeight="6.8",
            orient="auto",
        )
        el(marker, "path", d="M0 0.8 L9.5 5 L0 9.2 Z", fill=color)

    layers = {
        "bg": el(svg, "g", cls="diag-bg"),
        "lanes": el(svg, "g", cls="diag-lanes"),
        "edges": el(svg, "g", cls="diag-edges"),
        "nodes": el(svg, "g", cls="diag-nodes"),
        "labels": el(svg, "g", cls="diag-labels"),
    }
    el(
        layers["bg"],
        "rect",
        x=0,
        y=0,
        width=w,
        height=h,
        rx=12,
        fill=C["paper"],
        stroke="none",
        cls="diag-canvas-bg",
    )
    occupied: list[Box] = []
    return svg, layers, occupied


def draw_lane(
    parent: ET.Element,
    occupied: list[Box],
    x: float,
    y: float,
    w: float,
    h: float,
    title: str,
    subtitle: str = "",
    *,
    fill: str = C["band_slate"],
    stroke: str = C["border"],
    badge_color: str = C["muted"],
    dash: str | None = None,
    rx: float = 10,
) -> None:
    g = el(parent, "g", cls="diag-lane")
    attrs: dict[str, object] = {
        "x": x,
        "y": y,
        "width": w,
        "height": h,
        "rx": rx,
        "fill": fill,
        "stroke": stroke,
        "stroke_width": 1.5,
        "cls": "diag-lane-rect",
    }
    if dash:
        attrs["stroke_dasharray"] = dash
    el(g, "rect", **attrs)
    if title:
        t = el(
            g,
            "text",
            x=x + 14,
            y=y + 22,
            font_family=FONT,
            font_size="12.8",
            font_weight="700",
            fill=badge_color,
            cls="diag-lane-title",
        )
        t.text = title
        tw = est_text_width(title, 12.8)
        occupied.append((x + 10, y + 6, x + 18 + tw, y + 27))
    if subtitle:
        st = el(
            g,
            "text",
            x=x + w - 14,
            y=y + 22,
            font_family=MONO,
            font_size="11.2",
            font_weight="500",
            text_anchor="end",
            fill=C["faint"],
            cls="diag-lane-sub",
        )
        st.text = subtitle
        sw = est_text_width(subtitle, 11.2)
        occupied.append((x + w - 18 - sw, y + 6, x + w - 10, y + 27))


def draw_node(
    parent: ET.Element,
    occupied: list[Box],
    node_id: str,
    x: float,
    y: float,
    w: float,
    h: float,
    label: str,
    subtitle: str = "",
    *,
    role: str = "blue",
    badge: str = "",
    steps: list[int] | None = None,
    rx: float = 9,
) -> None:
    fill, stroke, badge_col = ROLE_STYLE.get(role, ROLE_STYLE["blue"])
    step_str = ",".join(str(s) for s in (steps or []))
    g = el(
        parent,
        "g",
        cls=f"flow-node role-{role}",
        data_node_id=node_id,
        data_steps=step_str if step_str else None,
        tabindex="0" if steps is not None else None,
        role="button" if steps is not None else None,
        aria_label=f"{label} ({subtitle})" if subtitle else label,
    )
    el(
        g,
        "rect",
        x=x,
        y=y,
        width=w,
        height=h,
        rx=rx,
        fill=fill,
        stroke=stroke,
        stroke_width="1.8",
        cls="flow-node-rect",
    )
    occupied.append((x, y, x + w, y + h))

    if badge:
        bw = max(22.0, est_text_width(badge, 10.2) + 10.0)
        el(
            g,
            "rect",
            x=x + 8,
            y=y + 6,
            width=round(bw, 1),
            height=16,
            rx=4.5,
            fill=badge_col,
            stroke="none",
            cls="flow-node-badge-bg",
        )
        bt = el(
            g,
            "text",
            x=round(x + 8 + bw / 2, 1),
            y=y + 17.5,
            font_family=MONO,
            font_size="10.2",
            font_weight="700",
            text_anchor="middle",
            fill=C["white"],
            cls="flow-node-badge-text",
        )
        bt.text = badge

    cx = x + w / 2
    if subtitle:
        ty = y + h / 2 - 4 if not badge else y + h / 2 + 2
        t = el(
            g,
            "text",
            x=cx,
            y=ty,
            font_family=FONT,
            font_size="14.2",
            font_weight="700",
            text_anchor="middle",
            fill=C["ink"],
            cls="flow-node-title",
        )
        t.text = label
        st = el(
            g,
            "text",
            x=cx,
            y=ty + 16.5,
            font_family=MONO,
            font_size="11",
            font_weight="500",
            text_anchor="middle",
            fill=C["muted"],
            cls="flow-node-sub",
        )
        st.text = subtitle
    else:
        ty = y + h / 2 + 5
        t = el(
            g,
            "text",
            x=cx,
            y=ty,
            font_family=FONT,
            font_size="14.5",
            font_weight="700",
            text_anchor="middle",
            fill=C["ink"],
            cls="flow-node-title",
        )
        t.text = label


def find_free_pill_box(
    base_x: float,
    base_y: float,
    pw: float,
    ph: float,
    occupied: list[Box],
    canvas_w: float = 1060,
    canvas_h: float = 360,
) -> tuple[float, float]:
    """Deterministically find a collision-free center (cx, cy) near (base_x, base_y)."""
    candidates = [
        (0, 0),
        (0, -14),
        (0, 14),
        (0, -28),
        (0, 28),
        (0, -44),
        (0, 44),
        (0, -56),
        (0, 56),
        (-24, -44),
        (24, -44),
        (-24, 44),
        (24, 44),
        (-42, 0),
        (42, 0),
        (-54, -22),
        (54, -22),
        (-54, 22),
        (54, 22),
        (-68, -44),
        (68, -44),
        (-68, 44),
        (68, 44),
        (0, -68),
        (0, 68),
    ]
    for dx, dy in candidates:
        cx = base_x + dx
        cy = base_y + dy
        box = (cx - pw / 2, cy - ph / 2, cx + pw / 2, cy + ph / 2)
        if box[0] < 6 or box[1] < 6 or box[2] > canvas_w - 6 or box[3] > canvas_h - 6:
            continue
        if not any(boxes_overlap(box, occ, pad=2.0) for occ in occupied):
            return cx, cy
    return base_x, base_y


def draw_edge(
    parent: ET.Element,
    occupied: list[Box],
    svg_id: str,
    points: list[tuple[float, float]],
    label: str = "",
    *,
    from_id: str = "",
    to_id: str = "",
    color_name: str = "ink",
    dash: str | None = None,
    sw: float = 1.9,
    label_pos: tuple[float, float] | None = None,
    label_offset: tuple[float, float] = (0.0, 0.0),
    arrow: bool = True,
    canvas_size: tuple[float, float] = (1060, 360),
) -> None:
    stroke_map = {
        "ink": C["line"],
        "blue": C["blue_stroke"],
        "amber": C["amber_stroke"],
        "teal": C["teal_stroke"],
        "purple": C["purple_stroke"],
        "rose": C["rose_stroke"],
        "muted": C["border_strong"],
    }
    stroke = stroke_map.get(color_name, C["line"])
    g = el(
        parent,
        "g",
        cls="flow-edge",
        data_from=from_id or None,
        data_to=to_id or None,
    )
    d_parts = [f"M {points[0][0]:.1f} {points[0][1]:.1f}"]
    for px, py in points[1:]:
        d_parts.append(f"L {px:.1f} {py:.1f}")
    p_attrs: dict[str, object] = {
        "d": " ".join(d_parts),
        "fill": "none",
        "stroke": stroke,
        "stroke_width": sw,
        "stroke_linejoin": "round",
        "stroke_linecap": "round",
        "cls": "flow-edge-path",
    }
    if arrow:
        p_attrs["marker_end"] = f"url(#{svg_id}-arrow-{color_name})"
    if dash:
        p_attrs["stroke_dasharray"] = dash
    el(g, "path", **p_attrs)

    if label:
        if label_pos is not None:
            lx, ly = label_pos
        else:
            best_len = -1.0
            lx, ly = (points[0][0] + points[-1][0]) / 2, (points[0][1] + points[-1][1]) / 2
            for (x1, y1), (x2, y2) in zip(points, points[1:]):
                seg_len = abs(x2 - x1) + abs(y2 - y1)
                if seg_len > best_len:
                    best_len = seg_len
                    lx, ly = (x1 + x2) / 2, (y1 + y2) / 2
        lx += label_offset[0]
        ly += label_offset[1]
        fs = 11.2
        pw = max(34.0, est_text_width(label, fs) + 12.0)
        ph = 19.0
        lx, ly = find_free_pill_box(
            lx, ly, pw, ph, occupied, canvas_size[0], canvas_size[1]
        )
        pbox: Box = (lx - pw / 2, ly - ph / 2, lx + pw / 2, ly + ph / 2)
        occupied.append(pbox)
        el(
            g,
            "rect",
            x=round(pbox[0], 1),
            y=round(pbox[1], 1),
            width=round(pw, 1),
            height=ph,
            rx=5.5,
            fill=C["white"],
            stroke=C["border"],
            stroke_width="1.1",
            cls="flow-edge-pill",
        )
        t = el(
            g,
            "text",
            x=round(lx, 1),
            y=round(ly + 3.8, 1),
            font_family=FONT,
            font_size=str(fs),
            font_weight="600",
            text_anchor="middle",
            fill=C["muted"],
            cls="flow-edge-label",
        )
        t.text = label


# ============================================================================
# PART 1: 13 Interactive Flow Diagrams (Exact match with flows.json)
# ============================================================================

FLOW_SPECS: dict[str, dict] = {
    "sql-lifecycle": {
        "w": 1060,
        "h": 340,
        "lanes": [
            (16, 16, 175, 308, "客户端层", "obclient / CLI", C["band_slate"], C["border"], C["muted"]),
            (205, 16, 175, 308, "网络与会话层", "src/observer/net", C["band_blue"], C["blue_stroke"], C["blue_badge"]),
            (394, 16, 462, 308, "SQL 编译、优化与执行层 (SQL Engine)", "src/observer/sql", C["band_amber"], C["amber_stroke"], C["amber_badge"]),
            (870, 16, 174, 308, "存储引擎层", "src/observer/storage", C["band_teal"], C["teal_stroke"], C["teal_badge"]),
        ],
        "nodes": {
            "client": (32, 128, 142, 74, "stdin / stdout", "gray", "S1,S8"),
            "net": (221, 128, 142, 74, "NetServer / Session", "blue", "S2,S8"),
            "parser": (412, 74, 132, 68, "Flex & Bison", "amber", "S3"),
            "resolver": (562, 74, 132, 68, "Resolver → Stmt", "amber", "S4"),
            "optimizer": (712, 74, 128, 68, "RBO / Cascade", "amber", "S5"),
            "executor": (562, 200, 278, 72, "Volcano Operator / CommandExecutor", "purple", "S6"),
            "storage": (886, 128, 142, 74, "Table / Index / Trx", "teal", "S7"),
        },
        "edges": [
            ("client", "net", "发送SQL请求", [(174, 148), (221, 148)], "blue", (0, -34)),
            ("net", "parser", "交给处理模块", [(363, 148), (384, 148), (384, 108), (412, 108)], "blue", (-18, -42)),
            ("parser", "resolver", "语法树", [(544, 108), (562, 108)], "amber", (0, -48)),
            ("resolver", "optimizer", "内部数据结构", [(694, 108), (712, 108)], "amber", (0, -48)),
            ("optimizer", "executor", "执行计划", [(776, 142), (776, 200)], "amber", (42, 0)),
            ("executor", "storage", "读写数据", [(840, 236), (957, 236), (957, 202)], "teal", (10, 16)),
            ("executor", "net", "返回结果", [(562, 236), (292, 236), (292, 202)], "purple", (0, 16)),
            ("net", "client", "发送应答", [(221, 182), (174, 182)], "purple", (0, 34)),
        ],
    },
    "add-sql-statement": {
        "w": 1060,
        "h": 320,
        "lanes": [
            (16, 16, 476, 288, "1. 词法与语法前端 (Parser Stage)", "src/observer/sql/parser", C["band_blue"], C["blue_stroke"], C["blue_badge"]),
            (504, 16, 178, 288, "2. 语义绑定 (Resolve)", "sql/stmt", C["band_amber"], C["amber_stroke"], C["amber_badge"]),
            (694, 16, 350, 288, "3. 计划算子 / 命令执行与结果集", "sql/optimizer & executor", C["band_teal"], C["teal_stroke"], C["teal_badge"]),
        ],
        "nodes": {
            "lex": (30, 126, 136, 72, "lex_sql.l", "blue", "S1"),
            "yacc": (184, 126, 136, 72, "yacc_sql.y", "blue", "S2"),
            "node": (338, 126, 140, 72, "parse_defs.h", "blue", "S3"),
            "stmt": (522, 126, 142, 72, "stmt.h / CalcStmt", "amber", "S4"),
            "op": (714, 62, 152, 72, "Logical / PhysicalOp", "teal", "S5"),
            "cmd": (714, 192, 152, 72, "CommandExecutor", "purple", "S6"),
            "result": (894, 126, 136, 72, "TupleSchema", "teal", "S7"),
        },
        "edges": [
            ("lex", "yacc", "token", [(166, 162), (184, 162)], "blue", (0, -50)),
            ("yacc", "node", "ParsedSqlNode", [(320, 162), (338, 162)], "blue", (0, -50)),
            ("node", "stmt", "Resolver", [(478, 162), (522, 162)], "amber", (0, -50)),
            ("stmt", "op", "查询类型SQL", [(593, 126), (593, 98), (714, 98)], "teal", (-14, -16)),
            ("stmt", "cmd", "命令类型SQL", [(593, 198), (593, 228), (714, 228)], "purple", (-14, 16)),
            ("op", "result", "设置TupleSchema", [(866, 98), (962, 98), (962, 126)], "teal", (22, -16)),
            ("cmd", "result", "输出执行结果", [(866, 228), (962, 228), (962, 198)], "purple", (22, 16)),
        ],
    },
    "cascade-search": {
        "w": 1060,
        "h": 360,
        "lanes": [
            (16, 16, 200, 328, "调度核心 (DFS Scheduler)", "Task Stack", C["band_slate"], C["border"], C["muted"]),
            (230, 16, 560, 328, "Memo 搜索空间：Group 枚举与规则转换", "Logical M_EXPR & Rules", C["band_blue"], C["blue_stroke"], C["blue_badge"]),
            (804, 16, 240, 328, "物理计划与代价估算", "Physical M_EXPR & Cost", C["band_teal"], C["teal_stroke"], C["teal_badge"]),
        ],
        "nodes": {
            "stack": (34, 140, 158, 76, "DFS 任务栈循环", "gray", "S1"),
            "ogroup": (254, 140, 154, 76, "遍历 Group 分发", "blue", "S2,S7"),
            "oexpr": (438, 66, 150, 72, "匹配规则生成任务", "blue", "S3"),
            "egroup": (438, 224, 150, 72, "Group 逻辑表达式枚举", "purple", "S4"),
            "rule": (622, 66, 152, 72, "规则等价/物理转换", "amber", "S5"),
            "oinputs": (834, 140, 180, 76, "递归计算子计划代价", "teal", "S6,S7"),
        },
        "edges": [
            ("stack", "ogroup", "弹出task", [(192, 178), (254, 178)], "ink", (0, -52)),
            ("ogroup", "oexpr", "逻辑M_EXPR", [(331, 140), (331, 102), (438, 102)], "blue", (-12, -16)),
            ("ogroup", "oinputs", "物理M_EXPR", [(408, 178), (834, 178)], "teal", (110, -14)),
            ("oexpr", "rule", "有可应用规则", [(588, 88), (622, 88)], "amber", (0, -34)),
            ("rule", "oexpr", "产生逻辑表达式", [(622, 120), (588, 120)], "amber", (0, 32)),
            ("oexpr", "egroup", "生成task", [(482, 138), (482, 224)], "purple", (-40, 0)),
            ("egroup", "oexpr", "枚举logical M_EXPR", [(544, 224), (544, 138)], "purple", (68, 18)),
            ("rule", "oinputs", "产生物理表达式", [(774, 102), (924, 102), (924, 140)], "teal", (20, -16)),
            ("oinputs", "ogroup", "递归计算子代价", [(924, 216), (924, 322), (331, 322), (331, 216)], "teal", (0, -12)),
        ],
    },
    "trx-lifecycle": {
        "w": 1060,
        "h": 340,
        "lanes": [
            (16, 16, 510, 308, "1. 事务运行期 (Active Snapshot & Tuple Versioning)", "trx_id & [begin_xid, end_xid]", C["band_blue"], C["blue_stroke"], C["blue_badge"]),
            (540, 16, 504, 198, "2A. 提交路径 (Commit & Version Backfill)", "生成 commit_id 并回填记录版本", C["band_teal"], C["teal_stroke"], C["teal_badge"]),
            (540, 226, 504, 98, "2B. 回滚路径 (Rollback / Abort)", "撤销未提交修改", C["band_rose"], C["rose_stroke"], C["rose_badge"]),
        ],
        "nodes": {
            "begin": (36, 134, 140, 74, "start_if_need()", "blue", "S1"),
            "read": (212, 64, 148, 72, "visit_record()", "blue", "S2"),
            "write": (360, 178, 150, 74, "insert / delete (-trx_id)", "amber", "S3"),
            "commit": (560, 82, 138, 72, "Trx::commit()", "teal", "S4"),
            "commitid": (720, 82, 144, 72, "next_id()", "teal", "S4"),
            "backfill": (884, 82, 144, 72, "update begin/end", "teal", "S5"),
            "rollback": (690, 248, 200, 62, "Trx::rollback()", "rose", "S6"),
        },
        "edges": [
            ("begin", "read", "访问记录", [(106, 134), (106, 100), (212, 100)], "blue", (-10, -16)),
            ("begin", "write", "修改记录", [(106, 208), (106, 215), (360, 215)], "amber", (0, 16)),
            ("read", "write", "版本判断", [(360, 100), (435, 100), (435, 178)], "blue", (0, -16)),
            ("write", "commit", "提交", [(510, 200), (528, 200), (528, 118), (560, 118)], "teal", (-22, 0)),
            ("commit", "commitid", "生成commit_id", [(698, 118), (720, 118)], "teal", (0, -50)),
            ("commitid", "backfill", "改写begin/end", [(864, 118), (884, 118)], "teal", (0, 50)),
            ("write", "rollback", "回滚", [(435, 252), (435, 279), (690, 279)], "rose", (0, 15)),
        ],
    },
    "mvcc-update-occ": {
        "w": 1060,
        "h": 380,
        "lanes": [
            (16, 16, 514, 224, "1. 执行期：追加写多版本更新 (Append Tuple Version)", "READ_ONLY 扫描 + 旧行 end=-T + 新行 begin=-T", C["band_blue"], C["blue_stroke"], C["blue_badge"]),
            (16, 252, 514, 112, "2. 冲突中止路径 (First-Updater-Wins & OCC Validation)", "LOCKED_CONCURRENCY_CONFLICT → rollback()", C["band_rose"], C["rose_stroke"], C["rose_badge"]),
            (544, 16, 500, 348, "3. 提交锁串行区与惰性回填 / GC (Atomic Commit Protocol)", "commit_mutex: 序号分配 → WAL → TxnStatusTable → GC", C["band_teal"], C["teal_stroke"], C["teal_badge"]),
        ],
        "nodes": {
            "scan": (32, 64, 144, 68, "READ_ONLY 收集行", "blue", "S1"),
            "delmark": (200, 64, 148, 68, "旧行 end_xid = -T", "amber", "S2,S4"),
            "newver": (372, 64, 144, 68, "新行 begin_xid = -T", "amber", "S3"),
            "idx": (372, 160, 144, 66, "保留旧项 + 插入新项", "blue", "S3"),
            "conflict": (184, 284, 210, 64, "rollback() 并返回冲突", "rose", "S4"),
            "commit": (566, 64, 200, 68, "commit_seq = fetch_add+1", "teal", "S5"),
            "wal": (814, 64, 210, 68, "wait_lsn(COMMIT log)", "teal", "S6"),
            "publish": (814, 182, 210, 68, "TxnStatusTable.commit", "teal", "S7"),
            "backfill": (566, 182, 200, 68, "回填 commit_seq & MvccGc", "purple", "S8"),
        },
        "edges": [
            ("scan", "delmark", "逐行更新", [(176, 98), (200, 98)], "blue", (0, 48)),
            ("delmark", "newver", "end_xid=-T", [(348, 98), (372, 98)], "amber", (0, -48)),
            ("newver", "idx", "新RID", [(444, 132), (444, 160)], "blue", (42, 0)),
            ("idx", "commit", "提交", [(516, 193), (536, 193), (536, 98), (566, 98)], "teal", (-22, 0)),
            ("delmark", "conflict", "负xid冲突", [(248, 132), (248, 284)], "rose", (-42, 10)),
            ("commit", "conflict", "OCC读集冲突", [(666, 132), (666, 266), (330, 266), (330, 284)], "rose", (0, 0)),
            ("commit", "wal", "等待落盘", [(766, 98), (814, 98)], "teal", (0, -48)),
            ("wal", "publish", "提交点", [(919, 132), (919, 182)], "teal", (36, 0)),
            ("publish", "backfill", "可延后", [(814, 216), (766, 216)], "purple", (0, 48)),
        ],
    },
    "checkpoint-recovery": {
        "w": 1060,
        "h": 360,
        "lanes": [
            (16, 16, 1028, 196, "1. 检查点路径 (Db::sync() Checkpoint Barrier)", "冻结门禁 → GC 回填 → 刷页前采样 LSN → DWB & 数据文件 fsync → 原子替换 sys.db", C["band_blue"], C["blue_stroke"], C["blue_badge"]),
            (16, 226, 1028, 118, "2. WAL 保留策略与崩溃恢复路径 (Db::init() Crash Recovery)", "保留全部 clog → 恢复 dblwr → 从 checkpoint_lsn 重放 WAL → 回滚未提交事务 → 启动 GC", C["band_amber"], C["amber_stroke"], C["amber_badge"]),
        ],
        "nodes": {
            "gate": (32, 68, 144, 70, "unique_lock 门禁", "blue", "S1"),
            "gc": (198, 68, 144, 70, "MvccGc::collect()", "purple", "S2"),
            "lsn": (364, 68, 148, 70, "current_lsn() 采样", "amber", "S3"),
            "flush": (534, 68, 148, 70, "Table::sync() 刷页", "blue", "S4"),
            "fsync": (704, 68, 150, 70, "dblwr + data fsync", "teal", "S5"),
            "meta": (876, 68, 152, 70, "rename → sys.db", "teal", "S6"),
            "wal": (704, 260, 176, 66, "保留全部 clog_*.log", "gray", "S7"),
            "recover": (364, 260, 250, 66, "dblwr 恢复 + WAL 重放", "amber", "S8"),
        },
        "edges": [
            ("gate", "gc", "持unique lock", [(176, 103), (198, 103)], "blue", (0, 48)),
            ("gc", "lsn", "回填后采样", [(342, 103), (364, 103)], "purple", (0, -48)),
            ("lsn", "flush", "只覆盖target之前", [(512, 103), (534, 103)], "amber", (0, 48)),
            ("flush", "fsync", "持久化屏障", [(682, 103), (704, 103)], "teal", (0, -48)),
            ("fsync", "meta", "原子替换sys.db", [(854, 103), (876, 103)], "teal", (0, 48)),
            ("meta", "wal", "第6步未实现", [(952, 138), (952, 293), (880, 293)], "muted", (36, 10), "5 4"),
            ("meta", "recover", "重启", [(912, 138), (912, 192), (489, 192), (489, 260)], "amber", (120, 0)),
            ("recover", "gc", "启动GC修复", [(364, 293), (270, 293), (270, 138)], "purple", (-48, 18)),
        ],
    },
    "thread-model": {
        "w": 1060,
        "h": 340,
        "lanes": [
            (16, 16, 330, 308, "1. 连接接入与模型分发", "NetServer::serve & ThreadHandler", C["band_slate"], C["border"], C["muted"]),
            (360, 16, 512, 136, "2A. 一连接一线程模型 (-T=one-thread-per-connection)", "OneThreadPerConnectionThreadHandler", C["band_blue"], C["blue_stroke"], C["blue_badge"]),
            (360, 166, 512, 158, "2B. 线程池模型 (-T=java-thread-pool)", "JavaThreadPoolThreadHandler & Executor", C["band_teal"], C["teal_stroke"], C["teal_badge"]),
            (886, 16, 158, 308, "3. 生命周期结束", "连接释放", C["band_slate"], C["border"], C["muted"]),
        ],
        "nodes": {
            "listen": (32, 134, 138, 72, "NetServer::serve", "gray", "S2"),
            "handler": (194, 134, 138, 72, "ThreadHandler::create", "blue", "S1"),
            "otpc": (510, 58, 220, 72, "专属线程监听并执行 SQL", "blue", "S3"),
            "pool": (378, 214, 144, 70, "Selector 监听消息", "teal", "S4"),
            "queue": (546, 214, 144, 70, "连接任务入队等待", "amber", "S5"),
            "worker": (714, 214, 142, 70, "Worker 执行与应答", "teal", "S6"),
            "close": (900, 134, 130, 72, "负责线程关闭连接", "gray", "S7"),
        },
        "edges": [
            ("listen", "handler", "new_connection", [(170, 170), (194, 170)], "blue", (0, -50)),
            ("handler", "otpc", "每连接一线程", [(263, 134), (263, 94), (510, 94)], "blue", (30, -14)),
            ("handler", "pool", "线程池模型", [(263, 206), (263, 249), (378, 249)], "teal", (10, 16)),
            ("pool", "queue", "有消息入队", [(522, 249), (546, 249)], "teal", (0, 48)),
            ("queue", "worker", "取任务处理", [(690, 249), (714, 249)], "teal", (0, 48)),
            ("otpc", "close", "处理完关闭", [(730, 94), (965, 94), (965, 134)], "blue", (0, -14)),
            ("worker", "close", "关闭连接", [(856, 249), (965, 249), (965, 206)], "teal", (10, 16)),
        ],
    },
    "buffer-pool-page-lifecycle": {
        "w": 1060,
        "h": 340,
        "lanes": [
            (16, 16, 210, 308, "1. 磁盘文件层", "Page 0 Header & Data Pages", C["band_slate"], C["border"], C["muted"]),
            (240, 16, 588, 308, "2. 缓冲池管理层 (DiskBufferPool & BPFrameManager)", "页帧查找、空闲直接载入 vs 内存满时淘汰重绑", C["band_blue"], C["blue_stroke"], C["blue_badge"]),
            (842, 16, 202, 308, "3. 上层业务模块", "RecordManager / B+Tree", C["band_teal"], C["teal_stroke"], C["teal_badge"]),
        ],
        "nodes": {
            "read_page": (34, 132, 174, 76, "Page 0 Bitmap & page_num", "gray", "S1,S2"),
            "find_frame": (262, 132, 166, 76, "BPFrameManager", "blue", "S2,S3,S6"),
            "load_frame": (642, 132, 168, 76, "Frame 绑定并读入页", "teal", "S4,S7"),
            "evict_page": (450, 224, 160, 72, "按策略淘汰旧 Frame", "amber", "S6,S7"),
            "rebind_frame": (642, 232, 168, 64, "复用被淘汰 Frame", "amber", "S7"),
            "use_page": (860, 132, 166, 76, "读写 page.data", "teal", "S5"),
        },
        "edges": [
            ("read_page", "find_frame", "", [(208, 170), (262, 170)], "blue"),
            ("find_frame", "load_frame", "有空闲页帧", [(428, 152), (642, 152)], "teal", (0, -16)),
            ("find_frame", "evict_page", "无空闲页帧", [(345, 208), (345, 260), (450, 260)], "amber", (-15, 16)),
            ("evict_page", "rebind_frame", "", [(610, 264), (642, 264)], "amber"),
            ("rebind_frame", "load_frame", "", [(726, 232), (726, 208)], "amber"),
            ("load_frame", "use_page", "", [(810, 170), (860, 170)], "teal"),
        ],
    },
    "bplus-tree-insert-split": {
        "w": 1060,
        "h": 360,
        "lanes": [
            (16, 16, 368, 328, "1. 叶结点定位与直接插入", "Root → Leaf (page2)", C["band_blue"], C["blue_stroke"], C["blue_badge"]),
            (398, 16, 336, 328, "2. 叶结点分裂与双向链表修复", "新建右兄弟 page5 + 迁移一半 + 改指针", C["band_amber"], C["amber_stroke"], C["amber_badge"]),
            (748, 16, 296, 328, "3. 父结点回插与递归上溯", "insert_entry_into_parent", C["band_purple"], C["purple_stroke"], C["purple_badge"]),
        ],
        "nodes": {
            "locate_leaf": (32, 76, 154, 72, "Root 逐层下探", "blue", "S1"),
            "find_slot": (210, 76, 158, 72, "叶内二分找插入点", "blue", "S2"),
            "insert_direct": (210, 228, 158, 72, "后移数据并直接插入", "teal", "S3,S6"),
            "new_brother": (416, 76, 144, 72, "分配右兄弟 page5", "amber", "S4"),
            "move_half": (576, 76, 144, 72, "后半部分迁入新页", "amber", "S5"),
            "fix_links": (496, 228, 168, 72, "修复前后兄弟页号", "amber", "S6"),
            "parent_insert": (770, 152, 168, 72, "最小键+页号插父结点", "purple", "S7"),
            "new_root": (770, 52, 168, 66, "创建新根 Root Page", "teal", "S7"),
            "recurse_up": (770, 256, 168, 66, "父满则递归向上分裂", "purple", "S8"),
        },
        "edges": [
            ("locate_leaf", "find_slot", "", [(186, 112), (210, 112)], "blue"),
            ("find_slot", "insert_direct", "结点未满", [(289, 148), (289, 228)], "teal", (-38, 0)),
            ("find_slot", "new_brother", "结点已满", [(368, 112), (416, 112)], "amber", (0, 50)),
            ("new_brother", "move_half", "", [(560, 112), (576, 112)], "amber"),
            ("move_half", "fix_links", "", [(648, 148), (648, 188), (580, 188), (580, 228)], "amber"),
            ("fix_links", "parent_insert", "", [(664, 264), (716, 264), (716, 188), (770, 188)], "purple"),
            ("parent_insert", "new_root", "没有父结点", [(854, 152), (854, 118)], "teal", (-52, 0)),
            ("parent_insert", "recurse_up", "父结点也分裂", [(830, 224), (830, 256)], "purple", (-56, 0)),
            ("recurse_up", "parent_insert", "", [(938, 289), (998, 289), (998, 188), (938, 188)], "purple"),
        ],
    },
    "bplus-tree-concurrency-latching": {
        "w": 1060,
        "h": 360,
        "lanes": [
            (16, 16, 184, 328, "1. 入口初始化", "LatchMemo & Root", C["band_slate"], C["border"], C["muted"]),
            (214, 16, 488, 328, "2. 蟹行下探循环 (Crabbing Descent Loop)", "取子页 pin+1 → 加锁 → 安全则释放祖先", C["band_blue"], C["blue_stroke"], C["blue_badge"]),
            (716, 16, 328, 328, "3. 叶内更新与 Memo 统一释放", "叶插入/分裂回插 → release_all()", C["band_teal"], C["teal_stroke"], C["teal_badge"]),
        ],
        "nodes": {
            "lock_root": (32, 76, 152, 72, "LatchMemo 锁根页", "gray", "S1"),
            "crab_child": (234, 76, 146, 72, "fetch_page (pin+1)", "blue", "S2"),
            "lock_child": (402, 76, 144, 72, "frame 加读/写锁", "blue", "S2"),
            "is_safe": (566, 76, 122, 72, "不会分裂/合并?", "amber", "S3"),
            "release_parent": (542, 224, 146, 72, "release_parent()", "teal", "S4"),
            "descend": (294, 224, 174, 72, "while (!node.is_leaf)", "blue", "S5"),
            "leaf_insert": (736, 76, 140, 72, "insert_into_leaf", "teal", "S6"),
            "split_parent": (894, 76, 136, 72, "insert_into_parent", "amber", "S7"),
            "memo_release": (788, 224, 190, 72, "memo.release_all()", "purple", "S8"),
        },
        "edges": [
            ("lock_root", "crab_child", "", [(184, 112), (234, 112)], "blue"),
            ("crab_child", "lock_child", "", [(380, 112), (402, 112)], "blue"),
            ("lock_child", "is_safe", "", [(546, 112), (566, 112)], "amber"),
            ("is_safe", "release_parent", "安全", [(627, 148), (627, 224)], "teal", (28, 0)),
            ("is_safe", "descend", "不安全，保留锁", [(585, 148), (585, 186), (381, 186), (381, 224)], "amber", (0, -14)),
            ("release_parent", "descend", "", [(542, 260), (468, 260)], "teal"),
            ("descend", "crab_child", "循环向下", [(294, 260), (264, 260), (264, 148)], "blue", (-38, 0)),
            ("descend", "leaf_insert", "到达叶结点", [(381, 296), (381, 322), (710, 322), (710, 112), (736, 112)], "teal", (-100, -12)),
            ("leaf_insert", "split_parent", "发生分裂", [(876, 112), (894, 112)], "amber", (0, -50)),
            ("leaf_insert", "memo_release", "", [(806, 148), (806, 224)], "teal"),
            ("split_parent", "memo_release", "", [(962, 148), (962, 224)], "amber"),
        ],
    },
    "double-write-buffer-flow": {
        "w": 1060,
        "h": 360,
        "lanes": [
            (16, 16, 1028, 156, "1. 运行期脏页刷盘路径 (Normal Flush via Double Write Buffer)", "先写共享表空间 dblwr.db 并 fsync，再写回数据文件", C["band_blue"], C["blue_stroke"], C["blue_badge"]),
            (16, 186, 1028, 158, "2. 崩溃重启恢复路径 (Crash Recovery against Torn Pages)", "读取 dblwr.db → 校验 Page Checksum → 完好副本还原数据页", C["band_amber"], C["amber_stroke"], C["amber_badge"]),
        ],
        "nodes": {
            "flush_dirty": (34, 68, 154, 70, "BufferPool 刷脏页", "blue", "S1"),
            "add_dwb": (254, 68, 158, 70, "dblwr.add_page()", "blue", "S1,S2"),
            "write_space": (446, 68, 166, 70, "写入 dblwr.db 副本", "amber", "S2"),
            "sync_disk": (656, 68, 168, 70, "fsync(dblwr.db)", "teal", "S3"),
            "write_datafile": (860, 68, 164, 70, "写回 *.data / *.index", "teal", "S4"),
            "recover_read": (150, 240, 182, 70, "dblwr.load_pages()", "amber", "S5"),
            "check_page": (396, 240, 184, 70, "校验 Page Checksum", "amber", "S6,S7"),
            "copy_page": (656, 212, 168, 58, "有效页还原至磁盘", "teal", "S6"),
            "ignore_page": (656, 282, 168, 52, "丢弃损坏的 DWB 页", "rose", "S7"),
        },
        "edges": [
            ("flush_dirty", "add_dwb", "不直接写磁盘", [(188, 103), (254, 103)], "blue", (0, 48)),
            ("add_dwb", "write_space", "", [(412, 103), (446, 103)], "amber"),
            ("write_space", "sync_disk", "装满", [(612, 103), (656, 103)], "teal", (0, 48)),
            ("sync_disk", "write_datafile", "", [(824, 103), (860, 103)], "teal"),
            ("recover_read", "check_page", "", [(332, 275), (396, 275)], "amber"),
            ("check_page", "copy_page", "未损坏", [(580, 255), (656, 241)], "teal", (-10, -18)),
            ("check_page", "ignore_page", "已损坏", [(580, 295), (656, 308)], "rose", (-10, 18)),
            ("copy_page", "write_datafile", "恢复写回", [(824, 241), (942, 241), (942, 138)], "teal", (42, -20)),
        ],
    },
    "lsm-tree-write-compaction-recovery": {
        "w": 1060,
        "h": 360,
        "lanes": [
            (16, 16, 1028, 174, "1. 写入、冻结转储与 Compaction 路径 (Write → Freeze → SSTable → Compaction → Manifest)", "顺序写 WAL → SkipList MemTable → 8KB 冻结转储 → Compaction", C["band_blue"], C["blue_stroke"], C["blue_badge"]),
            (16, 204, 1028, 140, "2. 系统启动与恢复路径 (CURRENT → Manifest Snapshot/Compaction → WAL Replay)", "读取 CURRENT → 恢复 Snapshot 与 Compaction Record → 回放 WAL", C["band_teal"], C["teal_stroke"], C["teal_badge"]),
        ],
        "nodes": {
            "wal_first": (30, 66, 138, 70, "顺序追加 0.wal", "amber", "S1"),
            "write_memtable": (188, 66, 146, 70, "SkipList 插入记录", "blue", "S2"),
            "memtable_full": (354, 66, 152, 70, "try_freeze_memtable", "blue", "S3"),
            "dump_sstable": (526, 66, 152, 70, "构建 Block / *.sst", "teal", "S4"),
            "compaction": (700, 56, 152, 66, "多 SSTable 归并", "purple", "S5"),
            "manifest_write": (874, 66, 154, 70, "Compaction / NewMem", "teal", "S6"),
            "read_manifest": (680, 248, 176, 70, "打开当前 *.mf 文件", "teal", "S7"),
            "replay_order": (416, 248, 188, 70, "Snapshot + Compaction", "blue", "S8"),
            "replay_wal": (152, 248, 188, 70, "按 NewMemtable 重放", "amber", "S8"),
        },
        "edges": [
            ("wal_first", "write_memtable", "", [(168, 101), (188, 101)], "blue"),
            ("write_memtable", "memtable_full", "", [(334, 101), (354, 101)], "blue"),
            ("memtable_full", "dump_sstable", "", [(506, 101), (526, 101)], "teal"),
            ("dump_sstable", "compaction", "", [(678, 89), (700, 89)], "purple"),
            ("compaction", "manifest_write", "", [(852, 89), (874, 89)], "teal"),
            ("dump_sstable", "manifest_write", "minor compaction", [(602, 136), (602, 168), (951, 168), (951, 136)], "teal", (0, -12)),
            ("manifest_write", "read_manifest", "重启", [(951, 136), (951, 283), (856, 283)], "amber", (32, 12)),
            ("read_manifest", "replay_order", "", [(680, 283), (604, 283)], "teal"),
            ("replay_order", "replay_wal", "", [(416, 283), (340, 283)], "amber"),
        ],
    },
    "pax-storage-layout": {
        "w": 1060,
        "h": 350,
        "lanes": [
            (16, 16, 444, 318, "1. 表格式选择与页内管理器 (Table & Handler)", "storage format=pax & PaxRecordPageHandler", C["band_blue"], C["blue_stroke"], C["blue_badge"]),
            (476, 16, 568, 318, "2. PAX 8KB 页内垂直分区布局 (Partition Attributes Across)", "[PageHeader | Bitmap | Column Index | Col 1 .. Col N]", C["band_amber"], C["amber_stroke"], C["amber_badge"]),
        ],
        "nodes": {
            "create_pax": (34, 68, 166, 72, "storage format=pax", "blue", "S1"),
            "pax_handler": (244, 68, 196, 72, "PaxRecordPageHandler", "blue", "S2"),
            "insert_rec": (74, 224, 158, 72, "按列拆解写入槽位", "teal", "S7"),
            "get_rec": (266, 224, 164, 72, "get_record / get_chunk", "purple", "S7"),
            "page_layout": (500, 68, 182, 72, "8KB 页内属性垂直切分", "amber", "S3"),
            "page_header": (736, 56, 280, 68, "record_num / capacity + slot bitmap", "gray", "S4"),
            "col_index": (736, 144, 280, 68, "col_0 .. col_n (列起始偏移数组)", "amber", "S5"),
            "col_data": (736, 236, 280, 74, "[Col 1 连续区] [Col 2 连续区] ... [Col N]", "teal", "S6"),
        },
        "edges": [
            ("create_pax", "pax_handler", "", [(200, 104), (244, 104)], "blue"),
            ("pax_handler", "page_layout", "", [(440, 104), (500, 104)], "amber"),
            ("page_layout", "page_header", "", [(682, 90), (736, 90)], "amber"),
            ("page_layout", "col_index", "", [(591, 140), (591, 178), (736, 178)], "amber"),
            ("col_index", "col_data", "", [(876, 212), (876, 236)], "teal"),
            ("pax_handler", "insert_rec", "", [(310, 140), (310, 182), (153, 182), (153, 224)], "teal"),
            ("pax_handler", "get_rec", "", [(348, 140), (348, 224)], "purple"),
            ("insert_rec", "col_data", "写入列", [(153, 296), (153, 320), (696, 320), (696, 284), (736, 284)], "teal", (-40, -10)),
            ("get_rec", "col_data", "按列读取", [(430, 260), (736, 260)], "purple", (0, -14)),
        ],
    },
}


def build_flow_svg(flow: dict) -> ET.Element:
    fid = flow["id"]
    spec = FLOW_SPECS[fid]
    svg, layers, occupied = new_svg(
        spec["w"],
        spec["h"],
        f"{flow['title']} — 交互式机制流程图",
        flow.get("summary", flow["title"]),
        f"flow-{fid}",
    )

    node_steps: dict[str, list[int]] = {}
    for idx, step in enumerate(flow.get("steps", [])):
        for nid in step.get("highlight", []):
            node_steps.setdefault(nid, []).append(idx)

    node_labels = {n["id"]: n["label"] for n in flow["nodes"]}

    for x, y, w, h, title, sub, fill, stroke, badge_col in spec["lanes"]:
        draw_lane(
            layers["lanes"],
            occupied,
            x,
            y,
            w,
            h,
            title,
            sub,
            fill=fill,
            stroke=stroke,
            badge_color=badge_col,
        )

    for nid, (x, y, w, h, sub, role, badge) in spec["nodes"].items():
        draw_node(
            layers["nodes"],
            occupied,
            nid,
            x,
            y,
            w,
            h,
            node_labels[nid],
            sub,
            role=role,
            badge=badge,
            steps=node_steps.get(nid, []),
        )

    for edge_spec in spec["edges"]:
        from_id, to_id, label, pts, col = edge_spec[:5]
        offset = edge_spec[5] if len(edge_spec) > 5 else (0.0, 0.0)
        dash = edge_spec[6] if len(edge_spec) > 6 else None
        draw_edge(
            layers["edges"],
            occupied,
            f"flow-{fid}",
            pts,
            label,
            from_id=from_id,
            to_id=to_id,
            color_name=col,
            label_offset=offset,
            dash=dash,
            canvas_size=(spec["w"], spec["h"]),
        )

    return svg


# ============================================================================
# PART 2: 4 Core Architecture Diagrams (Architecture Grammar)
# ============================================================================

def build_miniob_architecture_svg() -> ET.Element:
    """MiniOB full system architecture diagram (Architecture Mode)."""
    w, h = 1120, 680
    svg_id = "miniob-architecture"
    svg, layers, occupied = new_svg(
        w,
        h,
        "MiniOB 数据库内核总体架构图",
        "分层展示客户端、网络与线程模型、SQL解析/语义/优化/执行、会话与元数据控制面、多存储引擎/事务/缓冲池/日志与底层持久化文件。",
        svg_id,
    )

    t = el(layers["labels"], "text", x=24, y=32, font_family=FONT, font_size="18.5", font_weight="800", fill=C["ink"])
    t.text = "MiniOB 数据库系统总体架构 (System Architecture: Data Plane & Control/Catalog Plane)"
    st = el(layers["labels"], "text", x=1096, y=32, font_family=MONO, font_size="11.8", font_weight="600", text_anchor="end", fill=C["muted"])
    st.text = "Client ↔ NET Service ↔ SQL Engine ↔ Storage & Transaction ↔ Disk Files"
    occupied.append((20, 12, 1100, 42))

    # 1. Left column: Client & Network Layer
    draw_lane(layers["lanes"], occupied, 20, 52, 200, 510, "客户端与网络层", "Client & NET", fill=C["band_slate"], stroke=C["border_strong"], badge_color=C["ink"])
    draw_node(layers["nodes"], occupied, "arch-client", 38, 92, 164, 76, "客户端 (Client)", "obclient / mysql / stdin", role="gray", badge="CLI/TCP")
    draw_node(layers["nodes"], occupied, "arch-protocol", 38, 214, 164, 72, "通信协议", "Plain ('\\0') / MySQL / CLI", role="blue", badge="Protocol")
    draw_node(layers["nodes"], occupied, "arch-net", 38, 326, 164, 82, "网络模块 (NET Service)", "NetServer / Communicator", role="blue", badge="libevent")
    draw_node(layers["nodes"], occupied, "arch-thread", 38, 448, 164, 90, "线程模型 (ThreadHandler)", "One-Thread / JavaThreadPool", role="blue", badge="-T option")

    # 2. Upper-middle: SQL Processing Engine
    draw_lane(layers["lanes"], occupied, 238, 52, 624, 244, "SQL 编译、优化与执行层 (SQL Processing Engine)", "src/observer/sql", fill=C["band_blue"], stroke=C["blue_stroke"], badge_color=C["blue_badge"])
    draw_node(layers["nodes"], occupied, "arch-parser", 258, 94, 138, 70, "SQL解析 (Parser)", "Flex & Bison → AST", role="blue", badge="1. Parse")
    draw_node(layers["nodes"], occupied, "arch-resolver", 434, 94, 152, 70, "语义解析 (Resolver)", "AST → Stmt 绑定对象", role="blue", badge="2. Resolve")
    draw_node(layers["nodes"], occupied, "arch-optimizer", 624, 94, 218, 70, "查询优化 (Optimizer)", "RBO 改写 + Cascade CBO", role="amber", badge="3. Optimize")
    draw_node(layers["nodes"], occupied, "arch-executor", 258, 204, 328, 72, "计划执行 (Executor)", "火山模型 PhysicalOp / 向量化 / Command", role="teal", badge="4. Execute")
    draw_node(layers["nodes"], occupied, "arch-result", 624, 204, 218, 72, "执行结果 (SqlResult)", "TupleSchema / 行集编码返回", role="purple", badge="5. Result")

    # 3. Right column: Control & Catalog Plane
    draw_lane(layers["lanes"], occupied, 880, 52, 220, 510, "控制与元数据面", "Control & Catalog", fill=C["band_amber"], stroke=C["amber_stroke"], badge_color=C["amber_badge"], dash="6 4")
    draw_node(layers["nodes"], occupied, "arch-session", 898, 94, 184, 78, "会话管理 (Session)", "连接状态 / 事务上下文 / 参数", role="amber", badge="Session")
    draw_node(layers["nodes"], occupied, "arch-meta", 898, 200, 184, 110, "元数据管理 (Meta Data)", "DbMeta / TableMeta / Field / Index", role="amber", badge="Catalog")
    draw_node(layers["nodes"], occupied, "arch-trx", 898, 356, 184, 86, "事务管理 (MVCC / OCC)", "Vacuous / MvccTrx / OccTrx", role="purple", badge="TrxKit")
    draw_node(layers["nodes"], occupied, "arch-trx-sub", 898, 464, 184, 76, "TxnStatusTable & GC", "原子提交点 / 水位垃圾回收", role="purple", badge="SI & GC")

    # 4. Lower-middle: Storage Engine Layer
    draw_lane(layers["lanes"], occupied, 238, 312, 624, 250, "存储引擎层 (Storage Engine & Buffer / WAL Subsystem)", "src/observer/storage", fill=C["band_teal"], stroke=C["teal_stroke"], badge_color=C["teal_badge"])
    draw_node(layers["nodes"], occupied, "arch-record", 258, 352, 186, 72, "记录管理 (Record Manager)", "Heap 行存页 & PAX 列存页", role="teal", badge="Heap/PAX")
    draw_node(layers["nodes"], occupied, "arch-btree", 460, 352, 186, 72, "B+ Tree (索引与聚簇表)", "二级索引 / BtreeEngine / 蟹行锁", role="teal", badge="B+Tree")
    draw_node(layers["nodes"], occupied, "arch-lsm", 662, 352, 180, 72, "LSM-Tree (ObLsm)", "MemTable / SSTable / Compaction", role="teal", badge="ENGINE=lsm")
    draw_node(layers["nodes"], occupied, "arch-bp", 258, 458, 232, 82, "缓冲池 (Buffer Pool)", "BPFrameManager / DiskBufferPool", role="blue", badge="8KB Pages")
    draw_node(layers["nodes"], occupied, "arch-dwb", 506, 458, 164, 82, "Double Write Buffer", "共享表空间防半页写 (Torn Page)", role="amber", badge="dblwr.db")
    draw_node(layers["nodes"], occupied, "arch-clog", 686, 458, 156, 82, "日志管理 (Redo Log)", "CLog / WAL / MvccTrxLog", role="purple", badge="clog")

    # 5. Bottom band: Persistent Disk Files
    draw_lane(layers["lanes"], occupied, 20, 578, 1080, 84, "磁盘持久化文件 (Persistent Disk Layout: miniob/db/sys/)", "Page LSN + Checksum + Atomic Rename", fill=C["band_slate"], stroke=C["border_strong"], badge_color=C["ink"])
    disk_files = [
        ("arch-f-data", 38, "*.data (表数据页)", "Heap / PAX 8KB 页", "teal"),
        ("arch-f-idx", 248, "*.index (B+Tree 页)", "索引页 / 聚簇表页", "teal"),
        ("arch-f-dwb", 458, "dblwr.db (双写文件)", "16页共享恢复副本", "amber"),
        ("arch-f-clog", 668, "clog/clog_*.log", "物理+逻辑混合 WAL", "purple"),
        ("arch-f-meta", 878, "sys.db / *.table / lsm/", "控制文件、Schema、SST", "gray"),
    ]
    for fid, fx, flabel, fsub, frole in disk_files:
        draw_node(layers["nodes"], occupied, fid, fx, 604, 194, 46, flabel, fsub, role=frole, rx=7)

    # Edges with collision-checked pill placement
    cs = (w, h)
    draw_edge(layers["edges"], occupied, svg_id, [(120, 168), (120, 214)], "请求/应答", color_name="blue", canvas_size=cs)
    draw_edge(layers["edges"], occupied, svg_id, [(120, 286), (120, 326)], "", color_name="blue", canvas_size=cs)
    draw_edge(layers["edges"], occupied, svg_id, [(120, 408), (120, 448)], "new_connection", color_name="blue", canvas_size=cs)
    draw_edge(layers["edges"], occupied, svg_id, [(202, 367), (222, 367), (222, 129), (258, 129)], "SQL文本", color_name="blue", label_pos=(222, 184), canvas_size=cs)
    draw_edge(layers["edges"], occupied, svg_id, [(396, 129), (434, 129)], "AST", color_name="blue", label_pos=(415, 80), canvas_size=cs)
    draw_edge(layers["edges"], occupied, svg_id, [(586, 129), (624, 129)], "Stmt", color_name="blue", label_pos=(605, 80), canvas_size=cs)
    draw_edge(layers["edges"], occupied, svg_id, [(733, 164), (733, 184), (422, 184), (422, 204)], "Physical Plan", color_name="amber", label_pos=(550, 184), canvas_size=cs)
    draw_edge(layers["edges"], occupied, svg_id, [(586, 240), (624, 240)], "", color_name="purple", canvas_size=cs)
    draw_edge(layers["edges"], occupied, svg_id, [(733, 276), (733, 294), (228, 294), (228, 382), (202, 382)], "写回客户端", color_name="purple", label_pos=(660, 294), canvas_size=cs)

    draw_edge(layers["edges"], occupied, svg_id, [(351, 276), (351, 352)], "表记录读写", color_name="teal", label_pos=(351, 300), canvas_size=cs)
    draw_edge(layers["edges"], occupied, svg_id, [(553, 276), (553, 352)], "索引访问", color_name="teal", label_pos=(553, 300), canvas_size=cs)

    draw_edge(layers["edges"], occupied, svg_id, [(351, 424), (351, 458)], "fetch / flush", color_name="blue", label_pos=(351, 441), canvas_size=cs)
    draw_edge(layers["edges"], occupied, svg_id, [(553, 424), (430, 424), (430, 458)], "", color_name="blue", canvas_size=cs)
    draw_edge(layers["edges"], occupied, svg_id, [(490, 498), (506, 498)], "", color_name="amber", canvas_size=cs)
    draw_edge(layers["edges"], occupied, svg_id, [(990, 442), (990, 464)], "", color_name="purple", canvas_size=cs)
    draw_edge(layers["edges"], occupied, svg_id, [(898, 436), (764, 436), (764, 458)], "COMMIT / redo", color_name="purple", label_pos=(800, 441), canvas_size=cs)

    draw_edge(layers["edges"], occupied, svg_id, [(898, 240), (870, 240), (870, 80), (510, 80), (510, 94)], "Schema校验", color_name="amber", dash="4 3", label_pos=(730, 80), canvas_size=cs)
    draw_edge(layers["edges"], occupied, svg_id, [(898, 398), (854, 398), (854, 240), (842, 240)], "可见性 / 版本", color_name="purple", dash="4 3", label_pos=(870, 330), canvas_size=cs)

    draw_edge(layers["edges"], occupied, svg_id, [(320, 540), (135, 604)], "", color_name="teal", canvas_size=cs)
    draw_edge(layers["edges"], occupied, svg_id, [(410, 540), (345, 604)], "", color_name="teal", canvas_size=cs)
    draw_edge(layers["edges"], occupied, svg_id, [(588, 540), (555, 604)], "先fsync副本", color_name="amber", label_pos=(572, 566), canvas_size=cs)
    draw_edge(layers["edges"], occupied, svg_id, [(764, 540), (764, 604)], "顺序追加", color_name="purple", label_pos=(764, 566), canvas_size=cs)
    draw_edge(layers["edges"], occupied, svg_id, [(975, 540), (975, 604)], "checkpoint", color_name="muted", label_pos=(975, 566), canvas_size=cs)

    return svg


def build_storage_persistence_architecture_svg() -> ET.Element:
    """Storage, Buffer Pool, DWB, and WAL Persistence Architecture (Architecture Mode)."""
    w, h = 1060, 470
    svg_id = "miniob-persistence-architecture"
    svg, layers, occupied = new_svg(
        w,
        h,
        "MiniOB 存储文件、缓冲池与 WAL/DWB 恢复架构图",
        "展示内存 Buffer Pool、日志缓冲区、Double Write Buffer 与磁盘 sys.db、*.table、*.data、*.index、clog 文件的写入与恢复关系。",
        svg_id,
    )
    draw_lane(layers["lanes"], occupied, 16, 16, 504, 250, "内存运行态 (In-Memory Buffer & Transaction State)", "Buffer Pool + Log Buffer + TxnStatusTable", fill=C["band_blue"], stroke=C["blue_stroke"], badge_color=C["blue_badge"])
    draw_lane(layers["lanes"], occupied, 536, 16, 508, 250, "持久化屏障与双写缓冲 (Durability & Torn-Page Protection)", "WAL Write-Ahead + Double Write Buffer + Db::sync()", fill=C["band_amber"], stroke=C["amber_stroke"], badge_color=C["amber_badge"])
    draw_lane(layers["lanes"], occupied, 16, 286, 1028, 168, "数据库磁盘目录结构 (miniob/db/sys/ Physical Files & Page Layout)", "8KB Page {lsn, check_sum, data} · Atomic Control File sys.db", fill=C["band_teal"], stroke=C["teal_stroke"], badge_color=C["teal_badge"])

    draw_node(layers["nodes"], occupied, "pa-rec", 34, 58, 220, 72, "Record & B+Tree Pages", "Dirty Frame: page.lsn + slot bitmap", role="blue", badge="BPFrameManager")
    draw_node(layers["nodes"], occupied, "pa-trx", 274, 58, 228, 72, "MvccTrx & TxnStatusTable", "begin_xid / end_xid (-T → commit_seq)", role="purple", badge="8B XID")
    draw_node(layers["nodes"], occupied, "pa-logbuf", 34, 172, 220, 72, "LogEntryBuffer (WAL 缓冲)", "RECORD / BPLUS_TREE / TRX", role="purple", badge="append()")
    draw_node(layers["nodes"], occupied, "pa-gc", 274, 172, 228, 72, "MvccGc (水位垃圾回收)", "回填已提交 -T & 清理死版本+旧索引", role="teal", badge="Watermark")

    draw_node(layers["nodes"], occupied, "pa-wal", 556, 58, 226, 72, "DiskLogHandler (CLog)", "wait_lsn(page.lsn / commit_lsn)", role="purple", badge="1. WAL First")
    draw_node(layers["nodes"], occupied, "pa-dwb", 800, 58, 226, 72, "DiskDoubleWriteBuffer", "add_page → fsync(dblwr) → 写数据页", role="amber", badge="2. DWB Flush")
    draw_node(layers["nodes"], occupied, "pa-ckpt", 556, 172, 470, 72, "Db::sync() 检查点与 Db::init() 恢复", "冻结门禁 → GC → 采样 target_lsn → DWB/数据 fsync → rename sys.db", role="teal", badge="3. Checkpoint")

    files = [
        ("pa-f1", 34, "sys.db (控制文件)", "<checkpoint_lsn> <commit_seq>", "amber"),
        ("pa-f2", 234, "*.table (Schema JSON)", "字段偏移 + __trx_xid_begin/end", "gray"),
        ("pa-f3", 434, "*.data (Heap 8KB 页)", "Page 0 Bitmap + Record Pages", "teal"),
        ("pa-f4", 634, "*.index (B+Tree 8KB 页)", "IndexFileHeader + 索引结点页", "teal"),
        ("pa-f5", 834, "dblwr.db & clog_*.log", "16页双写副本 + 分段 WAL 日志", "purple"),
    ]
    for fid, fx, fl, fs, fr in files:
        draw_node(layers["nodes"], occupied, fid, fx, 342, 188, 90, fl, fs, role=fr)

    cs = (w, h)
    draw_edge(layers["edges"], occupied, svg_id, [(144, 130), (144, 172)], "产生 redo", color_name="purple", label_pos=(144, 151), canvas_size=cs)
    draw_edge(layers["edges"], occupied, svg_id, [(144, 172), (144, 151), (528, 151), (528, 94), (556, 94)], "刷盘前 wait_lsn", color_name="purple", label_pos=(388, 151), canvas_size=cs)
    draw_edge(layers["edges"], occupied, svg_id, [(782, 94), (800, 94)], "", color_name="amber", canvas_size=cs)
    draw_edge(layers["edges"], occupied, svg_id, [(791, 244), (791, 274), (128, 274), (128, 342)], "tmp+fsync+rename", color_name="amber", label_pos=(320, 274), canvas_size=cs)
    draw_edge(layers["edges"], occupied, svg_id, [(913, 130), (913, 151), (528, 151), (528, 342)], "写回真实数据页", color_name="teal", label_pos=(528, 274), canvas_size=cs)
    draw_edge(layers["edges"], occupied, svg_id, [(669, 130), (669, 151), (928, 151), (928, 342)], "持久化副本/日志", color_name="purple", label_pos=(928, 274), canvas_size=cs)

    return svg


def build_mvcc_occ_architecture_svg() -> ET.Element:
    """MVCC & OCC Tuple Versioning, TxnStatusTable, and Snapshot Isolation Architecture."""
    w, h = 1060, 440
    svg_id = "miniob-mvcc-occ-architecture"
    svg, layers, occupied = new_svg(
        w,
        h,
        "MiniOB UPDATE、MVCC 与 OCC 版本架构图",
        "展示 8 字节隐藏事务列、追加写多版本元组、索引保留与新增、TxnStatusTable 原子提交点与 OCC 读集校验。",
        svg_id,
    )
    draw_lane(layers["lanes"], occupied, 16, 16, 336, 408, "1. 事务上下文 (MvccTrx / OccTrx)", "Snapshot Isolation (SI) & Read/Write Set", fill=C["band_blue"], stroke=C["blue_stroke"], badge_color=C["blue_badge"])
    draw_lane(layers["lanes"], occupied, 366, 16, 340, 408, "2. 堆表多版本元组与索引 (Heap & B+Tree)", "PostgreSQL-style Append Version", fill=C["band_amber"], stroke=C["amber_stroke"], badge_color=C["amber_badge"])
    draw_lane(layers["lanes"], occupied, 720, 16, 324, 408, "3. 原子提交点与 GC (Status & Watermark)", "TxnStatusTable + commit_mutex + MvccGc", fill=C["band_teal"], stroke=C["teal_stroke"], badge_color=C["teal_badge"])

    draw_node(layers["nodes"], occupied, "mo-snap", 34, 62, 300, 74, "事务启动与快照 (BEGIN)", "trx_id = next_id() · snapshot_seq = published_csn", role="blue", badge="Snapshot")
    draw_node(layers["nodes"], occupied, "mo-vis", 34, 162, 300, 76, "可见性解析 Resolve(xid, self)", "begin <= snapshot_seq < end (通过状态表解析 -T)", role="blue", badge="Visibility")
    draw_node(layers["nodes"], occupied, "mo-wset", 34, 264, 144, 134, "写集 operations_", "DELETE(old_rid) + INSERT(new_rid)", role="amber", badge="WriteSet")
    draw_node(layers["nodes"], occupied, "mo-rset", 190, 264, 144, 134, "OCC 读集 read_set", "{rid, begin, end} 提交前重读校验", role="purple", badge="-t occ")

    draw_node(layers["nodes"], occupied, "mo-old", 384, 62, 304, 82, "旧版本元组 (Old RID)", "begin_xid = csn_1 · end_xid = -T (删除标记)", role="amber", badge="Old Tuple")
    draw_node(layers["nodes"], occupied, "mo-new", 384, 174, 304, 82, "新版本元组 (New RID)", "begin_xid = -T · end_xid = INF (INT64_MAX)", role="teal", badge="New Tuple")
    draw_node(layers["nodes"], occupied, "mo-idx", 384, 288, 304, 110, "二级索引策略 (B+Tree Index)", "保留旧索引项 (K_old → Old RID) 供老快照回表；新增 (K_new → New RID)", role="blue", badge="Index Retention")

    draw_node(layers["nodes"], occupied, "mo-lock", 738, 62, 288, 76, "提交锁串行区 (commit_mutex)", "分配 commit_seq → 写 COMMIT 日志 wait_lsn", role="purple", badge="1. Serialize")
    draw_node(layers["nodes"], occupied, "mo-status", 738, 174, 288, 82, "TxnStatusTable (原子提交点)", "ACTIVE → COMMITTED(commit_seq) / ABORTED", role="teal", badge="2. Atomic Publish")
    draw_node(layers["nodes"], occupied, "mo-gc", 738, 288, 288, 110, "惰性回填与 MvccGc::collect()", "watermark = min(active snapshot_seq)；回填 -T → csn，清理死元组与旧索引", role="teal", badge="3. Backfill & GC")

    cs = (w, h)
    draw_edge(layers["edges"], occupied, svg_id, [(184, 136), (184, 162)], "", color_name="blue", canvas_size=cs)
    draw_edge(layers["edges"], occupied, svg_id, [(334, 103), (384, 103)], "end_xid=-T", color_name="amber", label_pos=(359, 52), canvas_size=cs)
    draw_edge(layers["edges"], occupied, svg_id, [(334, 215), (384, 215)], "begin_xid=-T", color_name="teal", label_pos=(359, 158), canvas_size=cs)
    draw_edge(layers["edges"], occupied, svg_id, [(536, 256), (536, 288)], "维护索引", color_name="blue", label_pos=(536, 272), canvas_size=cs)
    draw_edge(layers["edges"], occupied, svg_id, [(688, 215), (738, 215)], "解析 -T 状态", color_name="purple", label_pos=(713, 158), canvas_size=cs)
    draw_edge(layers["edges"], occupied, svg_id, [(882, 138), (882, 174)], "确立提交点", color_name="teal", label_pos=(882, 156), canvas_size=cs)
    draw_edge(layers["edges"], occupied, svg_id, [(882, 256), (882, 288)], "发布快照并回填", color_name="teal", label_pos=(882, 272), canvas_size=cs)
    draw_edge(layers["edges"], occupied, svg_id, [(738, 343), (688, 343)], "回收死版本", color_name="teal", label_pos=(713, 412), canvas_size=cs)

    return svg


def build_btree_clustered_architecture_svg() -> ET.Element:
    """B+Tree Clustered Table Engine & 4 Key Modes Architecture."""
    w, h = 1060, 420
    svg_id = "miniob-btree-clustered-architecture"
    svg, layers, occupied = new_svg(
        w,
        h,
        "MiniOB B+Tree 聚簇表存储引擎与四种 Key 模式架构图",
        "对比 ENGINE=btree 聚簇表（COMPOSITE / ROWID 叶子存整行）与二级索引（ATTR_MEMCMP_VALUE / ATTR_RID）的编码及回表路径。",
        svg_id,
    )
    draw_lane(layers["lanes"], occupied, 16, 16, 514, 388, "1. B+Tree 聚簇表主存储 (Clustered B+Tree: Leaf Value = Full Record)", "IndexFileHeader (v2) · 按主键或隐藏 8B RowID 聚簇存放整行", fill=C["band_blue"], stroke=C["blue_stroke"], badge_color=C["blue_badge"])
    draw_lane(layers["lanes"], occupied, 544, 16, 500, 388, "2. 二级索引与回表路径对比 (Secondary Index & Lookup Path)", "BtreePkIndex (主键值回表) vs BplusTreeIndex (Heap RID 回表)", fill=C["band_teal"], stroke=C["teal_stroke"], badge_color=C["teal_badge"])

    draw_node(layers["nodes"], occupied, "bt-comp", 36, 66, 228, 92, "COMPOSITE 模式", "Key: pk_1 | pk_2 ... · Val: 整行 Record", role="blue", badge="有主键 btree 表")
    draw_node(layers["nodes"], occupied, "bt-rowid", 282, 66, 228, 92, "ROWID 模式", "Key: 8B 自增 rowid · Val: 整行 Record", role="amber", badge="无主键 btree 表")
    draw_node(layers["nodes"], occupied, "bt-leaf", 36, 192, 474, 86, "B+Tree 聚簇叶子页 (Clustered Leaf Page)", "单列主键时直接包装为共享 BtreePkIndex；插入自动校验 RECORD_DUPLICATE_KEY", role="blue", badge="Leaf Page (8KB)")
    draw_node(layers["nodes"], occupied, "bt-rec", 36, 306, 474, 76, "崩溃一致性与 RowID 恢复", "复用 BplusTreeMiniTransaction (MTR 日志)；重启取 max(header.next_rowid, max_key+1)", role="purple", badge="MTR & Recovery")

    draw_node(layers["nodes"], occupied, "bt-sec", 564, 66, 460, 92, "ATTR_MEMCMP_VALUE 模式 (btree/lsm 二级索引)", "Key: [索引属性 | 主键编码] · Value: 主键编码 (PK / RowID)", role="teal", badge="BtreePkIndex")
    draw_node(layers["nodes"], occupied, "bt-lookup", 564, 192, 460, 86, "聚簇表回表：Table::get_record_by_key(pk)", "二级索引扫出主键编码 → 到聚簇 B+Tree 点查取出整行 Record Bytes", role="teal", badge="PK Lookup")
    draw_node(layers["nodes"], occupied, "bt-heap", 564, 306, 460, 76, "对照：ATTR_RID 模式 (heap 二级索引，v0 兼容)", "Key: [索引属性 | RID(page,slot)] · Value: RID → 直接定位 Heap 数据页槽位", role="gray", badge="Heap RID Lookup")

    cs = (w, h)
    draw_edge(layers["edges"], occupied, svg_id, [(150, 158), (150, 192)], "", color_name="blue", canvas_size=cs)
    draw_edge(layers["edges"], occupied, svg_id, [(396, 158), (396, 192)], "", color_name="amber", canvas_size=cs)
    draw_edge(layers["edges"], occupied, svg_id, [(273, 278), (273, 306)], "", color_name="purple", canvas_size=cs)
    draw_edge(layers["edges"], occupied, svg_id, [(794, 158), (794, 192)], "返回主键编码", color_name="teal", label_pos=(794, 175), canvas_size=cs)
    draw_edge(layers["edges"], occupied, svg_id, [(564, 235), (510, 235)], "按 PK 点查", color_name="teal", label_pos=(537, 175), canvas_size=cs)

    return svg


ARCH_BUILDERS = {
    "miniob-architecture": build_miniob_architecture_svg,
    "miniob-persistence-architecture": build_storage_persistence_architecture_svg,
    "miniob-mvcc-occ-architecture": build_mvcc_occ_architecture_svg,
    "miniob-btree-clustered-architecture": build_btree_clustered_architecture_svg,
}


def build() -> list[Path]:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    flows = json.loads(FLOWS_PATH.read_text(encoding="utf-8"))
    written: list[Path] = []

    for name, builder in ARCH_BUILDERS.items():
        tree = ET.ElementTree(builder())
        ET.indent(tree, space="  ")
        out_path = OUT_DIR / f"{name}.svg"
        tree.write(out_path, encoding="utf-8", xml_declaration=True)
        written.append(out_path)

    for flow in flows:
        tree = ET.ElementTree(build_flow_svg(flow))
        ET.indent(tree, space="  ")
        out_path = OUT_DIR / f"flow-{flow['id']}.svg"
        tree.write(out_path, encoding="utf-8", xml_declaration=True)
        written.append(out_path)

    return written


def inline_svg_for_flow(flow: dict) -> str:
    svg = build_flow_svg(flow)
    ET.indent(svg, space="  ")
    return ET.tostring(svg, encoding="unicode")


def inline_svg_by_name(name: str) -> str:
    svg = ARCH_BUILDERS[name]()
    ET.indent(svg, space="  ")
    return ET.tostring(svg, encoding="unicode")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output_dir", nargs="?", type=Path, default=OUT_DIR)
    parser.parse_args()
    paths = build()
    print(f"Generated {len(paths)} deterministic SVG diagrams in {OUT_DIR}")


if __name__ == "__main__":
    main()
