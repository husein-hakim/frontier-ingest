from __future__ import annotations

from pathlib import Path
from textwrap import wrap

from reportlab.lib.colors import HexColor, white
from reportlab.lib.pagesizes import A4, landscape
from reportlab.pdfbase.pdfmetrics import stringWidth
from reportlab.pdfgen.canvas import Canvas


ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / "architecture.pdf"
PAGE_W, PAGE_H = landscape(A4)

NAVY = HexColor("#17324D")
TEAL = HexColor("#168C84")
BLUE = HexColor("#2F6DA1")
ORANGE = HexColor("#E47B45")
GREEN = HexColor("#2E8B57")
INK = HexColor("#1F2937")
MUTED = HexColor("#5F6B76")
LINE = HexColor("#D7DEE5")
LIGHT = HexColor("#F4F7F9")
PALE_TEAL = HexColor("#EAF7F5")
PALE_BLUE = HexColor("#EBF3FA")
PALE_ORANGE = HexColor("#FFF2EA")


def rounded_box(c: Canvas, x: float, y: float, w: float, h: float, fill, stroke=LINE, radius=8) -> None:
    c.setFillColor(fill)
    c.setStrokeColor(stroke)
    c.setLineWidth(0.8)
    c.roundRect(x, y, w, h, radius, fill=1, stroke=1)


def draw_wrapped(
    c: Canvas,
    text: str,
    x: float,
    y: float,
    width: float,
    font: str = "Helvetica",
    size: float = 8,
    color=INK,
    leading: float | None = None,
    max_lines: int | None = None,
) -> float:
    leading = leading or size * 1.25
    chars = max(10, int(width / (size * 0.52)))
    lines = []
    for paragraph in text.split("\n"):
        lines.extend(wrap(paragraph, width=chars, break_long_words=False) or [""])
    if max_lines and len(lines) > max_lines:
        lines = lines[:max_lines]
        lines[-1] = lines[-1].rstrip(" .") + "..."
    c.setFont(font, size)
    c.setFillColor(color)
    for line in lines:
        c.drawString(x, y, line)
        y -= leading
    return y


def draw_header(c: Canvas, page: int, title: str, subtitle: str) -> None:
    c.setFillColor(NAVY)
    c.rect(0, PAGE_H - 74, PAGE_W, 74, fill=1, stroke=0)
    c.setFillColor(white)
    c.setFont("Helvetica-Bold", 20)
    c.drawString(34, PAGE_H - 35, title)
    c.setFont("Helvetica", 9.5)
    c.setFillColor(HexColor("#D9E7F2"))
    c.drawString(34, PAGE_H - 54, subtitle)
    c.setFont("Helvetica-Bold", 8)
    c.drawRightString(PAGE_W - 34, PAGE_H - 38, f"FRONTIER INGEST  |  {page}/3")


def draw_footer(c: Canvas) -> None:
    c.setStrokeColor(LINE)
    c.line(34, 27, PAGE_W - 34, 27)
    c.setFont("Helvetica", 7)
    c.setFillColor(MUTED)
    c.drawString(34, 16, "Interview submission architecture - evidence-gated, idempotent and source-compliant")
    c.drawRightString(PAGE_W - 34, 16, "Generated 29 Aug 2026")


def draw_arrow(c: Canvas, x1: float, y: float, x2: float, color=BLUE) -> None:
    c.setStrokeColor(color)
    c.setFillColor(color)
    c.setLineWidth(1.4)
    c.line(x1, y, x2 - 6, y)
    c.line(x2 - 6, y, x2 - 11, y + 4)
    c.line(x2 - 6, y, x2 - 11, y - 4)


def draw_left_arrow(c: Canvas, x1: float, y: float, x2: float, color=BLUE) -> None:
    c.setStrokeColor(color)
    c.setFillColor(color)
    c.setLineWidth(1.4)
    c.line(x1, y, x2 + 6, y)
    c.line(x2 + 6, y, x2 + 11, y + 4)
    c.line(x2 + 6, y, x2 + 11, y - 4)


def page_one(c: Canvas) -> None:
    draw_header(
        c,
        1,
        "Evidence-Gated Adaptive Ingestion",
        "One production pipeline, two workload lanes, and a hard rule: no structured field without source evidence.",
    )

    y_lane = PAGE_H - 112
    rounded_box(c, 34, y_lane - 39, 365, 44, PALE_BLUE, stroke=HexColor("#B8D1E5"))
    rounded_box(c, 415, y_lane - 39, 393, 44, PALE_ORANGE, stroke=HexColor("#F2C7AE"))
    c.setFont("Helvetica-Bold", 10)
    c.setFillColor(BLUE)
    c.drawString(48, y_lane - 12, "CATALOG LANE")
    c.setFillColor(ORANGE)
    c.drawString(429, y_lane - 12, "FRESHNESS LANE")
    draw_wrapped(c, "Bulk pages, checkpointed batches: 1,000 startups, products and papers.", 139, y_lane - 12, 245, size=8)
    draw_wrapped(c, "Strict timestamp proof: all qualifying jobs and news from the last 24 hours.", 530, y_lane - 12, 265, size=8)

    stages = [
        ("1", "Discover", "APIs, feeds, pagination", PALE_BLUE),
        ("2", "Fetch once", "Rate limits, retries, block detection", LIGHT),
        ("3", "Raw evidence", "SHA-256 content store", PALE_TEAL),
        ("4", "Extract", "JSON, feeds, JSON-LD, HTML", LIGHT),
        ("5", "Evidence gate", "Only unresolved fields continue", PALE_ORANGE),
        ("6", "Targeted LLM", "Small fragments, typed output", PALE_TEAL),
        ("7", "Validate + link", "Freshness, schema, entities", LIGHT),
        ("8", "Upsert + export", "Postgres, JSON, six Sheet tabs", PALE_BLUE),
    ]
    left = 34
    gap = 9
    box_w = (PAGE_W - 68 - gap * 3) / 4
    box_h = 82
    rows_y = [PAGE_H - 264, PAGE_H - 375]
    for index, (number, title, body, fill) in enumerate(stages):
        row, base_col = divmod(index, 4)
        col = base_col if row == 0 else 3 - base_col
        x = left + col * (box_w + gap)
        y = rows_y[row]
        rounded_box(c, x, y, box_w, box_h, fill)
        c.setFillColor(NAVY)
        c.circle(x + 18, y + box_h - 18, 10, fill=1, stroke=0)
        c.setFillColor(white)
        c.setFont("Helvetica-Bold", 8)
        c.drawCentredString(x + 18, y + box_h - 21, number)
        c.setFillColor(INK)
        c.setFont("Helvetica-Bold", 10)
        c.drawString(x + 34, y + box_h - 21, title)
        draw_wrapped(c, body, x + 12, y + box_h - 44, box_w - 24, size=7.5, max_lines=3)
        if row == 0 and col < 3:
            draw_arrow(c, x + box_w, y + box_h / 2, x + box_w + gap)
        elif row == 1 and index < 7:
            draw_left_arrow(c, x, y + box_h / 2, x - gap)
    connector_x = left + 3 * (box_w + gap) + box_w / 2
    c.setStrokeColor(BLUE)
    c.setLineWidth(1.4)
    c.line(connector_x, rows_y[0], connector_x, rows_y[1] + box_h + 6)
    c.line(connector_x, rows_y[1] + box_h + 6, connector_x - 4, rows_y[1] + box_h + 11)
    c.line(connector_x, rows_y[1] + box_h + 6, connector_x + 4, rows_y[1] + box_h + 11)

    callouts = [
        ("Deterministic first", "Structured source fields consume zero LLM tokens.", BLUE),
        ("Field-level lineage", "Every record carries source URL, collected time and content hash.", TEAL),
        ("Safe abstention", "Uncertain freshness or entity matches are rejected or flagged for review.", ORANGE),
    ]
    callout_y = 88
    callout_w = (PAGE_W - 68 - 18) / 3
    for i, (title, body, color) in enumerate(callouts):
        x = 34 + i * (callout_w + 9)
        rounded_box(c, x, callout_y, callout_w, 70, white, stroke=LINE)
        c.setFillColor(color)
        c.rect(x, callout_y, 5, 70, fill=1, stroke=0)
        c.setFillColor(INK)
        c.setFont("Helvetica-Bold", 9)
        c.drawString(x + 16, callout_y + 49, title)
        draw_wrapped(c, body, x + 16, callout_y + 32, callout_w - 29, size=7.5, max_lines=3)
    draw_footer(c)
    c.showPage()


def table_row(c: Canvas, x: float, y: float, widths: list[float], values: list[str], height: float, header=False) -> None:
    fill = NAVY if header else white
    c.setFillColor(fill)
    c.setStrokeColor(LINE)
    c.rect(x, y - height, sum(widths), height, fill=1, stroke=1)
    cursor = x
    for index, (width, value) in enumerate(zip(widths, values)):
        if index:
            c.line(cursor, y, cursor, y - height)
        c.setFillColor(white if header else INK)
        c.setFont("Helvetica-Bold" if header else "Helvetica", 7.2 if not header else 7.5)
        draw_wrapped(c, value, cursor + 6, y - 12, width - 12, font="Helvetica-Bold" if header else "Helvetica", size=7.2, color=white if header else INK, max_lines=3)
        cursor += width


def page_two(c: Canvas) -> None:
    draw_header(
        c,
        2,
        "Source plan and token economics",
        "Use the cheapest trustworthy representation first; reserve language models for genuine ambiguity.",
    )
    c.setFillColor(INK)
    c.setFont("Helvetica-Bold", 12)
    c.drawString(34, PAGE_H - 103, "Monitored source inventory")
    x = 34
    y = PAGE_H - 116
    widths = [74, 175, 206, 313]
    table_row(c, x, y, widths, ["Dataset", "Primary sources", "Captured evidence", "Access and validation"], 24, header=True)
    rows = [
        ["Startups + products", "Y Combinator AI company directory", "Name, employee count, website, description, product, batch, status", "Paginated public directory; canonical company URL; deterministic parsing."],
        ["Research papers", "Hugging Face daily papers + arXiv + GitHub GraphQL", "Title, authors, abstract, paper URL, repository URL, stars and star timestamp", "Code-linked papers only; GitHub enrichment batched up to 40 repositories per call."],
        ["AI jobs", "Arbeitnow, Remote OK, Jobicy, Himalayas, Remotive", "Company, title, published time, remote flag, location and full description", "Five official/public APIs; provable age <= 24h; source-specific attribution honored."],
        ["AI news", "OpenAI, DeepMind, Hugging Face, MIT News AI, TechCrunch AI", "Title, publisher, authors, published time and full article text", "RSS/Atom discovery followed by one compliant article fetch; age <= 24h."],
    ]
    y -= 24
    for row in rows:
        table_row(c, x, y, widths, row, 40)
        y -= 40

    lower_y = 64
    left_w = 375
    right_x = 424
    right_w = PAGE_W - 34 - right_x
    rounded_box(c, 34, lower_y, left_w, 220, PALE_TEAL, stroke=HexColor("#B8DDD8"))
    rounded_box(c, right_x, lower_y, right_w, 220, LIGHT, stroke=LINE)
    c.setFillColor(TEAL)
    c.setFont("Helvetica-Bold", 11)
    c.drawString(50, lower_y + 202, "Token budgeter: selection, not summarization")
    token_steps = [
        ("1. Parse locally", "APIs, feeds, metadata and JSON-LD fill known fields at zero token cost."),
        ("2. Detect the gap", "The evidence gate names only missing or conflicting fields."),
        ("3. Select fragments", "Headings and nearby sentences are scored; whole pages are never sent."),
        ("4. Enforce hard limits", "Per-record, per-source and per-run budgets stop token drift."),
        ("5. Validate or abstain", "Typed output must cite supplied evidence; retry once, then flag review."),
    ]
    yy = lower_y + 177
    for title, body in token_steps:
        c.setFillColor(TEAL)
        c.circle(57, yy + 2, 3, fill=1, stroke=0)
        c.setFillColor(INK)
        c.setFont("Helvetica-Bold", 8.2)
        c.drawString(68, yy, title)
        yy = draw_wrapped(c, body, 68, yy - 12, 320, size=7.2, max_lines=2) - 6

    c.setFillColor(NAVY)
    c.setFont("Helvetica-Bold", 11)
    c.drawString(right_x + 16, lower_y + 202, "Access policy and anti-bot boundary")
    policy = [
        "Priority: official API -> RSS/Atom -> sitemap -> permitted HTTP -> browser rendering only when allowed.",
        "Per-host concurrency, minimum delays, exponential retry and jitter prevent accidental overload.",
        "Robots rules, terms, cache headers and attribution are source configuration, not ad hoc code.",
        "Block-page signatures pause the adapter. The system never bypasses CAPTCHA or access controls.",
        "Raw-response hashes deduplicate downloads and allow parser replays without touching the source again.",
    ]
    yy = lower_y + 174
    for item in policy:
        c.setFillColor(ORANGE)
        c.rect(right_x + 17, yy - 1, 5, 5, fill=1, stroke=0)
        yy = draw_wrapped(c, item, right_x + 31, yy, right_w - 48, size=7.4, max_lines=3) - 8

    c.setFillColor(MUTED)
    c.setFont("Helvetica", 6.6)
    c.drawString(34, 55, "Representative endpoints: ycombinator.com/companies/industry/ai | huggingface.co/api/daily_papers | api.github.com/graphql")
    c.drawString(34, 44, "Freshness inputs: arbeitnow.com/api/job-board-api | remoteok.com/api | jobicy.com/jobs-rss-feed | remotive.com/api/remote-jobs")
    draw_footer(c)
    c.showPage()


def page_three(c: Canvas) -> None:
    draw_header(
        c,
        3,
        "500,000-record production design and verification",
        "At-least-once processing becomes safe through stable keys, leases, immutable evidence and idempotent upserts.",
    )
    c.setFont("Helvetica-Bold", 12)
    c.setFillColor(INK)
    c.drawString(34, PAGE_H - 103, "Scale-out topology")

    top_y = PAGE_H - 200
    nodes = [
        (34, 116, "Schedulers", "catalog + freshness\ncheckpoints", PALE_BLUE),
        (169, 126, "Durable queue", "source, page, priority,\nattempt", LIGHT),
        (314, 126, "Stateless workers", "bounded async fetch +\nparse", PALE_TEAL),
        (459, 142, "Object storage", "content-addressed raw\nevidence", LIGHT),
        (620, 188, "PostgreSQL", "leases, records, mappings,\nquality events", PALE_BLUE),
    ]
    for index, (x, width, title, body, fill) in enumerate(nodes):
        rounded_box(c, x, top_y, width, 76, fill)
        c.setFillColor(NAVY)
        c.setFont("Helvetica-Bold", 9)
        c.drawCentredString(x + width / 2, top_y + 53, title)
        lines = body.split("\n")
        c.setFont("Helvetica", 7.2)
        c.setFillColor(MUTED)
        for line_index, line in enumerate(lines):
            c.drawCentredString(x + width / 2, top_y + 34 - line_index * 11, line)
        if index < len(nodes) - 1:
            next_x = nodes[index + 1][0]
            draw_arrow(c, x + width, top_y + 38, next_x)

    cards = [
        (34, "Capacity", ["Pages stream; no full-dataset memory load.", "500-2,000 row database batches.", "Autoscale on queue lag and oldest-item age."], BLUE),
        (302, "Correctness", ["Stable key + unique constraint = idempotency.", "Postgres SKIP LOCKED leases prevent double work.", "Dead-letter queue retains raw evidence and error."], TEAL),
        (570, "Operations", ["Metrics by source: latency, 429s, blocks, yield.", "Separate provider token and request budgets.", "Reprocess from raw hashes after parser upgrades."], ORANGE),
    ]
    card_y = 252
    card_w = 238
    for x, title, bullets, color in cards:
        rounded_box(c, x, card_y, card_w, 126, white, stroke=LINE)
        c.setFillColor(color)
        c.rect(x, card_y + 94, card_w, 32, fill=1, stroke=0)
        c.setFillColor(white)
        c.setFont("Helvetica-Bold", 10)
        c.drawString(x + 13, card_y + 105, title)
        yy = card_y + 78
        for item in bullets:
            c.setFillColor(color)
            c.circle(x + 16, yy + 2, 2.5, fill=1, stroke=0)
            yy = draw_wrapped(c, item, x + 27, yy, card_w - 40, size=7.3, max_lines=2) - 6

    c.setFillColor(MUTED)
    c.setFont("Helvetica", 6.6)
    c.drawString(
        34,
        239,
        "Storage choice: PostgreSQL is the ACID source of truth with JSONB and pgvector candidate search; Neo4j is an optional, rebuildable projection for multi-hop traversal.",
    )

    c.setFillColor(INK)
    c.setFont("Helvetica-Bold", 12)
    c.drawString(34, 224, "Submission quality gate - clean run on 29 Aug 2026")
    x = 34
    y = 208
    widths = [174, 78, 92, 123, 123, 178]
    table_row(c, x, y, widths, ["Dataset", "Rows", "Unique keys", "Valid source URLs", "Evidence complete", "Result"], 23, header=True)
    quality_rows = [
        ["AI startups", "1,000", "1,000", "1,000", "1,000", "PASS"],
        ["AI products", "1,000", "1,000", "1,000", "1,000", "PASS"],
        ["Research papers + code", "1,000", "1,000", "1,000", "1,000", "PASS"],
        ["AI jobs, verified <= 24h", "65", "65", "65", "65", "PASS"],
        ["AI news, verified <= 24h", "5", "5", "5", "5", "PASS"],
    ]
    y -= 23
    for row in quality_rows:
        table_row(c, x, y, widths, row, 24)
        y -= 24

    c.setFillColor(MUTED)
    c.setFont("Helvetica-Oblique", 7)
    c.drawString(34, 54, "Freshness counts intentionally vary by run. Records without provable timestamps are excluded, not guessed.")
    draw_footer(c)
    c.showPage()


def build() -> None:
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    canvas = Canvas(str(OUTPUT), pagesize=landscape(A4), pageCompression=1)
    canvas.setTitle("Frontier Ingest - Production Architecture")
    canvas.setAuthor("Frontier Ingest interview submission")
    canvas.setSubject("Evidence-gated ingestion for AI startups, products, research papers, jobs and news")
    page_one(canvas)
    page_two(canvas)
    page_three(canvas)
    canvas.save()
    print(OUTPUT)


if __name__ == "__main__":
    build()
