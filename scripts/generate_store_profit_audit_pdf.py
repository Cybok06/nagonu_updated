from collections import defaultdict
from datetime import datetime
from pathlib import Path
import sys

from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.platypus import (
    KeepTogether,
    PageBreak,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from db import db


REPORT_DATE = datetime(2026, 8, 17)
REPORT_END = datetime(2026, 8, 18)
OUTPUT = PROJECT_ROOT / "store_profit_audit_2026-08-17.pdf"

NAVY = colors.HexColor("#172554")
BLUE = colors.HexColor("#2563EB")
PALE_BLUE = colors.HexColor("#EFF6FF")
GREEN = colors.HexColor("#15803D")
PALE_GREEN = colors.HexColor("#F0FDF4")
RED = colors.HexColor("#B91C1C")
PALE_RED = colors.HexColor("#FEF2F2")
SLATE = colors.HexColor("#475569")
LIGHT = colors.HexColor("#F8FAFC")
GRID = colors.HexColor("#CBD5E1")


def money(value):
    try:
        return round(float(value or 0), 2)
    except Exception:
        return 0.0


def fmt(value):
    return f"GHS {money(value):,.2f}"


def ptext(value):
    text = str(value or "-")
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def page_footer(canvas, doc):
    canvas.saveState()
    width, _height = landscape(A4)
    canvas.setStrokeColor(GRID)
    canvas.line(14 * mm, 11 * mm, width - 14 * mm, 11 * mm)
    canvas.setFont("Helvetica", 8)
    canvas.setFillColor(SLATE)
    canvas.drawString(14 * mm, 6.5 * mm, "Nagonu Store Profit Reconciliation — read-only audit")
    canvas.drawRightString(width - 14 * mm, 6.5 * mm, f"Page {doc.page}")
    canvas.restoreState()


def table_style(header=True, numeric_cols=()):
    commands = [
        ("GRID", (0, 0), (-1, -1), 0.35, GRID),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("FONTNAME", (0, 0), (-1, -1), "Helvetica"),
        ("FONTSIZE", (0, 0), (-1, -1), 7.5),
        ("TOPPADDING", (0, 0), (-1, -1), 5),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
        ("ROWBACKGROUNDS", (0, 1 if header else 0), (-1, -1), [colors.white, LIGHT]),
    ]
    if header:
        commands += [
            ("BACKGROUND", (0, 0), (-1, 0), NAVY),
            ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ]
    for col in numeric_cols:
        commands.append(("ALIGN", (col, 1 if header else 0), (col, -1), "RIGHT"))
    return TableStyle(commands)


def load_report_data():
    query = {
        "created_at": {"$gte": REPORT_DATE, "$lt": REPORT_END},
        "$or": [
            {"store_slug": {"$exists": True, "$nin": ["", None]}},
            {"debug.store_checkout": True},
        ],
    }
    orders = list(db["orders"].find(query).sort([("store_slug", 1), ("created_at", 1)]))
    stores = {
        store.get("slug"): store
        for store in db["stores"].find({}, {"slug": 1, "name": 1, "owner_id": 1})
    }
    grouped = defaultdict(list)
    for order in orders:
        grouped[order.get("store_slug") or "(missing-store-slug)"].append(order)
    return stores, grouped


def calculate_order(order):
    line_rows = []
    correct = 0.0
    previously_saved = 0.0
    missing_fields = 0
    for index, item in enumerate(order.get("items") or []):
        selling = money(item.get("amount"))
        base = money(item.get("base_amount"))
        recalculated = max(0.0, round(selling - base, 2))
        saved_raw = item.get("store_profit_amount")
        if saved_raw is None:
            missing_fields += 1
            saved = 0.0
            saved_label = "Missing (0.00)"
        else:
            saved = money(saved_raw)
            saved_label = fmt(saved)
        variance = round(recalculated - saved, 2)
        correct += recalculated
        previously_saved += saved
        line_rows.append(
            {
                "index": index + 1,
                "service": item.get("serviceName") or "-",
                "bundle": item.get("value") or "-",
                "selling": selling,
                "base": base,
                "saved": saved,
                "saved_label": saved_label,
                "correct": recalculated,
                "variance": variance,
            }
        )
    return {
        "lines": line_rows,
        "correct": round(correct, 2),
        "saved": round(previously_saved, 2),
        "variance": round(correct - previously_saved, 2),
        "missing_fields": missing_fields,
        "sales": money(order.get("total_amount")),
        "system_profit": money(order.get("profit_amount_total")),
    }


def build_pdf():
    stores, grouped = load_report_data()
    styles = getSampleStyleSheet()
    title = ParagraphStyle("TitleAudit", parent=styles["Title"], textColor=NAVY, fontSize=23, leading=27, alignment=TA_LEFT)
    subtitle = ParagraphStyle("SubtitleAudit", parent=styles["Normal"], textColor=SLATE, fontSize=10, leading=14)
    h1 = ParagraphStyle("H1Audit", parent=styles["Heading1"], textColor=NAVY, fontSize=16, leading=20, spaceAfter=7)
    h2 = ParagraphStyle("H2Audit", parent=styles["Heading2"], textColor=BLUE, fontSize=11, leading=14, spaceAfter=5)
    small = ParagraphStyle("SmallAudit", parent=styles["Normal"], textColor=SLATE, fontSize=8, leading=11)
    body = ParagraphStyle("BodyAudit", parent=styles["Normal"], textColor=colors.HexColor("#1E293B"), fontSize=9, leading=13)

    calculated = {}
    global_sales = global_correct = global_saved = global_system = 0.0
    global_orders = global_lines = global_missing = 0
    store_summaries = []
    for slug, orders in grouped.items():
        rows = []
        for order in orders:
            result = calculate_order(order)
            calculated[str(order.get("_id"))] = result
            rows.append(result)
        summary = {
            "slug": slug,
            "name": (stores.get(slug) or {}).get("name") or slug,
            "orders": len(orders),
            "lines": sum(len(row["lines"]) for row in rows),
            "sales": round(sum(row["sales"] for row in rows), 2),
            "correct": round(sum(row["correct"] for row in rows), 2),
            "saved": round(sum(row["saved"] for row in rows), 2),
            "variance": round(sum(row["variance"] for row in rows), 2),
            "system_profit": round(sum(row["system_profit"] for row in rows), 2),
            "missing": sum(row["missing_fields"] for row in rows),
        }
        store_summaries.append(summary)
        global_orders += summary["orders"]
        global_lines += summary["lines"]
        global_sales += summary["sales"]
        global_correct += summary["correct"]
        global_saved += summary["saved"]
        global_system += summary["system_profit"]
        global_missing += summary["missing"]

    store_summaries.sort(key=lambda row: (-row["variance"], row["name"].lower()))
    doc = SimpleDocTemplate(
        str(OUTPUT), pagesize=landscape(A4),
        rightMargin=14 * mm, leftMargin=14 * mm, topMargin=14 * mm, bottomMargin=16 * mm,
        title="Store Profit Audit — 17 August 2026",
        author="Nagonu Data Services",
    )
    story = []
    story += [
        Paragraph("Store Profit Reconciliation Audit", title),
        Paragraph("Storefront orders placed on 17 August 2026 · Africa/Accra (GMT)", subtitle),
        Spacer(1, 5 * mm),
    ]

    metric_data = [
        ["Store orders", "Stores", "Customer sales", "Saved store profit", "Correct store profit", "Understatement"],
        [str(global_orders), str(len(store_summaries)), fmt(global_sales), fmt(global_saved), fmt(global_correct), fmt(global_correct - global_saved)],
    ]
    metrics = Table(metric_data, colWidths=[31 * mm, 27 * mm, 43 * mm, 45 * mm, 45 * mm, 43 * mm])
    metrics.setStyle(table_style(numeric_cols=(2, 3, 4, 5)))
    metrics.setStyle(TableStyle([("BACKGROUND", (5, 1), (5, 1), PALE_RED), ("TEXTCOLOR", (5, 1), (5, 1), RED), ("FONTNAME", (0, 1), (-1, 1), "Helvetica-Bold")]))
    story += [metrics, Spacer(1, 4 * mm)]
    story.append(Paragraph(
        "<b>Independent formula:</b> Correct store profit = customer selling amount − store base amount. "
        "The report deliberately ignores the saved <i>store_profit_amount</i> when recalculating. "
        "The saved value is shown only for comparison. The order-level <i>profit_amount_total</i> is system/platform profit and is reported separately.",
        body,
    ))
    story += [Spacer(1, 4 * mm), Paragraph("Store-level summary", h1)]

    summary_rows = [["Store", "Orders", "Lines", "Sales", "Saved profit", "Correct profit", "Variance", "Missing fields"]]
    for row in store_summaries:
        summary_rows.append([
            Paragraph(ptext(row["name"]), small), row["orders"], row["lines"], fmt(row["sales"]),
            fmt(row["saved"]), fmt(row["correct"]), fmt(row["variance"]), row["missing"],
        ])
    summary_rows.append(["TOTAL", global_orders, global_lines, fmt(global_sales), fmt(global_saved), fmt(global_correct), fmt(global_correct - global_saved), global_missing])
    summary_table = Table(summary_rows, repeatRows=1, colWidths=[61 * mm, 18 * mm, 16 * mm, 32 * mm, 34 * mm, 34 * mm, 31 * mm, 24 * mm])
    summary_table.setStyle(table_style(numeric_cols=(1, 2, 3, 4, 5, 6, 7)))
    summary_table.setStyle(TableStyle([
        ("BACKGROUND", (0, -1), (-1, -1), PALE_BLUE), ("FONTNAME", (0, -1), (-1, -1), "Helvetica-Bold"),
        ("TEXTCOLOR", (6, 1), (6, -1), RED),
    ]))
    story += [summary_table, PageBreak()]

    for store_index, summary in enumerate(store_summaries):
        slug = summary["slug"]
        orders = grouped[slug]
        story.append(Paragraph(ptext(summary["name"]), h1))
        story.append(Paragraph(f"Store slug: <b>{ptext(slug)}</b>", small))
        store_metrics = [
            ["Orders", "Lines", "Sales", "Saved profit", "Correct profit", "Variance", "System profit"],
            [summary["orders"], summary["lines"], fmt(summary["sales"]), fmt(summary["saved"]), fmt(summary["correct"]), fmt(summary["variance"]), fmt(summary["system_profit"])],
        ]
        t = Table(store_metrics, colWidths=[24 * mm, 22 * mm, 37 * mm, 39 * mm, 39 * mm, 35 * mm, 38 * mm])
        t.setStyle(table_style(numeric_cols=(0, 1, 2, 3, 4, 5, 6)))
        t.setStyle(TableStyle([("BACKGROUND", (5, 1), (5, 1), PALE_RED if summary["variance"] else PALE_GREEN), ("TEXTCOLOR", (5, 1), (5, 1), RED if summary["variance"] else GREEN)]))
        story += [t, Spacer(1, 4 * mm)]

        for order in orders:
            result = calculated[str(order.get("_id"))]
            created = order.get("created_at")
            placed = created.strftime("17 Aug 2026, %H:%M:%S GMT") if isinstance(created, datetime) else "Unknown"
            status = str(order.get("status") or "unknown").replace("_", " ").title()
            heading = (
                f"Order {ptext(order.get('order_id'))} · {placed} · {ptext(status)} · "
                f"Correct profit {fmt(result['correct'])} · Variance {fmt(result['variance'])}"
            )
            story.append(Paragraph(heading, h2))
            rows = [["#", "Service", "Bundle", "Selling amount", "Base amount", "Saved profit", "Correct profit", "Variance"]]
            for line in result["lines"]:
                rows.append([
                    line["index"], Paragraph(ptext(line["service"]), small), Paragraph(ptext(line["bundle"]), small),
                    fmt(line["selling"]), fmt(line["base"]), line["saved_label"], fmt(line["correct"]), fmt(line["variance"]),
                ])
            rows.append(["", "ORDER TOTAL", "", fmt(result["sales"]), "", fmt(result["saved"]), fmt(result["correct"]), fmt(result["variance"])])
            ot = Table(rows, repeatRows=1, colWidths=[9 * mm, 49 * mm, 30 * mm, 35 * mm, 33 * mm, 36 * mm, 36 * mm, 31 * mm])
            ot.setStyle(table_style(numeric_cols=(0, 3, 4, 5, 6, 7)))
            ot.setStyle(TableStyle([
                ("BACKGROUND", (0, -1), (-1, -1), PALE_BLUE), ("FONTNAME", (0, -1), (-1, -1), "Helvetica-Bold"),
                ("TEXTCOLOR", (7, 1), (7, -1), RED),
            ]))
            story += [ot, Spacer(1, 4 * mm)]
        if store_index < len(store_summaries) - 1:
            story.append(PageBreak())

    story += [PageBreak(), Paragraph("Audit notes", h1)]
    notes = [
        "Scope is limited to orders created from 00:00:00 through 23:59:59 on 17 August 2026, Africa/Accra (GMT).",
        "Only orders identified as storefront checkout orders were included.",
        "No existing calculated profit field was used to derive the correct profit.",
        "A missing saved profit field is treated as GHS 0.00 for variance analysis.",
        "Negative store profit is floored at GHS 0.00, matching the current Store Page implementation.",
        "This PDF is read-only evidence. It does not credit stores or modify orders, transactions, or balances.",
    ]
    for note in notes:
        story.append(Paragraph(f"• {ptext(note)}", body))
        story.append(Spacer(1, 1.5 * mm))

    doc.build(story, onFirstPage=page_footer, onLaterPages=page_footer)
    print(OUTPUT)


if __name__ == "__main__":
    build_pdf()
