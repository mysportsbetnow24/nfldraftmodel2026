from __future__ import annotations

import csv
import html
import zipfile
from pathlib import Path
from xml.sax.saxutils import escape

ROOT = Path(__file__).resolve().parents[1]
BIG_BOARD_CSV = ROOT / "data" / "outputs" / "big_board_2026.csv"
OWNER_NOTES_CSV = ROOT / "data" / "sources" / "manual" / "owner_scouting_notes_2026.csv"
REVIEW_CSV = ROOT / "data" / "sources" / "manual" / "primary_concerns_review_2026.csv"
OUTPUT_DIR = ROOT / "data" / "outputs" / "internal"
GUIDE_HTML = OUTPUT_DIR / "primary_concerns_all22_guide_2026.html"
GUIDE_DOCX = OUTPUT_DIR / "primary_concerns_all22_guide_2026.docx"

POSITION_GUIDE = {
    "QB": {
        "family": "QB",
        "concerns": [
            "Anticipation and timing: is he late on NFL-window throws or forcing the picture after it closes?",
            "Pressure response: does pocket movement stay calm, efficient, and on-platform when the rush compresses the set point?",
            "Turnover profile: are mistakes coming from aggression you can coach or processing misses that cap the ceiling?",
            "Third-down and red-zone problem solving: does decision speed hold when space gets tight and the disguise gets better?",
        ],
        "triggers": [
            "Low EPA/play or unstable efficiency",
            "Negative pressure-response profile",
            "High interception / turnover-worthy tendency",
            "Large gap between clean-pocket and pressured performance",
        ],
    },
    "RB": {
        "family": "RB",
        "concerns": [
            "Burst through daylight: does he create chunk plays once the crease opens or only take what is blocked?",
            "Contact balance: does he stay on track through glancing contact and arm tackles?",
            "Pass-game viability: does he help on third down as a receiver and protector?",
            "Decision tempo: does he press, cut, and reset efficiently without wasted steps?",
        ],
        "triggers": [
            "Low explosive-run rate",
            "Low missed-tackles-forced per touch",
            "Weak receiving involvement",
            "Poor pass-protection projection",
        ],
    },
    "WR": {
        "family": "REC",
        "concerns": [
            "Release plan vs press: does he have answers when corners disrupt timing at the line?",
            "Route pacing and stem control: does he manipulate leverage or just run to spots?",
            "Separation quality: does he win early, at the breakpoint, or only at the catch point?",
            "Finish strength: can he survive contact and still complete through the catch window?",
        ],
        "triggers": [
            "Low YPRR for the projection tier",
            "Low target-share / targets-per-route earning",
            "Poor short-area change-of-direction profile",
            "Short-arm or catch-radius limitation for boundary roles",
        ],
    },
    "TE": {
        "family": "REC",
        "concerns": [
            "Can he stay attached to the formation without telegraphing pass/run intent?",
            "Does he separate well enough against man coverage to be more than a schematic target?",
            "Can he hold route detail through traffic and linebacker contact?",
            "Is his body type and movement profile cleanly tied to one role or truly multiple roles?",
        ],
        "triggers": [
            "Low man-coverage efficiency",
            "Weak in-line blocking strain",
            "Low route-earning volume",
            "Mismatch-only usage without full-package evidence",
        ],
    },
    "OT": {
        "family": "OL",
        "concerns": [
            "Recovery after speed threat: can he reset his hands and feet when defenders chain counters?",
            "Anchor vs power: does he stop pocket collapse or survive only with help?",
            "Set-point discipline: does he create clean half-man leverage early in reps?",
            "How much room for error does his length/body type give him against NFL rushers?",
        ],
        "triggers": [
            "Short-arm or light-mass threshold misses",
            "Poor shuttle / movement profile",
            "High pressure-allowed rate",
            "Inconsistent pass-block grade vs strong competition",
        ],
    },
    "IOL": {
        "family": "OL",
        "concerns": [
            "Anchor and leverage late in the rep",
            "Processing on games, twists, and interior exchange communication",
            "Range to recover inside-out when rushers attack edges of the frame",
            "Whether movement skills are good enough to stay clean in space and on climbs",
        ],
        "triggers": [
            "Light interior anchor profile",
            "High pressure-allowed rate",
            "Weak pass-block efficiency",
            "Poor short-area movement metrics",
        ],
    },
    "EDGE": {
        "family": "DL",
        "concerns": [
            "Is the rush plan deeper than a first-step win?",
            "Can he corner, flatten, and finish rather than just generate initial stress?",
            "Does run-game edge setting hold well enough to avoid a package-only role?",
            "Are disruption totals repeatable or inflated by alignment/opponent context?",
        ],
        "triggers": [
            "Low pressure rate",
            "Low sacks-per-rush or finish rate",
            "Weak explosion / bend indicators",
            "Low hurry volume despite high pass-rush snaps",
        ],
    },
    "DT": {
        "family": "DL",
        "concerns": [
            "Pad level and block anchor on early downs",
            "Can he convert first contact into sustained interior push?",
            "Is the body type sufficient for NFL run-control demands?",
            "Does pass-rush impact hold from multiple alignments or only in a tailored role?",
        ],
        "triggers": [
            "Low interior pressure rate",
            "Light mass for interior anchor work",
            "Low lower-body explosion markers",
            "Run-defense inconsistency",
        ],
    },
    "LB": {
        "family": "LB",
        "concerns": [
            "Run-pass key speed and eye discipline",
            "Coverage spacing versus route combinations and tempo stress",
            "Block deconstruction when second-level bodies get on him clean",
            "Whether blitz/pressure flashes are utility or actually part of the role translation",
        ],
        "triggers": [
            "Poor shuttle/space-change profile",
            "Light play-strength profile",
            "Weak coverage grade relative to role",
            "Low stop-rate or thin near-ball impact",
        ],
    },
    "CB": {
        "family": "DB",
        "concerns": [
            "Long-speed recovery against vertical stress",
            "Eye discipline through stacked releases and late route breaks",
            "Ball finish at the catch point",
            "Run-support toughness and tackle finish when offenses target him intentionally",
        ],
        "triggers": [
            "Weak vertical speed threshold",
            "Low plays-on-ball rate",
            "High yards allowed per coverage snap",
            "Short-arm profile for outside work",
        ],
    },
    "S": {
        "family": "DB",
        "concerns": [
            "Range and overlap speed in split-field structures",
            "Communication and timing on rotations",
            "Open-field tackling under space stress",
            "Whether near-ball disruption is controlled utility or freelancing risk",
        ],
        "triggers": [
            "Weak range-speed profile",
            "Low plays-on-ball rate",
            "High yards allowed per coverage snap",
            "Unstable deep-half or post rotation discipline",
        ],
    },
}

ALL22_FOCUS = {
    "QB": [
        "Pre-snap identification vs rotation",
        "Time-to-throw discipline",
        "Middle-of-field accuracy under pressure",
        "Pocket climb vs edge pressure",
        "Anticipatory throws before break",
        "Red-zone compression decisions",
        "Third-down conversion throws",
        "Turnover-worthy play profile",
        "Late-down creation without chaos",
        "Two-minute operation command",
    ],
    "REC": [
        "Release plan vs press toolbox",
        "Route stem pacing and leverage",
        "Separation at top of route",
        "Ball tracking deep and boundary",
        "Hands reliability in traffic",
        "Contested-catch timing",
        "YAC path after first catch",
        "Effort as blocker",
        "Coverage recognition vs zone",
        "Response after failed rep",
    ],
    "RB": [
        "Tempo into developing gaps",
        "Read speed on split-zone concepts",
        "Contact balance through arm tackles",
        "Explosiveness through second level",
        "Cut efficiency without deceleration",
        "Ball security in traffic",
        "Route detail from backfield",
        "Pass-protection scan execution",
        "Short-yardage pad level",
        "Fourth-quarter stamina traits",
    ],
    "OL": [
        "Initial set angle consistency",
        "Recovery after edge stress",
        "Anchor versus power",
        "Hand replacement timing",
        "Rush pickup communication",
        "Leverage at point of attack",
        "Second-level body control",
        "Penalty tendency and causes",
        "Finish through whistle",
        "Play-to-play technique variance",
    ],
    "DL": [
        "Snap anticipation and get-off",
        "Rush plan sequencing",
        "Counter move timing",
        "Pad level through contact",
        "Block recognition in run fits",
        "Pursuit effort backside",
        "Pocket closing angle",
        "Contain discipline",
        "Late-game motor",
        "Finishing consistency",
    ],
    "LB": [
        "Run-pass key speed",
        "Fit integrity in run game",
        "Route combination recognition",
        "Space tackling profile",
        "Coverage eyes and leverage",
        "Block shed timing",
        "Blitz timing and lane choice",
        "Communication pre-snap",
        "Pursuit angles to perimeter",
        "Red-zone reaction speed",
    ],
    "DB": [
        "Press footwork and hand discipline",
        "Transition speed at breakpoints",
        "Route anticipation",
        "Ball tracking over shoulder",
        "Recovery speed after false step",
        "Support in run fits",
        "Tackle finish in space",
        "Leverage communication pre-snap",
        "Panic indicator at catch point",
        "Fourth-quarter competitiveness",
    ],
}


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _slugify(name: str) -> str:
    return name.lower().replace(" ", "-").replace(".", "").replace("'", "")


def build_review_csv() -> None:
    board_rows = _read_csv(BIG_BOARD_CSV)
    owner_rows = {
        (row.get("slug") or _slugify(row.get("player_name", ""))).strip().lower(): row
        for row in _read_csv(OWNER_NOTES_CSV)
    }
    REVIEW_CSV.parent.mkdir(parents=True, exist_ok=True)
    with REVIEW_CSV.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "slug",
                "player_name",
                "position",
                "school",
                "current_generated_primary_concerns",
                "current_public_primary_concerns_override",
                "reviewed_primary_concerns",
                "review_status",
                "owner_notes",
            ],
        )
        writer.writeheader()
        for row in board_rows:
            slug = (row.get("player_slug") or _slugify(row.get("player_name", ""))).strip().lower()
            owner_row = owner_rows.get(slug, {})
            writer.writerow(
                {
                    "slug": slug,
                    "player_name": row.get("player_name", ""),
                    "position": row.get("position", ""),
                    "school": row.get("school", ""),
                    "current_generated_primary_concerns": row.get("scouting_primary_concerns", ""),
                    "current_public_primary_concerns_override": owner_row.get("public_primary_concerns", ""),
                    "reviewed_primary_concerns": "",
                    "review_status": "",
                    "owner_notes": "",
                }
            )


def _html_list(items: list[str]) -> str:
    return "".join(f"<li>{html.escape(item)}</li>" for item in items)


def _write_simple_docx(path: Path, paragraphs: list[str]) -> None:
    content_types = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
  <Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
  <Default Extension="xml" ContentType="application/xml"/>
  <Override PartName="/word/document.xml" ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.document.main+xml"/>
  <Override PartName="/docProps/core.xml" ContentType="application/vnd.openxmlformats-package.core-properties+xml"/>
  <Override PartName="/docProps/app.xml" ContentType="application/vnd.openxmlformats-officedocument.extended-properties+xml"/>
</Types>"""
    rels = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="word/document.xml"/>
  <Relationship Id="rId2" Type="http://schemas.openxmlformats.org/package/2006/relationships/metadata/core-properties" Target="docProps/core.xml"/>
  <Relationship Id="rId3" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/extended-properties" Target="docProps/app.xml"/>
</Relationships>"""
    app = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Properties xmlns="http://schemas.openxmlformats.org/officeDocument/2006/extended-properties"
 xmlns:vt="http://schemas.openxmlformats.org/officeDocument/2006/docPropsVTypes">
  <Application>Codex</Application>
</Properties>"""
    core = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<cp:coreProperties xmlns:cp="http://schemas.openxmlformats.org/package/2006/metadata/core-properties"
 xmlns:dc="http://purl.org/dc/elements/1.1/"
 xmlns:dcterms="http://purl.org/dc/terms/"
 xmlns:dcmitype="http://purl.org/dc/dcmitype/"
 xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <dc:title>Scouting Grade Primary Concerns + All-22 Guide (2026)</dc:title>
  <dc:creator>Codex</dc:creator>
</cp:coreProperties>"""
    body = []
    for paragraph in paragraphs:
        safe = escape(paragraph or "")
        body.append(
            f"<w:p><w:r><w:t xml:space=\"preserve\">{safe}</w:t></w:r></w:p>"
        )
    document = (
        """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<w:document xmlns:wpc="http://schemas.microsoft.com/office/word/2010/wordprocessingCanvas"
 xmlns:mc="http://schemas.openxmlformats.org/markup-compatibility/2006"
 xmlns:o="urn:schemas-microsoft-com:office:office"
 xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"
 xmlns:m="http://schemas.openxmlformats.org/officeDocument/2006/math"
 xmlns:v="urn:schemas-microsoft-com:vml"
 xmlns:wp14="http://schemas.microsoft.com/office/word/2010/wordprocessingDrawing"
 xmlns:wp="http://schemas.openxmlformats.org/drawingml/2006/wordprocessingDrawing"
 xmlns:w10="urn:schemas-microsoft-com:office:word"
 xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main"
 xmlns:w14="http://schemas.microsoft.com/office/word/2010/wordml"
 xmlns:wpg="http://schemas.microsoft.com/office/word/2010/wordprocessingGroup"
 xmlns:wpi="http://schemas.microsoft.com/office/word/2010/wordprocessingInk"
 xmlns:wne="http://schemas.microsoft.com/office/word/2006/wordml"
 xmlns:wps="http://schemas.microsoft.com/office/word/2010/wordprocessingShape"
 mc:Ignorable="w14 wp14"><w:body>"""
        + "".join(body)
        + """<w:sectPr><w:pgSz w:w="12240" w:h="15840"/><w:pgMar w:top="1440" w:right="1440" w:bottom="1440" w:left="1440" w:header="708" w:footer="708" w:gutter="0"/></w:sectPr></w:body></w:document>"""
    )
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("[Content_Types].xml", content_types)
        zf.writestr("_rels/.rels", rels)
        zf.writestr("docProps/app.xml", app)
        zf.writestr("docProps/core.xml", core)
        zf.writestr("word/document.xml", document)


def build_guide_doc() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    sections: list[str] = []
    doc_lines: list[str] = []
    sections.append(
        """
        <h1>Scouting Grade Primary Concerns + All-22 Guide (2026)</h1>
        <p>Use this guide to replace the public generic concern text with player-specific notes. The goal is to tie concerns to role translation, measurable thresholds, and what actually shows up on tape.</p>
        <p><strong>How to use with the review CSV:</strong> start from the generated concern text, confirm or reject each concern on film, then rewrite the player concern section in plain football language. Keep it specific to the player, not the position template.</p>
        <h2>Writing Standard</h2>
        <ul>
          <li>Write concerns as player-specific translation risks, not generic warnings.</li>
          <li>Use evidence from size, traits, production, and All-22 study.</li>
          <li>Prioritize the 2-4 concerns that actually shape the player's NFL role or ceiling.</li>
          <li>If an issue is purely missing data, keep that internal and do not publish it as a player concern.</li>
        </ul>
        """
    )
    doc_lines.extend(
        [
            "Scouting Grade Primary Concerns + All-22 Guide (2026)",
            "",
            "Use this guide to replace the public generic concern text with player-specific notes. The goal is to tie concerns to role translation, measurable thresholds, and what actually shows up on tape.",
            "How to use with the review CSV: start from the generated concern text, confirm or reject each concern on film, then rewrite the player concern section in plain football language. Keep it specific to the player, not the position template.",
            "",
            "Writing Standard",
            "• Write concerns as player-specific translation risks, not generic warnings.",
            "• Use evidence from size, traits, production, and All-22 study.",
            "• Prioritize the 2-4 concerns that actually shape the player's NFL role or ceiling.",
            "• If an issue is purely missing data, keep that internal and do not publish it as a player concern.",
            "",
        ]
    )
    for position in ["QB", "RB", "WR", "TE", "OT", "IOL", "EDGE", "DT", "LB", "CB", "S"]:
        guide = POSITION_GUIDE[position]
        focus = ALL22_FOCUS[guide["family"]]
        doc_lines.extend(
            [
                position,
                "Primary concern prompts to review",
                *[f"• {item}" for item in guide["concerns"]],
                "Stat or profile flags that should trigger extra tape study",
                *[f"• {item}" for item in guide["triggers"]],
                "All-22 watch list",
                *[f"{idx}. {item}" for idx, item in enumerate(focus, start=1)],
                "",
            ]
        )
        sections.append(
            f"""
            <h2>{html.escape(position)}</h2>
            <h3>Primary concern prompts to review</h3>
            <ul>{_html_list(guide["concerns"])}</ul>
            <h3>Stat or profile flags that should trigger extra tape study</h3>
            <ul>{_html_list(guide["triggers"])}</ul>
            <h3>All-22 watch list</h3>
            <ol>{"".join(f"<li>{html.escape(item)}</li>" for item in focus)}</ol>
            """
        )
    GUIDE_HTML.write_text(
        "<html><head><meta charset='utf-8'></head><body style='font-family:Helvetica,Arial,sans-serif;line-height:1.4;'>"
        + "".join(sections)
        + "</body></html>",
        encoding="utf-8",
    )
    _write_simple_docx(GUIDE_DOCX, doc_lines)


def main() -> None:
    build_review_csv()
    build_guide_doc()
    print(f"Wrote {REVIEW_CSV}")
    print(f"Wrote {GUIDE_DOCX}")


if __name__ == "__main__":
    main()
