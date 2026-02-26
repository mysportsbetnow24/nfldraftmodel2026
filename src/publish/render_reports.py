from __future__ import annotations

import csv
import html
import json
import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
BOARD_PATH = ROOT / "data" / "processed" / "big_board_2026.csv"
OUT_DIR = ROOT / "data" / "outputs" / "player_reports_html"
INDEX_PATH = ROOT / "data" / "outputs" / "reports_index.html"
BLANK_TEMPLATE_PATH = ROOT / "data" / "outputs" / "scouting_card_template.html"
MAX_REPORTS = 300
TEMPLATE_MD_PATH = ROOT / "data" / "outputs" / "scouting_card_template.md"


FILM_TRAIT_LIBRARY: dict[str, list[tuple[str, float, str]]] = {
    "QB": [
        ("Processing / Timing", 0.22, "Speed to identify leverage and trigger on-schedule throws."),
        ("Accuracy / Ball Placement", 0.22, "Placement away from coverage and catchability by route level."),
        ("Pocket Management", 0.16, "Climb, reset, and evade while staying on platform."),
        ("Arm Talent", 0.16, "Drive and trajectory to all quadrants without overstraining."),
        ("Creation Value", 0.14, "Off-script play extension that still protects the football."),
        ("Decision Control", 0.10, "Turnover avoidance and risk calibration by game state."),
    ],
    "REC": [
        ("Release Package", 0.18, "Beat press with plan, tempo, and hand usage."),
        ("Separation Craft", 0.22, "Stem manipulation and route detail that creates windows."),
        ("Ball Skills", 0.18, "Hands, tracking, body control, and finish at the catch point."),
        ("YAC Threat", 0.14, "Burst, tackle avoidance, and acceleration after the catch."),
        ("Play Strength", 0.14, "Contact balance through stems and contested situations."),
        ("Blocking / Effort", 0.14, "Run-game strain and perimeter finish consistency."),
    ],
    "RB": [
        ("Vision / Anticipation", 0.22, "See lane development and adjust track efficiently."),
        ("Burst / Long Speed", 0.18, "Hit vertical daylight and finish explosive runs."),
        ("Contact Balance", 0.20, "Run through glancing blows and survive direct contact."),
        ("Lateral Agility", 0.12, "Jump cuts and redirection without losing pace."),
        ("Receiving Utility", 0.14, "Route comfort, hands reliability, and YAC from targets."),
        ("Pass Protection", 0.14, "Scan discipline, anchor, and willingness in pickup."),
    ],
    "OL": [
        ("Pass Set Footwork", 0.20, "Set points, balance, and mirror under speed-to-power."),
        ("Anchor / Core Strength", 0.20, "Absorb bull rush and re-anchor without panic."),
        ("Hand Usage", 0.16, "Independent hands, timing, and control through engagement."),
        ("Recovery Ability", 0.14, "Athletic repair when initially stressed."),
        ("Run Blocking Leverage", 0.16, "Pad level, displacement, and angle discipline."),
        ("Finish / Competitiveness", 0.14, "Sustain blocks and consistent strain."),
    ],
    "DL": [
        ("Get-Off", 0.18, "Snap anticipation and first-step urgency."),
        ("Bend / Cornering", 0.16, "Flexibility and angle efficiency into the pocket."),
        ("Hand Violence", 0.18, "Strike timing and shock to win contact points."),
        ("Counter Plan", 0.16, "Secondary moves after initial rush stalls."),
        ("Run Defense Discipline", 0.18, "Gap integrity and block recognition."),
        ("Finishing Ability", 0.14, "Close space and secure plays in backfield."),
    ],
    "LB": [
        ("Read / Trigger", 0.20, "Diagnose concepts and fire downhill with control."),
        ("Range / Pursuit", 0.18, "Sideline-to-sideline speed and angle discipline."),
        ("Block Deconstruction", 0.16, "Separate from climbing blockers with timing."),
        ("Tackling Reliability", 0.18, "Strike mechanics and wrap consistency."),
        ("Coverage Feel", 0.16, "Zone spacing and route anticipation."),
        ("Pressure Utility", 0.12, "Blitz timing and rush finish when deployed."),
    ],
    "DB": [
        ("Footwork / Transitions", 0.18, "Efficient breaks and route phase maintenance."),
        ("Mirror / Match Ability", 0.20, "Stay connected through stems without panic."),
        ("Ball Skills", 0.18, "Locate, track, and finish plays at the football."),
        ("Processing / Route ID", 0.16, "Diagnose concepts and communicate leverage."),
        ("Play Strength", 0.14, "Compete through contact and blocks."),
        ("Tackling / Run Support", 0.14, "Trigger and finish with control in space."),
    ],
    "DEFAULT": [
        ("Functional Athleticism", 0.20, "How movement traits show up in football actions."),
        ("Technical Execution", 0.20, "Repeatable details and assignment precision."),
        ("Processing", 0.18, "Recognition speed and decision quality."),
        ("Play Strength", 0.16, "Contact control and force transfer."),
        ("Competitiveness", 0.14, "Motor and response in adverse moments."),
        ("Role Projection", 0.12, "Clarity of path to early NFL usage."),
    ],
}


ALL22_FOCUS: dict[str, list[str]] = {
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
    "DEFAULT": [
        "First-step urgency",
        "Assignment recognition",
        "Technique consistency",
        "Play strength through contact",
        "Effort and chase motor",
        "Decision speed under stress",
        "Red-zone execution",
        "Third-down impact",
        "Adjustment to in-game counters",
        "Late-game stamina",
    ],
}


def _to_float(row: dict, key: str, default: float = 0.0) -> float:
    try:
        return float(row.get(key, default) or default)
    except (TypeError, ValueError):
        return default


def _clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


def _card_grade(score_100: float, src_lo: float = 68.0, src_hi: float = 95.0) -> float:
    """Maps 100-scale model scores to a scout-oriented 5.0-9.5 card scale."""
    if src_hi <= src_lo:
        return 7.0
    scaled = 5.0 + ((score_100 - src_lo) / (src_hi - src_lo)) * 4.5
    return round(_clamp(scaled, 5.0, 9.5), 1)


def _grade_band(card_grade: float) -> str:
    if card_grade >= 9.2:
        return "Blue-Chip / Rare"
    if card_grade >= 8.8:
        return "Pro Bowl Ceiling"
    if card_grade >= 8.3:
        return "High-End Starter"
    if card_grade >= 7.8:
        return "Quality Starter"
    if card_grade >= 7.3:
        return "NFL Contributor"
    if card_grade >= 6.8:
        return "Depth + Special Teams"
    if card_grade >= 6.3:
        return "Roster Competition"
    return "Priority Free Agent"


def _slugify(name: str) -> str:
    raw = (name or "").lower().strip()
    raw = raw.replace("'", "")
    raw = re.sub(r"[^a-z0-9\s-]", "", raw)
    raw = re.sub(r"\s+", "-", raw)
    raw = re.sub(r"-+", "-", raw)
    return raw or "player"


def _first_non_empty(*values: str) -> str:
    for v in values:
        if str(v or "").strip():
            return str(v)
    return ""


def _position_family(position: str) -> str:
    pos = str(position or "").upper().strip()
    if pos == "QB":
        return "QB"
    if pos in {"WR", "TE", "HB", "FB", "REC"}:
        return "REC"
    if pos in {"RB"}:
        return "RB"
    if pos in {"OT", "OG", "C", "IOL", "OL"}:
        return "OL"
    if pos in {"EDGE", "DE", "DT", "DL", "NT"}:
        return "DL"
    if pos in {"LB", "ILB", "OLB"}:
        return "LB"
    if pos in {"CB", "S", "DB", "NB"}:
        return "DB"
    return "DEFAULT"


def _all22_focus(position: str) -> str:
    family = _position_family(position)
    bullets = ALL22_FOCUS.get(family, ALL22_FOCUS["DEFAULT"])
    numbered = [f"{idx}. {item}" for idx, item in enumerate(bullets, start=1)]
    return "\n".join(numbered)


def _safe_float_str(row: dict, key: str, digits: int = 2) -> str:
    raw = row.get(key, "")
    if raw in (None, ""):
        return "Pending"
    try:
        return f"{float(raw):.{digits}f}"
    except (TypeError, ValueError):
        return str(raw)


def _film_subtraits(row: dict) -> tuple[list[tuple[str, float, float, str]], float]:
    family = _position_family(row.get("position", ""))
    template = FILM_TRAIT_LIBRARY.get(family, FILM_TRAIT_LIBRARY["DEFAULT"])

    trait_score = _to_float(row, "trait_score", 75.0)
    production_score = _to_float(row, "production_score", 75.0)
    context_score = _to_float(row, "context_score", 75.0)
    athletic_score = _to_float(row, "athletic_score", 75.0)

    base = _card_grade((trait_score * 0.5) + (production_score * 0.2) + (context_score * 0.2) + (athletic_score * 0.1))
    offsets = [0.15, 0.05, -0.05, 0.1, -0.1, -0.05]

    rows: list[tuple[str, float, float, str]] = []
    weighted = 0.0
    for idx, (label, weight, note) in enumerate(template):
        default_score = round(_clamp(base + offsets[idx % len(offsets)], 5.0, 9.5), 1)
        field = f"film_trait_{idx + 1}_score"
        override = row.get(field, "")
        if str(override).strip():
            try:
                score = round(_clamp(float(override), 5.0, 9.5), 1)
            except (TypeError, ValueError):
                score = default_score
        else:
            score = default_score
        rows.append((label, weight, score, note))
        weighted += score * weight

    return rows, round(weighted, 2)


def _trait_rows(row: dict) -> list[tuple[str, float, str]]:
    final_grade = _to_float(row, "final_grade", 78.0)
    athletic_score = _to_float(row, "athletic_score", 75.0)
    size_score = _to_float(row, "size_score", 75.0)
    production_score = _to_float(row, "production_score", 75.0)
    context_score = _to_float(row, "context_score", 75.0)
    trait_score = _to_float(row, "trait_score", 75.0)
    risk_penalty = _to_float(row, "risk_penalty", 2.0)

    ras = _first_non_empty(row.get("ras_estimate", ""), "Pending")
    md_comp = _first_non_empty(row.get("md_composite", ""), "Pending")
    film_cov = _first_non_empty(row.get("film_trait_coverage", ""), "0")

    pp_breakout = _first_non_empty(row.get("pp_breakout_age", ""), "N/A")
    pp_dom = _first_non_empty(row.get("pp_college_dominator", ""), "N/A")
    pp_tier = _first_non_empty(row.get("pp_profile_tier", "N/A"), "N/A")

    volatility = "Yes" if str(row.get("espn_volatility_flag", "0")) == "1" else "No"
    risk_tag = "Elevated" if risk_penalty >= 2.4 else "Manageable"

    rows = [
        (
            "Personal/Behavior",
            _card_grade(82.0 - (6.0 if volatility == "Yes" else 0.0)),
            f"Volatility indicator: {volatility}. Verify interviews, accountability, and role acceptance.",
        ),
        (
            "Athletic Ability",
            _card_grade((athletic_score * 0.7) + (size_score * 0.3)),
            f"Athletic score {athletic_score:.1f}; RAS {ras}; MockDraftable composite {md_comp}.",
        ),
        (
            "Strength & Explosion",
            _card_grade((size_score * 0.55) + (athletic_score * 0.45)),
            f"Size score {size_score:.1f}. Track contact power and force transfer on tape.",
        ),
        (
            "Competes",
            _card_grade((trait_score * 0.65) + (context_score * 0.35)),
            f"Trait score {trait_score:.1f}; film coverage {film_cov}; competitive strain projects as translatable.",
        ),
        (
            "Production",
            _card_grade((production_score * 0.85) + (context_score * 0.15)),
            f"Production {production_score:.1f}; Breakout age {pp_breakout}; Dominator {pp_dom}; profile tier {pp_tier}.",
        ),
        (
            "Mental/Learning",
            _card_grade((trait_score * 0.45) + (context_score * 0.55)),
            f"Context score {context_score:.1f}. Processing and role communication need game-by-game verification.",
        ),
        (
            "Injury History",
            _card_grade(80.0 - (risk_penalty * 5.0)),
            f"Model risk flag: {risk_tag}. Integrate medical stack once combine/club data is available.",
        ),
    ]

    # Add final grade context to first row note for easy scout review.
    rows[0] = (
        rows[0][0],
        rows[0][1],
        rows[0][2] + f" Model final grade: {final_grade:.2f} ({row.get('round_value','')}).",
    )
    return rows


def _summary_sections(row: dict) -> list[tuple[str, str]]:
    name = row.get("player_name", "")
    pos = row.get("position", "")
    school = row.get("school", "")
    final_grade = _to_float(row, "final_grade", 78.0)
    floor = _to_float(row, "floor_grade", max(70.0, final_grade - 2.0))
    ceiling = _to_float(row, "ceiling_grade", min(95.0, final_grade + 2.0))
    rank = row.get("consensus_rank", "")
    best_role = row.get("best_role", "")
    scheme_fit = row.get("best_scheme_fit", "")
    team_fit = row.get("best_team_fit", "")
    comp = row.get("historical_comp", "")
    note = row.get("scouting_notes", "")
    kiper_rank = _first_non_empty(row.get("kiper_rank", ""), "N/A")
    kiper_prev_rank = _first_non_empty(row.get("kiper_prev_rank", ""), "")
    kiper_delta = _first_non_empty(row.get("kiper_rank_delta", ""), "")
    kiper_strength_tags = _first_non_empty(row.get("kiper_strength_tags", ""), "")
    kiper_concern_tags = _first_non_empty(row.get("kiper_concern_tags", ""), "")
    kiper_statline_2025 = _first_non_empty(row.get("kiper_statline_2025", ""), "")

    pp_sig = _first_non_empty(row.get("pp_skill_signal", ""), "N/A")
    ras = _first_non_empty(row.get("ras_estimate", "Pending"), "Pending")
    md = _first_non_empty(row.get("md_composite", "Pending"), "Pending")

    summary_intro = _first_non_empty(
        row.get("scouting_report_summary", ""),
        (
            f"{name} ({pos}, {school}) checks in as a consensus top-{rank} profile with a model grade of "
            f"{final_grade:.2f}. Tape and data align to a {row.get('round_value','')} projection if the current trend holds."
        ),
    )

    wins = _first_non_empty(
        row.get("scouting_why_he_wins", ""),
        (
            f"Primary translatable strengths: {note}. "
            f"{'Structured trait tags: ' + kiper_strength_tags + '. ' if kiper_strength_tags else ''}"
            f"The profile fits best in a {scheme_fit} environment, where {best_role.lower()} can be deployed without forcing role expansion too early."
        ),
    )

    concerns = _first_non_empty(
        row.get("scouting_primary_concerns", ""),
        (
            f"Key development stress points: tighten play-to-play consistency, verify processing under pressure, and resolve any "
            f"volatile outcomes flagged by contextual risk. Athletic markers currently read RAS {ras} and MockDraftable {md}, "
            f"while PlayerProfiler skill signal sits at {pp_sig}. "
            f"{'Structured concern tags: ' + kiper_concern_tags + '.' if kiper_concern_tags else ''}"
        ),
    )

    production_snapshot = _first_non_empty(
        row.get("scouting_production_snapshot", ""),
        (
            f"{kiper_statline_2025}."
            if kiper_statline_2025
            else "No structured 2025 Kiper production snapshot ingested yet; rely on model production tables + manual film notes."
        ),
    )

    board_movement = _first_non_empty(row.get("scouting_board_movement", ""), "")
    if not board_movement:
        kiper_delta_num = _to_float(row, "kiper_rank_delta", 0.0)
        if kiper_rank == "N/A":
            board_movement = "No Kiper board movement data ingested yet for this player."
        else:
            if kiper_delta and str(kiper_delta) not in {"0", "0.0"}:
                direction = "up" if kiper_delta_num > 0 else "down"
                delta_abs = str(abs(int(kiper_delta_num)))
                board_movement = (
                    f"Kiper board context: current rank {kiper_rank}"
                    f"{f' (prev {kiper_prev_rank})' if kiper_prev_rank else ''}, moved {direction} {delta_abs} spots."
                )
            else:
                board_movement = (
                    f"Kiper board context: current rank {kiper_rank}"
                    f"{f' (prev {kiper_prev_rank})' if kiper_prev_rank else ''}, stable movement."
                )

    role_scheme_projection = _first_non_empty(
        row.get("scouting_role_projection", ""),
        (
            f"NFL projection: {best_role}. Ideal early deployment comes with {team_fit} based on current roster/scheme assumptions. "
            f"Best scheme fit: {scheme_fit}. Historical style comp: {comp}."
        ),
    )

    analyst_snapshot = (
        f"Kiper {kiper_rank}; "
        f"TDN {row.get('tdn_rank','') or 'N/A'}; "
        f"Ringer {row.get('ringer_rank','') or 'N/A'}; "
        f"Bleacher {row.get('br_rank','') or 'N/A'}; "
        f"AtoZ {row.get('atoz_rank','') or 'N/A'}; "
        f"SI/FCS {row.get('si_rank','') or 'N/A'}."
    )

    value = (
        f"Floor/Ceiling framework: floor {floor:.2f}, ceiling {ceiling:.2f}. Current card band: "
        f"{_grade_band(_card_grade(final_grade))}."
    )

    return [
        ("Report", summary_intro),
        ("How He Wins", wins),
        ("Primary Concerns", concerns),
        ("2025 Production Snapshot", production_snapshot),
        ("Board Movement", board_movement),
        ("Analyst Source Snapshot", analyst_snapshot),
        ("Role / Scheme Projection", role_scheme_projection),
        ("Value Range", value),
    ]


def _scout_scale_text() -> str:
    return (
        "9.2-9.5 Rare/Blue-Chip | 8.8-9.1 Pro Bowl Ceiling | 8.3-8.7 High-End Starter | "
        "7.8-8.2 Quality Starter | 7.3-7.7 Contributor | 6.8-7.2 Depth/ST | 6.3-6.7 Roster Competition | <6.3 Priority FA"
    )


def _editable_cell(value: str, key: str, tag: str = "span", cls: str = "editable") -> str:
    safe = html.escape(str(value or ""))
    return f'<{tag} class="{cls}" contenteditable="true" data-edit-key="{html.escape(key)}">{safe}</{tag}>'


def _player_card(row: dict) -> str:
    uid = html.escape(row.get("player_uid", _slugify(row.get("player_name", ""))))
    name = html.escape(row.get("player_name", ""))
    school = html.escape(row.get("school", ""))
    pos = html.escape(row.get("position", ""))
    class_year = html.escape(row.get("class_year", ""))
    height = html.escape(str(row.get("height", "")))
    weight = html.escape(str(row.get("weight_lb_effective", row.get("weight_lb", ""))))
    rank = html.escape(str(row.get("consensus_rank", "")))
    age = _first_non_empty(row.get("age", ""), "Pending")
    dob = _first_non_empty(row.get("birth_date", ""), "Pending")
    jersey = _first_non_empty(row.get("jersey", ""), "")

    final_grade = _to_float(row, "final_grade", 78.0)
    card_grade = _card_grade(final_grade)
    grade_band = _grade_band(card_grade)

    team_fit = html.escape(row.get("best_team_fit", ""))
    scheme_fit = html.escape(row.get("best_scheme_fit", ""))
    role = html.escape(row.get("best_role", ""))

    headshot = html.escape(row.get("headshot_url", ""))
    if headshot:
        img_tag = f'<img src="{headshot}" alt="{name}" class="headshot" />'
    else:
        img_tag = '<div class="headshot placeholder">Headshot Placeholder</div>'

    trait_rows = _trait_rows(row)
    trait_html = []
    for idx, (label, score, note) in enumerate(trait_rows, start=1):
        label_safe = html.escape(label)
        trait_html.append(
            "<tr>"
            f"<td class='label'>{label_safe}:</td>"
            f"<td class='score'>{_editable_cell(f'{score:.1f}', f'trait.{idx}.score')}</td>"
            f"<td class='notes'>{_editable_cell(note, f'trait.{idx}.notes')}</td>"
            "</tr>"
        )

    film_rows, film_weighted = _film_subtraits(row)
    film_html = []
    for idx, (label, weight_pct, score, note) in enumerate(film_rows, start=1):
        film_html.append(
            "<tr class='film-row'>"
            f"<td class='label'>{html.escape(label)}:</td>"
            f"<td class='score film-weight'>{weight_pct * 100:.0f}%</td>"
            f"<td class='score'>{_editable_cell(f'{score:.1f}', f'film.{idx}.score')}</td>"
            f"<td class='notes' colspan='4'>{_editable_cell(note, f'film.{idx}.notes')}</td>"
            "</tr>"
        )

    arm_length = _safe_float_str(row, "arm_length_in", 1)
    hand_size = _safe_float_str(row, "hand_size_in", 1)
    wingspan = _safe_float_str(row, "wingspan_in", 1)

    forty = _safe_float_str(row, "forty_yard", 2)
    split_10 = _safe_float_str(row, "split_10", 2)
    vertical = _safe_float_str(row, "vertical_jump", 1)
    broad = _safe_float_str(row, "broad_jump", 1)
    short_shuttle = _safe_float_str(row, "shuttle_20", 2)
    three_cone = _safe_float_str(row, "three_cone", 2)
    bench = _safe_float_str(row, "bench_reps", 0)

    ras = _first_non_empty(row.get("ras_estimate", ""), "Pending")
    ras_starter_target = _first_non_empty(row.get("ras_benchmark_starter_target", ""), "N/A")
    ras_impact_target = _first_non_empty(row.get("ras_benchmark_impact_target", ""), "N/A")
    ras_target_line = f"Starter>={ras_starter_target}, Impact>={ras_impact_target}"
    md_composite = _first_non_empty(row.get("md_composite", ""), "Pending")
    pff_grade = _safe_float_str(row, "pff_grade", 1)
    pp_signal = _first_non_empty(row.get("pp_skill_signal", ""), "N/A")
    source_signal_line = (
        f"Kiper {_first_non_empty(row.get('kiper_rank', ''), 'N/A')} | "
        f"TDN {_first_non_empty(row.get('tdn_rank', ''), 'N/A')} | "
        f"Ringer {_first_non_empty(row.get('ringer_rank', ''), 'N/A')} | "
        f"Bleacher {_first_non_empty(row.get('br_rank', ''), 'N/A')} | "
        f"AtoZ {_first_non_empty(row.get('atoz_rank', ''), 'N/A')} | "
        f"SI/FCS {_first_non_empty(row.get('si_rank', ''), 'N/A')}"
    )

    all22_text = _all22_focus(row.get("position", ""))
    verification_note = _first_non_empty(
        row.get("verification_notes", ""),
        "Cross-check with combine medical, interview, and full-season All-22 cutups before lock.",
    )
    development_plan = _first_non_empty(
        row.get("development_plan", ""),
        "Year 1: role-specific package + special teams utility where applicable. Year 2: expand responsibility into full-time snaps.",
    )
    source_confidence = _first_non_empty(
        row.get("source_confidence", ""),
        "Medium confidence - model and available film align; pending combine + interview stack.",
    )

    summary_blocks = _summary_sections(row)
    summary_html = []
    for idx, (title, body) in enumerate(summary_blocks, start=1):
        summary_html.append(
            f"<h4>{html.escape(title)}:</h4>"
            f"{_editable_cell(body, f'summary.{idx}', tag='div', cls='editable block')}"
        )

    model_snapshot = {
        "player_uid": row.get("player_uid", ""),
        "player_name": row.get("player_name", ""),
        "position": row.get("position", ""),
        "school": row.get("school", ""),
        "consensus_rank": row.get("consensus_rank", ""),
        "final_grade": row.get("final_grade", ""),
        "round_value": row.get("round_value", ""),
        "ras_estimate": row.get("ras_estimate", ""),
        "pp_skill_signal": row.get("pp_skill_signal", ""),
        "kiper_rank": row.get("kiper_rank", ""),
        "kiper_rank_delta": row.get("kiper_rank_delta", ""),
        "tdn_rank": row.get("tdn_rank", ""),
        "ringer_rank": row.get("ringer_rank", ""),
        "br_rank": row.get("br_rank", ""),
        "atoz_rank": row.get("atoz_rank", ""),
        "si_rank": row.get("si_rank", ""),
        "md_composite": row.get("md_composite", ""),
        "film_weighted_grade": film_weighted,
        "source_confidence": source_confidence,
    }

    return f"""
<article class="scout-card" data-player-uid="{uid}">
  <header class="masthead">
    <div class="identity">
      {img_tag}
      <div>
        <h1>{name}</h1>
        <p class="stack">{pos} | {school} | Class {class_year} | Rank {rank}</p>
        <p class="stack">Model Grade {final_grade:.2f} | Card Grade {_editable_cell(f'{card_grade:.1f}', 'grade.card')} ({html.escape(grade_band)})</p>
      </div>
    </div>
    <div class="fit-box">
      <p><strong>Best Team Fit:</strong> {team_fit}</p>
      <p><strong>Best Scheme Fit:</strong> {scheme_fit}</p>
      <p><strong>Projected Role:</strong> {role}</p>
    </div>
  </header>

  <section class="sheet">
    <table class="info-table">
      <tr class="section"><th colspan="7">Player Information</th></tr>
      <tr>
        <td class="label">Name:</td><td>{_editable_cell(row.get('player_name',''), 'info.name')}</td>
        <td class="label">School:</td><td>{_editable_cell(row.get('school',''), 'info.school')}</td>
        <td class="label">Position:</td><td>{_editable_cell(row.get('position',''), 'info.position')}</td>
        <td class="label">Jersey #: {_editable_cell(jersey, 'info.jersey')}</td>
      </tr>
      <tr>
        <td class="label">Class / Age:</td>
        <td colspan="2">{_editable_cell(f"{class_year} / {age}", 'info.class_age')}</td>
        <td class="label">DOB:</td>
        <td colspan="3">{_editable_cell(dob, 'info.dob')}</td>
      </tr>
      <tr>
        <td class="label">Size / Length:</td>
        <td colspan="6">{_editable_cell(f"{height} | {weight} lbs | Arms {arm_length} in | Hands {hand_size} in | Wingspan {wingspan} in", 'info.size_length')}</td>
      </tr>
      <tr>
        <td class="label">Alignment/Scheme:</td>
        <td colspan="6">{_editable_cell(row.get('best_scheme_fit',''), 'info.alignment')}</td>
      </tr>
      <tr>
        <td class="label">Games Watched:</td>
        <td colspan="6">{_editable_cell('', 'info.games_watched')}</td>
      </tr>
      <tr>
        <td class="label">Major Factors:</td>
        <td class="label" style="text-align:center; width:60px;">#</td>
        <td colspan="5">{_editable_cell('Model, film, production, athletic profile, context.', 'info.major_factors')}</td>
      </tr>
      <tr>
        <td class="label">Grading Scale:</td>
        <td colspan="6">{html.escape(_scout_scale_text())}</td>
      </tr>
      <tr class="section"><th colspan="7">Athletic + Data Snapshot</th></tr>
      <tr>
        <td class="label">Testing Line:</td>
        <td colspan="6">{_editable_cell(f"40 {forty} | 10-split {split_10} | Vert {vertical} | Broad {broad} | Shuttle {short_shuttle} | 3-Cone {three_cone} | Bench {bench}", 'athletic.testing')}</td>
      </tr>
      <tr>
        <td class="label">Composite Signals:</td>
        <td colspan="6">{_editable_cell(f"RAS {ras} ({ras_target_line}) | MockDraftable {md_composite} | PFF Grade {pff_grade} | PlayerProfiler Signal {pp_signal}", 'athletic.composites')}</td>
      </tr>
      <tr>
        <td class="label">Scouting Sources:</td>
        <td colspan="6">{_editable_cell(source_signal_line, 'athletic.scouting_sources')}</td>
      </tr>
      {''.join(trait_html)}
      <tr class="section"><th colspan="7">Film Sub-Trait Matrix</th></tr>
      <tr class="subhead">
        <td class="label">Sub-Trait</td>
        <td class="score">Wt</td>
        <td class="score">Score</td>
        <td class="label" colspan="4">Translation Note</td>
      </tr>
      {''.join(film_html)}
      <tr>
        <td class="label">Weighted Film Grade:</td>
        <td colspan="6">{_editable_cell(f"{film_weighted:.2f}", 'film.weighted_grade')}</td>
      </tr>
      <tr class="section"><th colspan="7">All-22 Focus (Top 10)</th></tr>
      <tr>
        <td colspan="7" class="summary-cell compact">{_editable_cell(all22_text, 'all22.top10', tag='div', cls='editable block')}</td>
      </tr>
      <tr class="section"><th colspan="7">Risk + Verification</th></tr>
      <tr>
        <td class="label">Medical/Character:</td>
        <td colspan="6">{_editable_cell(verification_note, 'risk.verification')}</td>
      </tr>
      <tr>
        <td class="label">Development Plan:</td>
        <td colspan="6">{_editable_cell(development_plan, 'risk.development_plan')}</td>
      </tr>
      <tr>
        <td class="label">Source Confidence:</td>
        <td colspan="6">{_editable_cell(source_confidence, 'risk.source_confidence')}</td>
      </tr>
      <tr class="section"><th colspan="7">Player Summary</th></tr>
      <tr>
        <td colspan="7" class="summary-cell">
          {''.join(summary_html)}
        </td>
      </tr>
    </table>
  </section>

  <section class="controls">
    <button type="button" class="btn save">Save Local Edit</button>
    <button type="button" class="btn reset">Reset To Model</button>
    <button type="button" class="btn export">Export JSON</button>
    <button type="button" class="btn html">Export HTML</button>
  </section>

  <script type="application/json" class="model-snapshot">{html.escape(json.dumps(model_snapshot))}</script>
</article>
""".strip()


def _css() -> str:
    return """
@import url('https://fonts.googleapis.com/css2?family=Barlow+Condensed:wght@500;700&family=Merriweather:wght@300;400;700&display=swap');

:root {
  --ink: #161616;
  --steel: #5e6872;
  --line: #2a2a2a;
  --paper: #f9f7f2;
  --panel: #ffffff;
  --header: #b7b7b7;
  --accent: #0d3b4f;
}

* { box-sizing: border-box; }
body {
  margin: 0;
  padding: 2rem;
  color: var(--ink);
  background:
    radial-gradient(circle at 20% 10%, rgba(13,59,79,0.08), transparent 45%),
    radial-gradient(circle at 82% 12%, rgba(111,87,44,0.10), transparent 42%),
    var(--paper);
  font-family: 'Merriweather', serif;
}

.scout-card {
  max-width: 1040px;
  margin: 0 auto 2rem;
  border: 1.6px solid var(--line);
  background: var(--panel);
  box-shadow: 0 16px 42px rgba(0,0,0,0.12);
}

.masthead {
  display: grid;
  grid-template-columns: 1fr 320px;
  gap: 1rem;
  padding: 1rem 1.2rem;
  border-bottom: 1.2px solid var(--line);
  background: linear-gradient(90deg, #e7ecef, #f4f5f1);
}

.identity { display: flex; gap: 1rem; align-items: center; }
.headshot { width: 104px; height: 104px; object-fit: cover; border: 1px solid var(--line); }
.placeholder { display:flex; align-items:center; justify-content:center; font-size: 0.8rem; color: #555; background:#ececec; }
h1 {
  margin: 0;
  font-family: 'Barlow Condensed', sans-serif;
  font-size: 2rem;
  letter-spacing: 0.02em;
  text-transform: uppercase;
}
.stack { margin: 0.28rem 0; color: #2d2d2d; font-size: 0.94rem; }
.fit-box { border: 1px solid #a4adb6; padding: 0.7rem; font-size: 0.9rem; background: #fbfcfd; }
.fit-box p { margin: 0.35rem 0; }

.sheet { padding: 0.8rem 1rem 1rem; }
.info-table {
  width: 100%;
  border-collapse: collapse;
  table-layout: fixed;
  font-size: 0.95rem;
}
.info-table td, .info-table th { border: 1px solid var(--line); padding: 0.2rem 0.35rem; vertical-align: top; }
.info-table .section th {
  background: var(--header);
  font-family: 'Barlow Condensed', sans-serif;
  text-align: left;
  font-size: 1.15rem;
  letter-spacing: 0.02em;
  text-transform: uppercase;
}
.info-table .label {
  width: 170px;
  font-family: 'Barlow Condensed', sans-serif;
  font-size: 1.05rem;
}
.info-table .score { width: 70px; text-align: center; font-weight: 700; }
.info-table .notes { font-size: 0.9rem; line-height: 1.35; }
.info-table .subhead td {
  background: #eceff2;
  font-family: 'Barlow Condensed', sans-serif;
  text-transform: uppercase;
  letter-spacing: 0.02em;
}
.film-row .film-weight { color: #374754; font-weight: 700; }
.summary-cell { padding: 0.7rem; min-height: 280px; }
.summary-cell.compact { min-height: 0; padding: 0.45rem 0.7rem; }
.summary-cell h4 {
  margin: 0.5rem 0 0.2rem;
  font-family: 'Barlow Condensed', sans-serif;
  text-transform: uppercase;
  letter-spacing: 0.02em;
  font-size: 1rem;
}

.editable {
  outline: none;
  min-height: 1.05rem;
  display: inline-block;
  width: 100%;
}
.editable.block {
  display: block;
  min-height: 2.4rem;
  padding: 0.2rem 0.25rem;
  white-space: pre-wrap;
}
.editable:focus {
  background: #f7f1c6;
  box-shadow: inset 0 0 0 1px #b79b41;
}

.controls {
  display: flex;
  gap: 0.65rem;
  padding: 0.8rem 1rem 1rem;
}
.btn {
  border: 1px solid #253442;
  background: #173548;
  color: #fff;
  padding: 0.45rem 0.7rem;
  font-family: 'Barlow Condensed', sans-serif;
  text-transform: uppercase;
  letter-spacing: 0.03em;
  cursor: pointer;
}
.btn.reset { background: #5b4a2c; border-color: #5b4a2c; }
.btn.export { background: #2f5f38; border-color: #2f5f38; }
.btn.html { background: #374754; border-color: #374754; }

.index-card {
  max-width: 980px;
  margin: 0 auto;
  border: 1.5px solid var(--line);
  background: #fff;
  padding: 1rem;
}
.index-card h1 {
  font-size: 2.1rem;
  margin-bottom: 0.5rem;
}
#reportSearch {
  width: 100%;
  padding: 0.55rem;
  border: 1px solid #5c646c;
  margin: 0.5rem 0 0.8rem;
  font-family: 'Merriweather', serif;
}
.index-list { margin: 0; padding-left: 1.2rem; line-height: 1.65; }
.index-list a { color: #133f5a; text-decoration: none; }
.index-list a:hover { text-decoration: underline; }

@media (max-width: 860px) {
  body { padding: 0.7rem; }
  .masthead { grid-template-columns: 1fr; }
  .info-table .label { width: 120px; }
}
""".strip()


def _js() -> str:
    return """
(function () {
  const card = document.querySelector('.scout-card');
  if (!card) return;

  const uid = card.getAttribute('data-player-uid') || 'player';
  const storageKey = `scouting-card-edit:${uid}`;
  const editableEls = Array.from(card.querySelectorAll('[data-edit-key]'));

  function collectEdits() {
    const out = {};
    editableEls.forEach((el) => {
      out[el.dataset.editKey] = el.innerText.trim();
    });
    return out;
  }

  function applyEdits(payload) {
    editableEls.forEach((el) => {
      const key = el.dataset.editKey;
      if (Object.prototype.hasOwnProperty.call(payload, key)) {
        el.innerText = payload[key];
      }
    });
  }

  function saveLocal() {
    const payload = collectEdits();
    localStorage.setItem(storageKey, JSON.stringify(payload));
  }

  function resetLocal() {
    localStorage.removeItem(storageKey);
    window.location.reload();
  }

  function exportJSON() {
    const modelTag = card.querySelector('.model-snapshot');
    let modelSnapshot = {};
    if (modelTag) {
      try {
        modelSnapshot = JSON.parse(modelTag.textContent);
      } catch (err) {
        modelSnapshot = {};
      }
    }

    const payload = {
      exported_at: new Date().toISOString(),
      player_uid: uid,
      model_snapshot: modelSnapshot,
      scouting_card: collectEdits(),
    };

    const blob = new Blob([JSON.stringify(payload, null, 2)], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `${uid}-scouting-card.json`;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
  }

  function exportHTML() {
    const doc = '<!doctype html>\\n' + document.documentElement.outerHTML;
    const blob = new Blob([doc], { type: 'text/html;charset=utf-8' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `${uid}-scouting-card.html`;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
  }

  const existing = localStorage.getItem(storageKey);
  if (existing) {
    try {
      applyEdits(JSON.parse(existing));
    } catch (err) {
      localStorage.removeItem(storageKey);
    }
  }

  const btnSave = card.querySelector('.btn.save');
  const btnReset = card.querySelector('.btn.reset');
  const btnExport = card.querySelector('.btn.export');
  const btnHtml = card.querySelector('.btn.html');

  if (btnSave) btnSave.addEventListener('click', saveLocal);
  if (btnReset) btnReset.addEventListener('click', resetLocal);
  if (btnExport) btnExport.addEventListener('click', exportJSON);
  if (btnHtml) btnHtml.addEventListener('click', exportHTML);
})();
""".strip()


def _render_page(title: str, body: str, include_js: bool = True) -> str:
    scripts = f"<script>{_js()}</script>" if include_js else ""
    return f"""
<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <title>{html.escape(title)}</title>
  <style>{_css()}</style>
</head>
<body>
{body}
{scripts}
</body>
</html>
""".strip()


def _blank_template_card() -> str:
    fake_row = {
        "player_uid": "template",
        "player_name": "Prospect Name",
        "school": "School",
        "position": "POS",
        "class_year": "JR",
        "consensus_rank": "--",
        "final_grade": "84.00",
        "round_value": "Round 2-3",
        "best_team_fit": "Team",
        "best_scheme_fit": "Scheme",
        "best_role": "Role",
        "height": "6'2\"",
        "weight_lb_effective": "215",
        "scouting_notes": "Template note: replace with your own evaluation language.",
    }
    return _player_card(fake_row)


def _blank_template_markdown() -> str:
    return """
# Scouting Card Template (Editable)

## Player Information
- Name:
- School:
- Position:
- Jersey:
- Class / Age:
- DOB:
- Size / Length:
- Alignment / Scheme:
- Games Watched:

## Athletic + Data Snapshot
- Testing Line:
- Composite Signals (RAS, MockDraftable, PFF, PlayerProfiler):

## Core Trait Grades (5.0-9.5)
- Personal/Behavior:
- Athletic Ability:
- Strength & Explosion:
- Competes:
- Production:
- Mental/Learning:
- Injury History:

## Film Sub-Trait Matrix (Position-Specific)
- Sub-trait 1:
- Sub-trait 2:
- Sub-trait 3:
- Sub-trait 4:
- Sub-trait 5:
- Sub-trait 6:
- Weighted Film Grade:

## All-22 Focus (Top 10)
1.
2.
3.
4.
5.
6.
7.
8.
9.
10.

## Risk + Verification
- Medical/Character:
- Development Plan (Year 1 / Year 2):
- Source Confidence:

## Player Summary
- Report:
- How He Wins:
- Primary Concerns:
- 2025 Production Snapshot:
- Board Movement:
- Analyst Source Snapshot:
- Role / Scheme Projection:
- Value Range:
""".strip() + "\n"


def render_reports() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    with BOARD_PATH.open() as f:
        rows = list(csv.DictReader(f))

    rows = rows[:MAX_REPORTS]
    expected_files = {f"{_slugify(row.get('player_name', ''))}.html" for row in rows}
    for stale in OUT_DIR.glob("*.html"):
        if stale.name not in expected_files:
            stale.unlink()

    index_rows = []
    for row in rows:
        slug = _slugify(row.get("player_name", ""))
        out_path = OUT_DIR / f"{slug}.html"
        page = _render_page(
            title=f"{row.get('player_name','Player')} | 2026 Scouting Card",
            body=_player_card(row),
            include_js=True,
        )
        out_path.write_text(page)

        index_rows.append(
            (
                int(float(row.get("consensus_rank", 9999) or 9999)),
                f"player_reports_html/{out_path.name}",
                row.get("player_name", ""),
                row.get("position", ""),
                row.get("school", ""),
                row.get("round_value", ""),
                row.get("final_grade", ""),
            )
        )

    index_rows.sort(key=lambda x: x[0])
    list_items = []
    for rank, href, name, pos, school, rnd, grade in index_rows:
        list_items.append(
            f"<li data-search='{html.escape((name + ' ' + pos + ' ' + school).lower())}'>"
            f"<a href='{html.escape(href)}'>{rank}. {html.escape(name)} ({html.escape(pos)}) - {html.escape(school)} | "
            f"{html.escape(str(grade))} | {html.escape(rnd)}</a></li>"
        )

    index_body = f"""
<section class="index-card">
  <h1>2026 Scouting Cards</h1>
  <p>Editable, scout-style cards with model-backed grades and narrative sections.</p>
  <p><a href="scouting_card_template.html">Open Blank Scouting Card Template</a></p>
  <input id="reportSearch" placeholder="Search by player, school, or position" />
  <ul class="index-list" id="reportList">{''.join(list_items)}</ul>
</section>
<script>
const input = document.getElementById('reportSearch');
const list = document.getElementById('reportList');
if (input && list) {{
  input.addEventListener('input', () => {{
    const q = input.value.trim().toLowerCase();
    list.querySelectorAll('li').forEach((li) => {{
      const hit = (li.dataset.search || '').includes(q);
      li.style.display = hit ? '' : 'none';
    }});
  }});
}}
</script>
""".strip()

    INDEX_PATH.write_text(_render_page("2026 Scouting Cards", index_body, include_js=False))

    BLANK_TEMPLATE_PATH.write_text(
        _render_page(
            "Blank Scouting Card Template",
            _blank_template_card(),
            include_js=True,
        )
    )
    TEMPLATE_MD_PATH.write_text(_blank_template_markdown())


if __name__ == "__main__":
    render_reports()
