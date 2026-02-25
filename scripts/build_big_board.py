#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.ingest.combine_loader import load_combine_results
from src.ingest.espn_loader import load_espn_player_signals
from src.ingest.film_traits_loader import load_film_trait_rows
from src.ingest.playerprofiler_loader import load_playerprofiler_signals
from src.ingest.mockdraftable_loader import load_mockdraftable_baselines
from src.ingest.rankings_loader import (
    analyst_aggregate_score,
    canonical_player_name,
    load_analyst_rows,
    load_external_big_board,
    load_external_big_board_rows,
    normalize_pos,
)
from src.modeling.comp_model import assign_comp
from src.modeling.grading import grade_player, scouting_note
from src.modeling.mockdraftable_features import compute_mockdraftable_composite
from src.modeling.ras import historical_ras_comparison, ras_percentile, ras_tier
from src.modeling.team_fit import best_team_fit
from src.schemas import parse_height_to_inches

PROCESSED = ROOT / "data" / "processed"
OUTPUTS = ROOT / "data" / "outputs"

POSITION_DEFAULT_FRAME = {
    "QB": (76, 220),
    "RB": (71, 210),
    "WR": (73, 200),
    "TE": (77, 250),
    "OT": (78, 315),
    "IOL": (75, 310),
    "EDGE": (76, 260),
    "DT": (75, 305),
    "LB": (74, 235),
    "CB": (71, 190),
    "S": (72, 205),
}

ALLOWED_POSITIONS = set(POSITION_DEFAULT_FRAME.keys())



def _height_str(inches: int) -> str:
    feet = inches // 12
    rem = inches % 12
    return f"{feet}'{rem}\""



def read_seed(path: Path) -> list[dict]:
    with path.open() as f:
        rows = list(csv.DictReader(f))
    for row in rows:
        row["rank_seed"] = int(row["rank_seed"])
        row["weight_lb"] = int(row["weight_lb"])
        row["seed_row_id"] = int(row["seed_row_id"])
    return rows



def dedupe_seed_rows(rows: list[dict]) -> list[dict]:
    """Remove duplicate prospects by canonical name + normalized position (keep best seed rank)."""
    best = {}
    for row in rows:
        pos = normalize_pos(row["pos_raw"])
        if pos not in ALLOWED_POSITIONS:
            continue
        key = (canonical_player_name(row["player_name"]), pos)
        current = best.get(key)
        if current is None or row["rank_seed"] < current["rank_seed"]:
            best[key] = row

    deduped = list(best.values())
    deduped.sort(key=lambda r: r["rank_seed"])
    return deduped



def augment_seed_with_external_and_analyst(
    seed_rows: list[dict],
    external_rows: list[dict],
    analyst_rows: list[dict],
) -> tuple[list[dict], int, int]:
    merged = list(seed_rows)
    existing = {(canonical_player_name(r["player_name"]), normalize_pos(r["pos_raw"])) for r in merged}
    next_id = max((r["seed_row_id"] for r in merged), default=0) + 1

    added_external = 0
    added_analyst = 0

    for ext in external_rows:
        pos = normalize_pos(ext.get("external_pos", ""))
        if pos not in ALLOWED_POSITIONS:
            continue

        player_name = (ext.get("player_name") or "").strip()
        key = (canonical_player_name(player_name), pos)
        if not player_name or key in existing:
            continue

        ext_rank = int(ext.get("external_rank", 300) or 300)
        pseudo_seed_rank = min(300, max(12, int(round(ext_rank * 0.92 + 12))))
        h, w = POSITION_DEFAULT_FRAME[pos]

        merged.append(
            {
                "rank_seed": pseudo_seed_rank,
                "player_name": player_name,
                "school": ext.get("external_school") or "Unknown",
                "pos_raw": pos,
                "height": _height_str(h),
                "weight_lb": int(w),
                "class_year": "SR",
                "source_primary": "External_Board_Import",
                "seed_row_id": next_id,
            }
        )
        next_id += 1
        existing.add(key)
        added_external += 1

    # Add analyst-only players not present in seed or external board.
    for row in sorted(analyst_rows, key=lambda r: int(r["source_rank"])):
        pos = normalize_pos(row.get("position", ""))
        if pos not in ALLOWED_POSITIONS:
            continue

        player_name = (row.get("player_name") or "").strip()
        key = (canonical_player_name(player_name), pos)
        if not player_name or key in existing:
            continue

        source_rank = int(row.get("source_rank", 200) or 200)
        pseudo_seed_rank = min(300, max(24, int(round(source_rank * 1.25 + 30))))
        h, w = POSITION_DEFAULT_FRAME[pos]

        merged.append(
            {
                "rank_seed": pseudo_seed_rank,
                "player_name": player_name,
                "school": row.get("school") or "Unknown",
                "pos_raw": pos,
                "height": _height_str(h),
                "weight_lb": int(w),
                "class_year": "SR",
                "source_primary": f"Analyst_Import_{row.get('source','Unknown')}",
                "seed_row_id": next_id,
            }
        )
        next_id += 1
        existing.add(key)
        added_analyst += 1

    return merged, added_external, added_analyst



def write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        return
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)



def write_top_board_md(path: Path, rows: list[dict], limit: int = 100) -> None:
    lines = [
        "# 2026 Big Board (Top 100)",
        "",
        "| Rank | Player | Pos | School | Grade | Round | Best Team Fit | PFF | RAS |",
        "|---:|---|---|---|---:|---|---|---:|---:|",
    ]
    for row in rows[:limit]:
        pff = row.get("pff_grade") or ""
        ras = row.get("ras_estimate") or ""
        lines.append(
            f"| {row['consensus_rank']} | {row['player_name']} | {row['position']} | {row['school']} | {row['final_grade']} | {row['round_value']} | {row['best_team_fit']} | {pff} | {ras} |"
        )
    path.write_text("\n".join(lines))



def _scale_waa(pff_waa: float | None) -> float:
    if pff_waa is None:
        return 55.0
    bounded = max(-0.5, min(2.0, pff_waa))
    return 50.0 + bounded * 22.0



def _official_ras_fields(position: str, combine: dict) -> tuple[dict, dict]:
    official = combine.get("ras_official")
    if official is None:
        return (
            {
                "ras_estimate": "",
                "ras_tier": "",
                "ras_percentile": "",
                "ras_source": "pending_combine",
            },
            {
                "ras_historical_comp_1": "",
                "ras_historical_comp_2": "",
                "ras_comparison_note": "Pending official combine RAS",
            },
        )

    score = round(max(0.0, min(10.0, float(official))), 2)
    tier = ras_tier(score)
    ras = {
        "ras_estimate": score,
        "ras_tier": tier,
        "ras_percentile": ras_percentile(score),
        "ras_source": "combine_official",
    }
    return ras, historical_ras_comparison(position, tier)



def main() -> None:
    raw_seed = read_seed(PROCESSED / "prospect_seed_2026.csv")
    analyst_rows = load_analyst_rows()
    external_board_rows = load_external_big_board_rows()

    expanded_seed, added_external, added_analyst = augment_seed_with_external_and_analyst(
        seed_rows=raw_seed,
        external_rows=external_board_rows,
        analyst_rows=analyst_rows,
    )

    seed = dedupe_seed_rows(expanded_seed)

    analyst_scores = analyst_aggregate_score(analyst_rows)
    external_board = load_external_big_board()
    combine_results = load_combine_results()
    film_rows = load_film_trait_rows()
    espn_pack = load_espn_player_signals(target_year=2026)
    pp_pack = load_playerprofiler_signals()
    mockdraftable_baselines = load_mockdraftable_baselines()
    espn_by_name_pos = espn_pack.get("by_name_pos", {})
    espn_by_name = espn_pack.get("by_name", {})
    pp_by_name_pos = pp_pack.get("by_name_pos", {})
    pp_by_name = pp_pack.get("by_name", {})
    has_espn_signals = bool(espn_by_name_pos)
    has_pp_signals = bool(pp_by_name_pos)

    film_map = {}
    for frow in film_rows:
        film_pos = normalize_pos(frow["position"])
        key = (canonical_player_name(frow["player_name"]), film_pos)
        existing = film_map.get(key)
        if existing is None or frow.get("coverage_count", 0) > existing.get("coverage_count", 0):
            film_map[key] = frow

    enriched = []
    for row in seed:
        pos = normalize_pos(row["pos_raw"])
        key = canonical_player_name(row["player_name"])

        ext = external_board.get(key, {})
        combine = combine_results.get(key, {})
        film = film_map.get((key, pos), {})
        espn = espn_by_name_pos.get((key, pos), espn_by_name.get(key, {}))
        pp = pp_by_name_pos.get((key, pos), pp_by_name.get(key, {}))

        seed_height_in = parse_height_to_inches(row["height"]) or POSITION_DEFAULT_FRAME.get(pos, (74, 220))[0]
        seed_weight_lb = int(row["weight_lb"])
        effective_height_in = int(combine["height_in"]) if combine.get("height_in") is not None else seed_height_in
        effective_weight_lb = int(combine["weight_lb"]) if combine.get("weight_lb") is not None else seed_weight_lb

        grades = grade_player(
            position=pos,
            rank_seed=row["rank_seed"],
            class_year=row["class_year"],
            height_in=effective_height_in,
            weight_lb=effective_weight_lb,
            film_subtraits=film.get("traits", {}),
        )

        seed_signal = float(301 - row["rank_seed"])
        analyst_score = float(analyst_scores.get(key, 35.0))
        external_rank = ext.get("external_rank")
        external_rank_signal = float(max(1, 301 - external_rank)) if external_rank else 35.0

        pff_grade = ext.get("pff_grade")
        pff_grade_signal = float(pff_grade) if pff_grade is not None else 70.0
        pff_waa = ext.get("pff_waa")
        waa_signal = _scale_waa(pff_waa)

        espn_rank_signal = float(espn.get("espn_rank_signal", 35.0) or 35.0)
        espn_pos_signal = float(espn.get("espn_pos_signal", 35.0) or 35.0)
        espn_grade_signal = float(espn.get("espn_grade_signal", 70.0) or 70.0)
        espn_prod_signal = float(espn.get("espn_prod_signal", 55.0) or 55.0)
        espn_volatility_flag = bool(espn.get("espn_volatility_flag", False))

        pp_skill_signal = float(pp.get("pp_skill_signal", 55.0) or 55.0)
        pp_breakout_signal = float(pp.get("pp_breakout_signal", 55.0) or 55.0)
        pp_dominator_signal = float(pp.get("pp_dominator_signal", 55.0) or 55.0)
        pp_risk_flag = bool(pp.get("pp_risk_flag", 0))
        pp_player_available = bool(str(pp.get("pp_data_coverage", "")).strip())

        # Conservative blend so PP improves signal quality for skill players without overpowering scouting/consensus.
        if has_espn_signals and has_pp_signals and pp_player_available:
            consensus_score = (
                0.30 * seed_signal
                + 0.14 * analyst_score
                + 0.14 * external_rank_signal
                + 0.08 * pff_grade_signal
                + 0.04 * waa_signal
                + 0.10 * espn_rank_signal
                + 0.03 * espn_pos_signal
                + 0.04 * espn_grade_signal
                + 0.03 * espn_prod_signal
                + 0.08 * pp_skill_signal
                + 0.01 * pp_breakout_signal
                + 0.01 * pp_dominator_signal
            )
        elif has_espn_signals:
            consensus_score = (
                0.33 * seed_signal
                + 0.15 * analyst_score
                + 0.15 * external_rank_signal
                + 0.09 * pff_grade_signal
                + 0.04 * waa_signal
                + 0.12 * espn_rank_signal
                + 0.04 * espn_pos_signal
                + 0.05 * espn_grade_signal
                + 0.03 * espn_prod_signal
            )
        elif has_pp_signals and pp_player_available:
            consensus_score = (
                0.40 * seed_signal
                + 0.19 * analyst_score
                + 0.19 * external_rank_signal
                + 0.10 * pff_grade_signal
                + 0.05 * waa_signal
                + 0.06 * pp_skill_signal
                + 0.01 * pp_breakout_signal
            )
        else:
            consensus_score = (
                0.45 * seed_signal
                + 0.20 * analyst_score
                + 0.20 * external_rank_signal
                + 0.10 * pff_grade_signal
                + 0.05 * waa_signal
            )

        if espn_volatility_flag:
            consensus_score -= 2.0
        if pp_risk_flag:
            consensus_score -= 1.2

        fit_team, fit_score = best_team_fit(pos)
        comp = assign_comp(pos, row["rank_seed"])
        ras, ras_comps = _official_ras_fields(pos, combine)

        md_meas = {
            "height": effective_height_in,
            "weight": effective_weight_lb,
            "arm": combine.get("arm_in", ""),
            "hand": combine.get("hand_in", ""),
            "ten_split": combine.get("ten_split", ""),
            "forty": combine.get("forty", ""),
            "vertical": combine.get("vertical", ""),
            "broad": combine.get("broad", ""),
            "shuttle": combine.get("shuttle", ""),
            "three_cone": combine.get("three_cone", ""),
            "bench": combine.get("bench", ""),
        }
        md_features = compute_mockdraftable_composite(pos, md_meas, mockdraftable_baselines)

        report = {
            **row,
            "player_uid": f"{row['seed_row_id']}-{row['player_name'].lower().replace(' ', '-')}",
            "position": pos,
            "height_in": effective_height_in,
            "weight_lb_effective": effective_weight_lb,
            "seed_signal": round(seed_signal, 2),
            "analyst_signal": round(analyst_score, 2),
            "external_rank": external_rank if external_rank is not None else "",
            "external_rank_signal": round(external_rank_signal, 2),
            "pff_grade": round(pff_grade, 2) if pff_grade is not None else "",
            "pff_waa": round(pff_waa, 3) if pff_waa is not None else "",
            "pff_grade_locked": True,
            "consensus_score": round(consensus_score, 2),
            "espn_source_year": espn.get("espn_source_year", ""),
            "espn_ovr_rank": espn.get("espn_ovr_rank", ""),
            "espn_pos_rank": espn.get("espn_pos_rank", ""),
            "espn_grade": espn.get("espn_grade", ""),
            "espn_grade_z": espn.get("espn_grade_z", ""),
            "espn_rank_signal": round(espn_rank_signal, 2),
            "espn_pos_signal": round(espn_pos_signal, 2),
            "espn_grade_signal": round(espn_grade_signal, 2),
            "espn_prod_signal": round(espn_prod_signal, 2),
            "espn_qbr": espn.get("espn_qbr", ""),
            "espn_epa_per_play": espn.get("espn_epa_per_play", ""),
            "espn_trait_processing": espn.get("espn_trait_processing", ""),
            "espn_trait_separation": espn.get("espn_trait_separation", ""),
            "espn_trait_play_strength": espn.get("espn_trait_play_strength", ""),
            "espn_trait_motor": espn.get("espn_trait_motor", ""),
            "espn_trait_instincts": espn.get("espn_trait_instincts", ""),
            "espn_text_coverage": espn.get("espn_text_coverage", ""),
            "espn_volatility_flag": int(espn_volatility_flag),
            "espn_volatility_hits": espn.get("espn_volatility_hits", ""),
            "pp_source": pp.get("pp_source", ""),
            "pp_last_updated": pp.get("pp_last_updated", ""),
            "pp_breakout_age": pp.get("pp_breakout_age", ""),
            "pp_college_dominator": pp.get("pp_college_dominator", ""),
            "pp_breakout_signal": round(pp_breakout_signal, 2) if pp_player_available else "",
            "pp_dominator_signal": round(pp_dominator_signal, 2) if pp_player_available else "",
            "pp_skill_signal": round(pp_skill_signal, 2) if pp_player_available else "",
            "pp_data_coverage": pp.get("pp_data_coverage", "") if pp_player_available else "",
            "pp_early_declare": pp.get("pp_early_declare", "") if pp_player_available else "",
            "pp_risk_flag": int(pp_risk_flag) if pp_player_available else "",
            "pp_profile_tier": pp.get("pp_profile_tier", ""),
            "pp_notes": pp.get("pp_notes", ""),
            # combine fields
            "combine_source": combine.get("combine_source", ""),
            "combine_last_updated": combine.get("combine_last_updated", ""),
            "combine_height_in": combine.get("height_in", ""),
            "combine_weight_lb": combine.get("weight_lb", ""),
            "combine_arm_in": combine.get("arm_in", ""),
            "combine_hand_in": combine.get("hand_in", ""),
            "combine_forty": combine.get("forty", ""),
            "combine_ten_split": combine.get("ten_split", ""),
            "combine_vertical": combine.get("vertical", ""),
            "combine_broad": combine.get("broad", ""),
            "combine_shuttle": combine.get("shuttle", ""),
            "combine_three_cone": combine.get("three_cone", ""),
            "combine_bench": combine.get("bench", ""),
            "combine_ras_official": combine.get("ras_official", ""),
            **md_features,
            "film_traits_source": film.get("source", ""),
            "film_eval_date": film.get("eval_date", ""),
            **grades,
            "best_team_fit": fit_team,
            "best_team_fit_score": fit_score,
            **comp,
            **ras,
            **ras_comps,
            "scouting_notes": scouting_note(pos, grades["final_grade"], row["rank_seed"]),
            "headshot_url": "",
        }
        enriched.append(report)

    # Safety dedupe after enrichment.
    final_map = {}
    for row in enriched:
        key = (canonical_player_name(row["player_name"]), row["position"])
        existing = final_map.get(key)
        if existing is None or row["consensus_score"] > existing["consensus_score"]:
            final_map[key] = row

    final_rows = list(final_map.values())
    final_rows.sort(key=lambda x: x["consensus_score"], reverse=True)
    for i, row in enumerate(final_rows, start=1):
        row["consensus_rank"] = i

    write_csv(PROCESSED / "big_board_2026.csv", final_rows)
    write_csv(OUTPUTS / "big_board_2026.csv", final_rows)
    write_top_board_md(OUTPUTS / "big_board_2026_top100.md", final_rows, 100)

    with (OUTPUTS / "big_board_2026.json").open("w") as f:
        json.dump(final_rows, f, indent=2)

    print(f"Seed rows (raw): {len(raw_seed)}")
    print(f"Seed rows (expanded): {len(expanded_seed)}")
    print(f"Added from external board: {added_external}")
    print(f"Added from analyst feeds: {added_analyst}")
    print(f"Seed rows (deduped): {len(seed)}")
    print(f"Film charts loaded: {len(film_map)}")
    print(f"ESPN signals loaded: {len(espn_by_name_pos)}")
    print(f"PlayerProfiler signals loaded: {len(pp_by_name_pos)}")
    print(f"MockDraftable baselines loaded: {len(mockdraftable_baselines)}")
    print(f"Board rows: {len(final_rows)}")


if __name__ == "__main__":
    main()
