from __future__ import annotations

import csv
import math
from collections import defaultdict
from pathlib import Path

from src.ingest.rankings_loader import canonical_player_name


ROOT = Path(__file__).resolve().parents[2]
PROCESSED_PATH = ROOT / "data" / "processed" / "consensus_big_boards_2026.csv"


def load_consensus_board_signals(path: Path | None = None) -> dict[str, dict]:
    """
    Aggregate consensus-board rows into one player-level signal.
    Expected columns:
      - source
      - consensus_rank
      - player_name
      - school
      - position
    """
    path = path or PROCESSED_PATH
    if not path.exists():
        return {}

    by_player: dict[str, list[dict]] = defaultdict(list)
    with path.open() as f:
        for row in csv.DictReader(f):
            key = canonical_player_name(row.get("player_name", ""))
            if not key:
                continue
            try:
                rank = float(row.get("consensus_rank", ""))
            except (TypeError, ValueError):
                continue
            if rank <= 0:
                continue
            by_player[key].append(row | {"_rank": rank})

    out: dict[str, dict] = {}
    for key, rows in by_player.items():
        ranks = [r["_rank"] for r in rows]
        mean_rank = sum(ranks) / len(ranks)
        if len(ranks) > 1:
            mean_sq = sum((r - mean_rank) ** 2 for r in ranks) / len(ranks)
            rank_std = math.sqrt(mean_sq)
        else:
            rank_std = 0.0

        # Convert 1..300-ish rank into 1..100 signal.
        rank_signal_100 = max(1.0, min(100.0, (301.0 - mean_rank) / 3.0))
        # Stability bonus rewards agreement across sources.
        stability_bonus = max(0.0, 4.0 - rank_std)
        consensus_signal = max(1.0, min(100.0, rank_signal_100 + (0.35 * stability_bonus)))

        sources = sorted({str(r.get("source", "")).strip() for r in rows if str(r.get("source", "")).strip()})
        out[key] = {
            "consensus_mean_rank": round(mean_rank, 2),
            "consensus_rank_std": round(rank_std, 2),
            "consensus_source_count": len(sources),
            "consensus_sources": "|".join(sources),
            "consensus_signal": round(consensus_signal, 2),
        }

    return out
