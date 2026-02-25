from __future__ import annotations

from dataclasses import dataclass
from typing import List


@dataclass
class PackageUseCase:
    package: str
    category: str
    use_case: str


# Curated subset relevant to NFL/CFB modeling from the Sports Analytics task-view ecosystem.
SPORTS_ANALYTICS_PACKAGES: List[PackageUseCase] = [
    PackageUseCase("nflreadr", "nfl_data", "Load nflverse datasets in R"),
    PackageUseCase("cfbfastR", "cfb_data", "Load college football PBP and summaries"),
    PackageUseCase("nflplotR", "viz", "Visualize NFL play and player data"),
    PackageUseCase("tidymodels", "modeling", "Model training and evaluation workflows"),
    PackageUseCase("xgboost", "modeling", "Gradient boosting for prospect success models"),
    PackageUseCase("mgcv", "modeling", "GAM models for non-linear age/production curves"),
    PackageUseCase("lme4", "modeling", "Hierarchical models by conference/team"),
    PackageUseCase("survival", "modeling", "Career survival / second-contract modeling"),
    PackageUseCase("forecast", "time_series", "Trend and aging-curve analysis"),
    PackageUseCase("arrow", "storage", "Columnar storage for large PBP tables"),
    PackageUseCase("duckdb", "storage", "Fast local analytics for joins/feature building"),
]


def as_rows() -> List[dict]:
    return [pkg.__dict__ for pkg in SPORTS_ANALYTICS_PACKAGES]
