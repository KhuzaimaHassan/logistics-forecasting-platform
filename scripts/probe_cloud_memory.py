"""Offline Linux memory gate using test-trained Boosters, not cloud model proof."""

import json
import resource
from concurrent.futures import ThreadPoolExecutor

import lightgbm as lgb
import numpy as np
import pandas as pd

import src.agents.graph  # noqa: F401 - include lazy agent imports in peak
import src.serving.app  # noqa: F401 - include the actual API dependency graph
from src.serving.feature_extractor import ACTIVE_ZONE_CATEGORIES, DEMAND_FEATURE_COLS


def main():
    frame = pd.DataFrame({key: np.arange(263) % 7 for key in DEMAND_FEATURE_COLS})
    frame["zone_id"] = pd.Categorical(range(1, 264), categories=ACTIVE_ZONE_CATEGORIES)
    dataset = lgb.Dataset(frame, label=np.arange(263) % 11)
    model = lgb.train(
        {
            "objective": "regression",
            "num_threads": 1,
            "verbosity": -1,
            "num_leaves": 31,
        },
        dataset,
        num_boost_round=150,
    )
    with ThreadPoolExecutor(max_workers=8) as pool:
        for predictions in pool.map(
            lambda _: model.predict(frame, num_threads=1), range(8)
        ):
            assert len(predictions) == 263 and np.isfinite(predictions).all()
    peak_mib = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
    print(
        json.dumps(
            {
                "scope": "offline fixture-trained Booster and API/agent imports",
                "peak_rss_mib": round(peak_mib, 1),
                "budget_mib": 480,
                "actual_production_model_verified": False,
            }
        )
    )
    if peak_mib >= 480:
        raise SystemExit("Cloud runtime exceeds the 480 MiB headroom gate")


if __name__ == "__main__":
    main()
