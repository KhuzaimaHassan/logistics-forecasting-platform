"""Feast registry management and programmatic apply helper."""

import argparse
import sys
from pathlib import Path
from typing import List, Optional, Union

from feast import Entity, FeatureStore, FeatureView

from src.features.config import (
    ensure_feast_schema,
    get_default_repo_path,
    get_feature_store,
)


def apply_feature_definitions(
    store: Optional[FeatureStore] = None,
    entities: Optional[List[Entity]] = None,
    views: Optional[List[FeatureView]] = None,
    repo_path: Optional[Union[str, Path]] = None,
    use_sqlite_fallback: bool = False,
) -> FeatureStore:
    """Programmatically register entities and feature views into the Feast SQL registry.

    Args:
        store: Optional pre-configured FeatureStore instance.
        entities: List of Feast Entity objects to apply. Defaults to platform entities if None.
        views: List of Feast FeatureView objects to apply. Defaults to platform views if None.
        repo_path: Optional repository path if store is not provided.
        use_sqlite_fallback: Fallback flag if instantiating default store.

    Returns:
        The updated FeatureStore instance.
    """
    from src.features.entities import get_all_entities
    from src.features.views import get_all_feature_views

    if store is None:
        if not use_sqlite_fallback:
            ensure_feast_schema()
        store = get_feature_store(
            repo_path=repo_path,
            use_sqlite_fallback=use_sqlite_fallback,
        )

    target_entities = entities if entities is not None else get_all_entities()
    target_views = views if views is not None else get_all_feature_views()

    objects_to_apply = []
    if target_entities:
        objects_to_apply.extend(target_entities)
    if target_views:
        objects_to_apply.extend(target_views)

    if objects_to_apply:
        store.apply(objects_to_apply)

    return store


def main() -> None:
    """CLI entry point to apply feature store definitions or verify registry."""
    parser = argparse.ArgumentParser(
        description="Feast feature registry CLI for logistics forecasting platform."
    )
    parser.add_argument(
        "action",
        choices=["apply", "status", "check"],
        help="Action: apply (apply repo definitions), status (print registry info), check (verify connection).",
    )
    parser.add_argument(
        "--repo-path",
        type=Path,
        default=get_default_repo_path(),
        help="Path to the Feast repository directory (default: src/features).",
    )
    parser.add_argument(
        "--fallback",
        action="store_true",
        help="Use SQLite fallback for local testing without PostgreSQL.",
    )

    args = parser.parse_args()

    try:
        if not args.fallback:
            ensure_feast_schema()
        store = get_feature_store(
            repo_path=args.repo_path,
            use_sqlite_fallback=args.fallback,
        )

        if args.action == "check":
            print(f"Feast registry connection successful. Project: '{store.project}'")
            print(f"Registry Type: {store.config.registry.registry_type}")
            print(f"Offline Store: {store.config.offline_store.type}")
            print(f"Online Store: {store.config.online_store.type}")
        elif args.action == "status":
            entities = store.list_entities()
            views = store.list_feature_views()
            print(f"Project: {store.project}")
            print(
                f"Registered Entities ({len(entities)}): {[e.name for e in entities]}"
            )
            print(f"Registered Feature Views ({len(views)}): {[v.name for v in views]}")
        elif args.action == "apply":
            print(f"Applying definitions in {args.repo_path} to Feast registry...")
            apply_feature_definitions(store=store)
            entities = store.list_entities()
            views = store.list_feature_views()
            print(f"Registry updated for project '{store.project}'.")
            print(
                f"Registered Entities ({len(entities)}): {[e.name for e in entities]}"
            )
            print(f"Registered Feature Views ({len(views)}): {[v.name for v in views]}")
    except Exception as exc:
        print(f"Error executing feature registry CLI: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
