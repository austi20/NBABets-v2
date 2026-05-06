from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Engine

from app.config.settings import get_settings

LATEST_ACTIVITY_QUERY = """
SELECT MAX(activity_ts) AS latest_activity
FROM (
    SELECT MAX(fetched_at) AS activity_ts FROM raw_payloads
    UNION ALL
    SELECT MAX(predicted_at) AS activity_ts FROM predictions
    UNION ALL
    SELECT MAX(completed_at) AS activity_ts FROM model_runs
    UNION ALL
    SELECT MAX(computed_at) AS activity_ts FROM backtest_results
)
"""


@dataclass(frozen=True)
class DatabaseCandidate:
    url: str
    source: str
    exists: bool
    accessible: bool
    has_schema: bool
    has_activity: bool
    freshness: datetime | None


@dataclass(frozen=True)
class DatabaseSelection:
    url: str
    source: str
    freshness: datetime | None
    message: str


def select_best_database(configured_url: str | None) -> DatabaseSelection:
    candidates = [_assess_candidate(candidate) for candidate in discover_database_candidates(configured_url)]
    if configured_url:
        configured_candidate = next((candidate for candidate in candidates if candidate.url == configured_url), None)
        if configured_candidate is not None:
            if configured_candidate.accessible and configured_candidate.has_schema:
                freshness_label = configured_candidate.freshness.isoformat() if configured_candidate.freshness else "unknown freshness"
                return DatabaseSelection(
                    url=configured_candidate.url,
                    source=configured_candidate.source,
                    freshness=configured_candidate.freshness,
                    message=f"Using configured database with freshness {freshness_label}.",
                )
            return DatabaseSelection(
                url=configured_candidate.url,
                source=configured_candidate.source,
                freshness=configured_candidate.freshness,
                message="Using configured database; schema will be initialized if needed.",
            )
    accessible_candidates = [candidate for candidate in candidates if candidate.accessible and candidate.has_schema]
    if accessible_candidates:
        best = max(
            accessible_candidates,
            key=lambda candidate: (
                candidate.has_activity,
                candidate.freshness or datetime(1970, 1, 1, tzinfo=UTC),
                candidate.exists,
            ),
        )
        freshness_label = best.freshness.isoformat() if best.freshness else "unknown freshness"
        return DatabaseSelection(
            url=best.url,
            source=best.source,
            freshness=best.freshness,
            message=f"Using {best.source} database with freshness {freshness_label}.",
        )

    settings = get_settings()
    fallback_path = _sqlite_path_from_url(settings.database_url)
    fallback_path.parent.mkdir(parents=True, exist_ok=True)
    fallback_url = _sqlite_url(fallback_path)
    return DatabaseSelection(
        url=fallback_url,
        source="local sqlite fallback",
        freshness=None,
        message=f"No populated database found. Falling back to {fallback_path}.",
    )


def discover_database_candidates(configured_url: str | None) -> list[DatabaseCandidate]:
    seen_urls: set[str] = set()
    candidates: list[DatabaseCandidate] = []

    def add_candidate(url: str, source: str, exists: bool) -> None:
        if url in seen_urls:
            return
        seen_urls.add(url)
        candidates.append(
            DatabaseCandidate(
                url=url,
                source=source,
                exists=exists,
                accessible=False,
                has_schema=False,
                has_activity=False,
                freshness=None,
            )
        )

    if configured_url:
        add_candidate(configured_url, "configured", _url_exists(configured_url))

    for path in _sqlite_candidate_paths():
        add_candidate(_sqlite_url(path), f"local file {path}", _path_has_content(path))

    return candidates


def _assess_candidate(candidate: DatabaseCandidate) -> DatabaseCandidate:
    if candidate.url.startswith("sqlite") and not candidate.exists:
        return candidate
    try:
        engine = _build_probe_engine(candidate.url)
        with engine.connect() as connection:
            inspector = inspect(connection)
            if not inspector.has_table("raw_payloads"):
                return DatabaseCandidate(
                    url=candidate.url,
                    source=candidate.source,
                    exists=candidate.exists,
                    accessible=True,
                    has_schema=False,
                    has_activity=False,
                    freshness=_file_mtime(candidate.url),
                )
            latest_activity = connection.execute(text(LATEST_ACTIVITY_QUERY)).scalar_one_or_none()
        return DatabaseCandidate(
            url=candidate.url,
            source=candidate.source,
            exists=candidate.exists,
            accessible=True,
            has_schema=True,
            has_activity=_coerce_datetime(latest_activity) is not None,
            freshness=_coerce_datetime(latest_activity) or _file_mtime(candidate.url),
        )
    except Exception:
        return candidate


def _build_probe_engine(url: str) -> Engine:
    connect_args = {"check_same_thread": False} if url.startswith("sqlite") else {}
    return create_engine(url, future=True, pool_pre_ping=True, connect_args=connect_args)


def _sqlite_candidate_paths() -> list[Path]:
    settings = get_settings()
    root = settings.app_data_dir
    candidates = {
        _sqlite_path_from_url(settings.database_url),
        settings.provider_cache_db_path.with_name("nba_props.sqlite"),
        root / "data" / "processed" / "nba_props.sqlite",
        Path.cwd() / "data" / "processed" / "nba_props.sqlite",
        Path.cwd() / "nba_props.sqlite",
    }
    return sorted(candidates, key=lambda item: str(item))


def _sqlite_url(path: Path) -> str:
    return f"sqlite:///{path.as_posix()}"


def _url_exists(url: str) -> bool:
    if not url.startswith("sqlite"):
        return True
    path = _sqlite_path_from_url(url)
    return _path_has_content(path)


def _file_mtime(url: str) -> datetime | None:
    if not url.startswith("sqlite"):
        return None
    path = _sqlite_path_from_url(url)
    if not path.exists():
        return None
    return datetime.fromtimestamp(path.stat().st_mtime, tz=UTC)


def _sqlite_path_from_url(url: str) -> Path:
    return Path(url.replace("sqlite:///", "", 1))


def _path_has_content(path: Path) -> bool:
    return path.exists() and path.stat().st_size > 0


def _coerce_datetime(value: object) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    return None
