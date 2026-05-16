from __future__ import annotations

import sys as _sys
from datetime import datetime
from functools import lru_cache
from os import getenv
from pathlib import Path

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


def _resolve_env_files() -> list[str]:
    """Return an ordered list of .env candidate paths.

    When running as a PyInstaller single-file exe (sys.frozen=True) the
    working directory may not be the project root, so pydantic-settings'
    default '.env' lookup fails.  We prepend the exe's own directory so
    a .env placed alongside the exe is always found first, regardless of
    how the shortcut's working directory is configured.
    """
    candidates: list[str] = []

    def _add_candidate(path: Path) -> None:
        candidate = str(path)
        if candidate not in candidates:
            candidates.append(candidate)

    def _add_parent_chain(start: Path) -> None:
        for directory in (start, *start.parents):
            _add_candidate(directory / ".env")

    if getattr(_sys, "frozen", False):
        # Exe directory (e.g. dist/) - highest priority when frozen
        exe_dir = Path(_sys.executable).parent
        _add_parent_chain(exe_dir)
        # PyInstaller extraction temp dir (_MEIPASS) - bundled .env fallback
        meipass = getattr(_sys, "_MEIPASS", None)
        if meipass:
            _add_parent_chain(Path(meipass))
    # Include cwd chain so sidecar processes launched from desktop_tauri/src-tauri
    # still discover a repo-root .env during local development.
    _add_parent_chain(Path.cwd())
    local_app_data = getenv("LOCALAPPDATA")
    if local_app_data:
        app_env = Path(local_app_data) / "NBAPropEngine" / ".env"
        if app_env.is_file():
            _add_candidate(app_env)
    # Keep plain cwd-relative fallback as final entry for compatibility.
    _add_candidate(Path(".env"))
    # Set by the Tauri host when spawning the sidecar: absolute repo .env (last wins in pydantic-settings).
    explicit = getenv("NBA_PROP_ENV_FILE")
    if explicit:
        exp_path = Path(explicit).expanduser()
        try:
            exp_resolved = exp_path.resolve()
        except OSError:
            exp_resolved = exp_path
        if exp_resolved.is_file():
            _add_candidate(exp_resolved)
    return candidates


def _runtime_root_dir() -> Path:
    explicit = getenv("NBA_PROP_ENV_FILE")
    if explicit:
        explicit_path = Path(explicit).expanduser()
        try:
            resolved = explicit_path.resolve()
        except OSError:
            resolved = explicit_path
        if resolved.is_file():
            return resolved.parent
    for directory in (Path.cwd(), *Path.cwd().parents):
        if (directory / "pyproject.toml").is_file():
            return directory
    return Path.cwd()


def _resolve_runtime_path(value: object) -> object:
    if value in (None, ""):
        return value
    path = Path(str(value)).expanduser()
    if path.is_absolute():
        return str(path)
    return str((_runtime_root_dir() / path).resolve())


def _default_app_data_dir() -> Path:
    explicit = getenv("NBA_PROP_APP_DATA_DIR")
    if explicit:
        return Path(explicit).expanduser()
    local_app_data = getenv("LOCALAPPDATA")
    if local_app_data:
        return Path(local_app_data) / "NBAPropEngine"
    return Path.home() / ".nba-prop-engine"


def _default_database_url() -> str:
    database_path = _default_app_data_dir() / "data" / "processed" / "nba_props.sqlite"
    return f"sqlite:///{database_path.as_posix()}"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=_resolve_env_files(),
        env_file_encoding="utf-8",
        extra="ignore",
    )

    app_env: str = Field(default="dev", alias="APP_ENV")
    database_url: str = Field(
        default_factory=_default_database_url,
        alias="DATABASE_URL",
    )
    app_data_dir: Path = Field(default_factory=_default_app_data_dir, alias="APP_DATA_DIR")
    logs_dir: Path = Field(default_factory=lambda: _default_app_data_dir() / "logs", alias="LOGS_DIR")
    duckdb_path: Path = Field(
        default_factory=lambda: _default_app_data_dir() / "data" / "processed" / "nba_props.duckdb",
        alias="DUCKDB_PATH",
    )
    provider_cache_db_path: Path = Field(
        default_factory=lambda: _default_app_data_dir() / "data" / "processed" / "provider_cache.sqlite",
        alias="PROVIDER_CACHE_DB_PATH",
    )
    raw_payload_dir: Path = Field(
        default_factory=lambda: _default_app_data_dir() / "data" / "raw",
        alias="RAW_PAYLOAD_DIR",
    )
    snapshot_dir: Path = Field(
        default_factory=lambda: _default_app_data_dir() / "data" / "snapshots",
        alias="SNAPSHOT_DIR",
    )
    reports_dir: Path = Field(
        default_factory=lambda: _default_app_data_dir() / "reports",
        alias="REPORTS_DIR",
    )
    historical_parquet_root: Path | None = Field(
        default=None,
        alias="HISTORICAL_PARQUET_ROOT",
        description="Optional path to parquet box score partitions for multi-season training history. "
        "If set and the directory exists, data is merged with the live SQLite training set.",
    )
    provider_rotation_state_path: Path = Field(
        default_factory=lambda: _default_app_data_dir() / "data" / "snapshots" / "provider_rotation.json",
        alias="PROVIDER_ROTATION_STATE_PATH",
    )
    startup_eta_history_path: Path = Field(
        default_factory=lambda: _default_app_data_dir() / "data" / "snapshots" / "startup_step_durations.json",
        alias="STARTUP_ETA_HISTORY_PATH",
    )
    local_agent_policy_state_path: Path = Field(
        default_factory=lambda: _default_app_data_dir() / "data" / "snapshots" / "local_agent_policy.json",
        alias="LOCAL_AGENT_POLICY_STATE_PATH",
    )
    brain_db_path: Path = Field(
        default_factory=lambda: _default_app_data_dir() / "data" / "brain" / "brain.sqlite",
        alias="BRAIN_DB_PATH",
    )
    brain_vault_root: Path = Field(
        default=Path("E:/AI Brain/ClaudeBrain"),
        alias="BRAIN_VAULT_ROOT",
    )
    brain_autonomy_level: str = Field(default="auto_detect", alias="BRAIN_AUTONOMY_LEVEL")
    feature_version: str = Field(default="v1", alias="FEATURE_VERSION")
    model_version: str = Field(default="v1", alias="MODEL_VERSION")
    training_seed: int = Field(default=42, alias="TRAINING_SEED")
    enable_provider_cache: bool = Field(default=True, alias="ENABLE_PROVIDER_CACHE")
    volatility_tier_enabled: bool = Field(default=True, alias="VOLATILITY_TIER_ENABLED")
    provider_cache_log_overlap_days: int = Field(default=2, alias="PROVIDER_CACHE_LOG_OVERLAP_DAYS")
    provider_cache_odds_ttl_minutes: int = Field(default=5, alias="PROVIDER_CACHE_ODDS_TTL_MINUTES")
    provider_cache_injuries_ttl_minutes: int = Field(default=10, alias="PROVIDER_CACHE_INJURIES_TTL_MINUTES")
    provider_cache_schedule_ttl_minutes: int = Field(default=15, alias="PROVIDER_CACHE_SCHEDULE_TTL_MINUTES")
    provider_cache_rosters_ttl_hours: int = Field(default=12, alias="PROVIDER_CACHE_ROSTERS_TTL_HOURS")
    provider_cache_allow_past_odds_reuse: bool = Field(
        default=True,
        alias="PROVIDER_CACHE_ALLOW_PAST_ODDS_REUSE",
    )
    kalshi_base_url: str = Field(
        default="https://external-api.kalshi.com/trade-api/v2",
        alias="KALSHI_BASE_URL",
    )
    kalshi_market_data_base_url: str = Field(
        default="https://external-api.kalshi.com/trade-api/v2",
        alias="KALSHI_MARKET_DATA_BASE_URL",
    )
    kalshi_ws_enabled: bool = False
    kalshi_ws_base_url: str = "wss://api.elections.kalshi.com/trade-api/ws/v2"
    kalshi_ws_max_backoff_seconds: int = 30
    kalshi_ws_ping_interval_seconds: int = 10
    kalshi_ws_max_consecutive_auth_failures: int = 5
    kalshi_live_trading: bool = Field(default=False, alias="KALSHI_LIVE_TRADING")
    auto_init_budget_from_wallet: bool = Field(
        default=True, alias="AUTO_INIT_BUDGET_FROM_WALLET"
    )
    brain_auto_resync_seconds: int = Field(
        default=300, alias="BRAIN_AUTO_RESYNC_SECONDS"
    )
    sportsbook_refresh_seconds: int = Field(
        default=600, alias="SPORTSBOOK_REFRESH_SECONDS"
    )
    trading_stream_max_hz: float = Field(
        default=1.0, alias="TRADING_STREAM_MAX_HZ"
    )
    trading_limits_path: str = Field(
        default="config/trading_limits.json",
        alias="TRADING_LIMITS_PATH",
    )
    kalshi_symbols_path: str = Field(
        default="config/kalshi_symbols.json",
        alias="KALSHI_SYMBOLS_PATH",
    )
    kalshi_resolution_targets_path: str = Field(
        default="config/kalshi_resolution_targets.json",
        alias="KALSHI_RESOLUTION_TARGETS_PATH",
    )
    kalshi_decisions_path: str = Field(
        default="data/decisions/decisions.json",
        alias="KALSHI_DECISIONS_PATH",
    )
    kalshi_decision_brain_enabled: bool = Field(default=True, alias="KALSHI_DECISION_BRAIN_ENABLED")
    kalshi_decision_brain_root: Path | None = Field(default=None, alias="KALSHI_DECISION_BRAIN_ROOT")
    kalshi_decision_brain_candidate_limit: int = Field(
        default=200,
        ge=1,
        alias="KALSHI_DECISION_BRAIN_CANDIDATE_LIMIT",
    )
    kalshi_decision_brain_auto_sync_on_startup: bool = Field(
        default=True,
        alias="KALSHI_DECISION_BRAIN_AUTO_SYNC_ON_STARTUP",
    )
    stats_provider: str = Field(default="balldontlie", alias="STATS_PROVIDER")
    odds_provider: str = Field(default="balldontlie", alias="ODDS_PROVIDER")
    injury_provider: str = Field(default="balldontlie", alias="INJURY_PROVIDER")
    balldontlie_api_key: str | None = Field(default="3485b14a-01ed-42b3-882a-fa6c0d3f2e0a", alias="BALLDONTLIE_API_KEY")
    # Ball Dont Lie tier limit is 600 req/min; default leaves headroom (~10%).
    balldontlie_max_rpm: int = Field(default=540, ge=0, alias="BALLDONTLIE_MAX_RPM")
    balldontlie_rate_window_seconds: float = Field(default=60.0, gt=0, alias="BALLDONTLIE_RATE_WINDOW_SECONDS")
    request_timeout_seconds: int = Field(default=30, alias="REQUEST_TIMEOUT_SECONDS")
    nba_api_enable_boxscore_enrichment: bool = Field(
        default=False,
        alias="NBA_API_ENABLE_BOXSCORE_ENRICHMENT",
    )
    nba_api_boxscore_timeout_seconds: int = Field(
        default=8,
        alias="NBA_API_BOXSCORE_TIMEOUT_SECONDS",
    )
    nba_api_boxscore_fetch_retries: int = Field(
        default=1,
        alias="NBA_API_BOXSCORE_FETCH_RETRIES",
    )
    nba_api_boxscore_fetch_concurrency: int = Field(
        default=4,
        alias="NBA_API_BOXSCORE_FETCH_CONCURRENCY",
    )
    nba_api_request_delay_seconds: float = Field(
        default=0.4,
        alias="NBA_API_REQUEST_DELAY_SECONDS",
    )
    nba_api_retry_attempts: int = Field(
        default=4,
        alias="NBA_API_RETRY_ATTEMPTS",
    )
    nba_api_retry_max_backoff_seconds: float = Field(
        default=8.0,
        alias="NBA_API_RETRY_MAX_BACKOFF_SECONDS",
    )
    default_lookback_games: int = Field(default=20, alias="DEFAULT_LOOKBACK_GAMES")
    startup_history_days: int = Field(default=150, alias="STARTUP_HISTORY_DAYS")
    runtime_memory_fraction_limit: float = Field(default=0.75, alias="RUNTIME_MEMORY_FRACTION_LIMIT")
    runtime_cpu_fraction_limit: float = Field(default=0.85, alias="RUNTIME_CPU_FRACTION_LIMIT")
    simulation_target_margin: float = Field(default=0.01, alias="SIMULATION_TARGET_MARGIN")
    simulation_min_samples: int = Field(default=10000, alias="SIMULATION_MIN_SAMPLES")
    simulation_max_samples: int = Field(default=100000, alias="SIMULATION_MAX_SAMPLES")
    simulation_batch_size: int = Field(default=10000, alias="SIMULATION_BATCH_SIZE")
    rotation_shock_enabled: bool = Field(default=True, alias="ROTATION_SHOCK_ENABLED")
    rotation_shock_shadow_mode: bool = Field(default=False, alias="ROTATION_SHOCK_SHADOW_MODE")
    rotation_shock_ablation_mode: str = Field(default="full", alias="ROTATION_SHOCK_ABLATION_MODE")
    legacy_pipeline_enabled: bool = Field(default=True, alias="LEGACY_PIPELINE_ENABLED")
    data_sufficiency_tier_a_min_games: int = Field(default=10, alias="DATA_SUFFICIENCY_TIER_A_MIN_GAMES")
    data_sufficiency_tier_a_min_minutes: float = Field(
        default=100.0,
        alias="DATA_SUFFICIENCY_TIER_A_MIN_MINUTES",
    )
    data_sufficiency_tier_b_min_games: int = Field(default=5, alias="DATA_SUFFICIENCY_TIER_B_MIN_GAMES")
    data_sufficiency_tier_b_min_minutes: float = Field(
        default=50.0,
        alias="DATA_SUFFICIENCY_TIER_B_MIN_MINUTES",
    )
    data_sufficiency_recent_avg_minutes_floor: float = Field(
        default=12.0,
        alias="DATA_SUFFICIENCY_RECENT_AVG_MINUTES_FLOOR",
    )
    # P1 CHANGE 3A: Stronger history gate to prevent garbage game data from polluting predictions
    minimum_prediction_history_games: int = Field(default=5, alias="MINIMUM_PREDICTION_HISTORY_GAMES")
    minimum_prediction_history_minutes: float = Field(
        default=50.0,
        alias="MINIMUM_PREDICTION_HISTORY_MINUTES",
    )
    calibration_purge_days: int = Field(default=7, alias="CALIBRATION_PURGE_DAYS")
    ai_local_endpoint: str = Field(
        default="http://127.0.0.1:8080/v1/chat/completions",
        alias="AI_LOCAL_ENDPOINT",
    )
    ai_local_model: str = Field(default="qwen3-1.7b-q8", alias="AI_LOCAL_MODEL")
    ai_local_api_key: str = Field(default="local-llamacpp", alias="AI_LOCAL_API_KEY")
    ai_local_server_binary: Path | None = Field(default=None, alias="AI_LOCAL_SERVER_BINARY")
    ai_local_model_path: Path | None = Field(default=None, alias="AI_LOCAL_MODEL_PATH")
    ai_local_server_wait_seconds: int = Field(default=45, alias="AI_LOCAL_SERVER_WAIT_SECONDS")
    ai_timeout_seconds: int = Field(default=45, alias="AI_TIMEOUT_SECONDS")
    local_autonomy_enabled: bool = Field(default=True, alias="LOCAL_AUTONOMY_ENABLED")
    local_autonomy_min_confidence: float = Field(default=0.70, alias="LOCAL_AUTONOMY_MIN_CONFIDENCE")
    local_autonomy_max_actions: int = Field(default=5, alias="LOCAL_AUTONOMY_MAX_ACTIONS")
    agent_mode: str = Field(default="off", alias="AGENT_MODE")
    agent_default_dry_run: bool = Field(default=True, alias="AGENT_DEFAULT_DRY_RUN")
    agent_budget_tokens_daily: int = Field(default=200000, alias="AGENT_BUDGET_TOKENS_DAILY")
    agent_timeout_seconds: int = Field(default=45, alias="AGENT_TIMEOUT_SECONDS")
    workflow_agent_enabled: bool = Field(default=True, alias="WORKFLOW_AGENT_ENABLED")
    api_monitor_agent_enabled: bool = Field(default=True, alias="API_MONITOR_AGENT_ENABLED")
    data_quality_agent_enabled: bool = Field(default=True, alias="DATA_QUALITY_AGENT_ENABLED")
    network_reliability_agent_enabled: bool = Field(default=True, alias="NETWORK_RELIABILITY_AGENT_ENABLED")
    workflow_agent_error_threshold: int = Field(default=5, alias="WORKFLOW_AGENT_ERROR_THRESHOLD")
    workflow_agent_allow_auto_actions: bool = Field(default=False, alias="WORKFLOW_AGENT_ALLOW_AUTO_ACTIONS")
    data_quality_raw_payload_retention_days: int = Field(default=21, alias="DATA_QUALITY_RAW_PAYLOAD_RETENTION_DAYS")
    trading_exchange: str = Field(default="paper", alias="TRADING_EXCHANGE")
    trading_paper_adapter: str = Field(default="realistic", alias="TRADING_PAPER_ADAPTER")
    trading_live_enabled: bool = Field(default=False, alias="TRADING_LIVE_ENABLED")
    kalshi_api_key_id: str | None = Field(default=None, alias="KALSHI_API_KEY_ID")
    kalshi_private_key_path: Path | None = Field(default=None, alias="KALSHI_PRIVATE_KEY_PATH")
    network_circuit_breaker_failures: int = Field(default=5, alias="NETWORK_CIRCUIT_BREAKER_FAILURES")
    network_circuit_breaker_open_seconds: int = Field(default=90, alias="NETWORK_CIRCUIT_BREAKER_OPEN_SECONDS")
    network_retry_attempts: int = Field(default=6, alias="NETWORK_RETRY_ATTEMPTS")
    prediction_validator_enabled: bool = Field(default=True, alias="PREDICTION_VALIDATOR_ENABLED")
    prediction_validator_extreme_prob_threshold: float = Field(
        default=0.95, alias="PREDICTION_VALIDATOR_EXTREME_PROB_THRESHOLD"
    )
    prediction_validator_batch_size: int = Field(default=10, alias="PREDICTION_VALIDATOR_BATCH_SIZE")
    parlay_advisor_enabled: bool = Field(default=True, alias="PARLAY_ADVISOR_ENABLED")
    parlay_advisor_correlation_threshold: float = Field(
        default=0.60, alias="PARLAY_ADVISOR_CORRELATION_THRESHOLD"
    )
    local_autonomy_overfit_block_threshold: float = Field(
        default=0.80, alias="LOCAL_AUTONOMY_OVERFIT_BLOCK_THRESHOLD"
    )
    examiner_enabled: bool = Field(default=False, alias="EXAMINER_ENABLED")
    examiner_csv_path: Path = Field(
        default=Path("data/processed/nba_props_examiner_graded.csv"),
        alias="EXAMINER_CSV_PATH",
    )
    examiner_top_k: int = Field(default=6, ge=1, le=50, alias="EXAMINER_TOP_K")
    examiner_min_confidence_for_retrain: float = Field(
        default=0.75, ge=0.0, le=1.0, alias="EXAMINER_MIN_CONFIDENCE_FOR_RETRAIN"
    )
    examiner_real_only_default: bool = Field(default=True, alias="EXAMINER_REAL_ONLY_DEFAULT")

    release_policy_override_enabled: bool = Field(
        default=False,
        alias="RELEASE_POLICY_OVERRIDE_ENABLED",
    )
    release_policy_override_until: datetime | None = Field(
        default=None,
        alias="RELEASE_POLICY_OVERRIDE_UNTIL",
    )
    release_policy_override_reason: str = Field(default="", alias="RELEASE_POLICY_OVERRIDE_REASON")

    @field_validator("release_policy_override_until", mode="before")
    @classmethod
    def _empty_release_override_until(cls, value: object) -> object:
        if value is None or value == "":
            return None
        return value

    @field_validator("kalshi_live_trading", mode="before")
    @classmethod
    def _kalshi_live_trading_coerce(cls, value: object) -> object:
        if isinstance(value, str):
            normalized = value.strip().strip('"').strip("'").lower()
            return normalized in {"1", "true", "yes", "on"}
        if isinstance(value, bool):
            return value
        if isinstance(value, int):
            return value != 0
        return value

    @field_validator(
        "trading_limits_path",
        "kalshi_symbols_path",
        "kalshi_resolution_targets_path",
        "kalshi_decisions_path",
        mode="before",
    )
    @classmethod
    def _relative_runtime_paths(cls, value: object) -> object:
        return _resolve_runtime_path(value)

    @field_validator(
        "ai_local_server_binary",
        "ai_local_model_path",
        "kalshi_private_key_path",
        "kalshi_decision_brain_root",
        mode="before",
    )
    @classmethod
    def _empty_local_ai_paths(cls, value: object) -> object:
        if value is None or value == "":
            return None
        return value


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    settings = Settings()
    settings.app_data_dir.mkdir(parents=True, exist_ok=True)
    settings.logs_dir.mkdir(parents=True, exist_ok=True)
    settings.raw_payload_dir.mkdir(parents=True, exist_ok=True)
    settings.snapshot_dir.mkdir(parents=True, exist_ok=True)
    settings.reports_dir.mkdir(parents=True, exist_ok=True)
    settings.duckdb_path.parent.mkdir(parents=True, exist_ok=True)
    settings.provider_cache_db_path.parent.mkdir(parents=True, exist_ok=True)
    settings.provider_rotation_state_path.parent.mkdir(parents=True, exist_ok=True)
    settings.startup_eta_history_path.parent.mkdir(parents=True, exist_ok=True)
    settings.local_agent_policy_state_path.parent.mkdir(parents=True, exist_ok=True)
    settings.brain_db_path.parent.mkdir(parents=True, exist_ok=True)
    return settings


def _merged_keys(primary_key: str | None, additional_keys_raw: str | None) -> list[str]:
    keys: list[str] = []
    if primary_key:
        keys.append(primary_key.strip())
    if additional_keys_raw:
        keys.extend(
            item.strip()
            for item in additional_keys_raw.split(",")
            if item.strip()
        )
    return list(dict.fromkeys(keys))
