"""Initial baseline schema for Alembic-managed databases."""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "20260526_01"
down_revision = None
branch_labels = None
depends_on = None


def _has_existing_schema() -> bool:
    inspector = sa.inspect(op.get_bind())
    existing = set(inspector.get_table_names())
    return bool(existing - {"alembic_version"})


def upgrade() -> None:
    if _has_existing_schema():
        op.execute("SELECT 1")
        return

    op.create_table(
        "seasons",
        sa.Column("season_id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("code", sa.String(16), nullable=False),
        sa.Column("start_date", sa.Date(), nullable=False),
        sa.Column("end_date", sa.Date(), nullable=False),
        sa.UniqueConstraint("code"),
    )
    op.create_index("ix_seasons_code", "seasons", ["code"])

    op.create_table(
        "teams",
        sa.Column("team_id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("provider_team_id", sa.String(64), nullable=True),
        sa.Column("abbreviation", sa.String(8), nullable=False),
        sa.Column("name", sa.String(128), nullable=False),
        sa.Column("city", sa.String(128), nullable=True),
        sa.Column("conference", sa.String(32), nullable=True),
        sa.Column("division", sa.String(32), nullable=True),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("1")),
        sa.UniqueConstraint("abbreviation"),
    )
    op.create_index("ix_teams_provider_team_id", "teams", ["provider_team_id"])
    op.create_index("ix_teams_abbreviation", "teams", ["abbreviation"])
    op.create_index("ix_teams_name", "teams", ["name"])

    op.create_table(
        "players",
        sa.Column("player_id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("provider_player_id", sa.String(64), nullable=True),
        sa.Column("full_name", sa.String(128), nullable=False),
        sa.Column("normalized_name", sa.String(128), nullable=False),
        sa.Column("team_id", sa.Integer(), sa.ForeignKey("teams.team_id"), nullable=True),
        sa.Column("position", sa.String(16), nullable=True),
        sa.Column("status", sa.String(32), nullable=True),
        sa.Column("meta", sa.JSON(), nullable=False, server_default=sa.text("'{}'")),
        sa.UniqueConstraint("full_name", "team_id", name="uq_players_name_team"),
    )
    op.create_index("ix_players_provider_player_id", "players", ["provider_player_id"])
    op.create_index("ix_players_full_name", "players", ["full_name"])
    op.create_index("ix_players_normalized_name", "players", ["normalized_name"])

    op.create_table(
        "sportsbooks",
        sa.Column("sportsbook_id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("key", sa.String(64), nullable=False),
        sa.Column("display_name", sa.String(128), nullable=False),
        sa.Column("region", sa.String(16), nullable=True),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("1")),
        sa.UniqueConstraint("key"),
    )
    op.create_index("ix_sportsbooks_key", "sportsbooks", ["key"])

    op.create_table(
        "prop_markets",
        sa.Column("market_id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("key", sa.String(64), nullable=False),
        sa.Column("stat_type", sa.String(64), nullable=False),
        sa.Column("display_name", sa.String(128), nullable=False),
        sa.Column("distribution_family", sa.String(32), nullable=False, server_default="negative_binomial"),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("1")),
        sa.UniqueConstraint("key"),
    )
    op.create_index("ix_prop_markets_key", "prop_markets", ["key"])
    op.create_index("ix_prop_markets_stat_type", "prop_markets", ["stat_type"])

    op.create_table(
        "games",
        sa.Column("game_id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("provider_game_id", sa.String(64), nullable=True),
        sa.Column("season_id", sa.Integer(), sa.ForeignKey("seasons.season_id"), nullable=True),
        sa.Column("game_date", sa.Date(), nullable=False),
        sa.Column("start_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("home_team_id", sa.Integer(), sa.ForeignKey("teams.team_id"), nullable=False),
        sa.Column("away_team_id", sa.Integer(), sa.ForeignKey("teams.team_id"), nullable=False),
        sa.Column("spread", sa.Float(), nullable=True),
        sa.Column("total", sa.Float(), nullable=True),
        sa.Column("status", sa.String(32), nullable=False, server_default="scheduled"),
        sa.Column("final_home_score", sa.Integer(), nullable=True),
        sa.Column("final_away_score", sa.Integer(), nullable=True),
        sa.Column("meta", sa.JSON(), nullable=False, server_default=sa.text("'{}'")),
        sa.UniqueConstraint("provider_game_id"),
    )
    op.create_index("ix_games_provider_game_id", "games", ["provider_game_id"])
    op.create_index("ix_games_game_date", "games", ["game_date"])
    op.create_index("ix_games_start_time", "games", ["start_time"])
    op.create_index("ix_games_home_team_id", "games", ["home_team_id"])
    op.create_index("ix_games_away_team_id", "games", ["away_team_id"])
    op.create_index("ix_games_status", "games", ["status"])

    op.create_table(
        "raw_payloads",
        sa.Column("payload_id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("provider_type", sa.String(32), nullable=False),
        sa.Column("provider_name", sa.String(64), nullable=False),
        sa.Column("endpoint", sa.String(256), nullable=False),
        sa.Column("fetched_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("content_hash", sa.String(64), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
    )
    op.create_index("ix_raw_payloads_provider_type", "raw_payloads", ["provider_type"])
    op.create_index("ix_raw_payloads_provider_name", "raw_payloads", ["provider_name"])
    op.create_index("ix_raw_payloads_fetched_at", "raw_payloads", ["fetched_at"])
    op.create_index("ix_raw_payloads_content_hash", "raw_payloads", ["content_hash"])

    op.create_table(
        "model_runs",
        sa.Column("model_run_id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("model_version", sa.String(64), nullable=False),
        sa.Column("feature_version", sa.String(64), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("training_window_start", sa.DateTime(timezone=True), nullable=True),
        sa.Column("training_window_end", sa.DateTime(timezone=True), nullable=True),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("metrics", sa.JSON(), nullable=False, server_default=sa.text("'{}'")),
    )
    op.create_index("ix_model_runs_model_version", "model_runs", ["model_version"])
    op.create_index("ix_model_runs_feature_version", "model_runs", ["feature_version"])
    op.create_index("ix_model_runs_started_at", "model_runs", ["started_at"])

    op.create_table(
        "line_snapshots",
        sa.Column("snapshot_id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("game_id", sa.Integer(), sa.ForeignKey("games.game_id"), nullable=False),
        sa.Column("sportsbook_id", sa.Integer(), sa.ForeignKey("sportsbooks.sportsbook_id"), nullable=False),
        sa.Column("player_id", sa.Integer(), sa.ForeignKey("players.player_id"), nullable=False),
        sa.Column("market_id", sa.Integer(), sa.ForeignKey("prop_markets.market_id"), nullable=False),
        sa.Column("line_value", sa.Float(), nullable=False),
        sa.Column("over_odds", sa.Integer(), nullable=True),
        sa.Column("under_odds", sa.Integer(), nullable=True),
        sa.Column("event_status", sa.String(32), nullable=True),
        sa.Column("source_payload_id", sa.Integer(), sa.ForeignKey("raw_payloads.payload_id"), nullable=True),
        sa.Column("meta", sa.JSON(), nullable=False, server_default=sa.text("'{}'")),
        sa.UniqueConstraint(
            "timestamp",
            "game_id",
            "sportsbook_id",
            "player_id",
            "market_id",
            "line_value",
            name="uq_line_snapshots_dedupe",
        ),
    )
    op.create_index("ix_line_snapshots_timestamp", "line_snapshots", ["timestamp"])
    op.create_index("ix_line_snapshots_game_id", "line_snapshots", ["game_id"])
    op.create_index("ix_line_snapshots_sportsbook_id", "line_snapshots", ["sportsbook_id"])
    op.create_index("ix_line_snapshots_player_id", "line_snapshots", ["player_id"])
    op.create_index("ix_line_snapshots_market_id", "line_snapshots", ["market_id"])

    op.create_table(
        "trading_orders",
        sa.Column("intent_id", sa.String(128), primary_key=True),
        sa.Column("kalshi_order_id", sa.String(128), nullable=True),
        sa.Column("market_symbol", sa.String(255), nullable=False),
        sa.Column("market_key", sa.String(64), nullable=False),
        sa.Column("side", sa.String(16), nullable=False),
        sa.Column("stake", sa.Float(), nullable=False),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column("message", sa.String(512), nullable=False, server_default=""),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_trading_orders_market_symbol", "trading_orders", ["market_symbol"])

    op.create_table(
        "trading_fills",
        sa.Column("fill_id", sa.String(128), primary_key=True),
        sa.Column("intent_id", sa.String(128), sa.ForeignKey("trading_orders.intent_id"), nullable=False),
        sa.Column("market_symbol", sa.String(255), nullable=False),
        sa.Column("market_key", sa.String(64), nullable=False),
        sa.Column("side", sa.String(16), nullable=False),
        sa.Column("stake", sa.Float(), nullable=False),
        sa.Column("price", sa.Float(), nullable=False),
        sa.Column("fee", sa.Float(), nullable=False, server_default="0.0"),
        sa.Column("realized_pnl", sa.Float(), nullable=False, server_default="0.0"),
        sa.Column("kalshi_trade_id", sa.String(128), nullable=True),
        sa.Column("filled_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_trading_fills_intent_id", "trading_fills", ["intent_id"])
    op.create_index("ix_trading_fills_market_symbol", "trading_fills", ["market_symbol"])

    op.create_table(
        "trading_positions",
        sa.Column("market_symbol", sa.String(255), primary_key=True),
        sa.Column("market_key", sa.String(64), nullable=False),
        sa.Column("side", sa.String(16), nullable=False),
        sa.Column("open_stake", sa.Float(), nullable=False, server_default="0.0"),
        sa.Column("weighted_price_total", sa.Float(), nullable=False, server_default="0.0"),
        sa.Column("realized_pnl", sa.Float(), nullable=False, server_default="0.0"),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )

    op.create_table(
        "trading_kill_switch",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("killed", sa.Boolean(), nullable=False, server_default=sa.text("0")),
        sa.Column("set_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.Column("set_by", sa.String(64), nullable=False, server_default="migration"),
        sa.CheckConstraint("id = 1", name="ck_kill_switch_singleton"),
    )

    op.create_table(
        "trading_daily_pnl",
        sa.Column("date", sa.Date(), primary_key=True),
        sa.Column("realized_pnl", sa.Float(), nullable=False, server_default="0.0"),
    )


def downgrade() -> None:
    for table_name in (
        "trading_daily_pnl",
        "trading_kill_switch",
        "trading_positions",
        "trading_fills",
        "trading_orders",
        "line_snapshots",
        "model_runs",
        "raw_payloads",
        "games",
        "prop_markets",
        "sportsbooks",
        "players",
        "teams",
        "seasons",
    ):
        op.drop_table(table_name)
