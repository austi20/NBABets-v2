"""Add projection-first line evaluation tables and kill-switch flatten seed."""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "20260526_02"
down_revision = "20260526_01"
branch_labels = None
depends_on = None


def _table_exists(table_name: str) -> bool:
    return table_name in set(sa.inspect(op.get_bind()).get_table_names())


def _columns(table_name: str) -> set[str]:
    inspector = sa.inspect(op.get_bind())
    if table_name not in set(inspector.get_table_names()):
        return set()
    return {column["name"] for column in inspector.get_columns(table_name)}


def upgrade() -> None:
    if not _table_exists("stat_projections"):
        op.create_table(
            "stat_projections",
            sa.Column("stat_projection_id", sa.Integer(), primary_key=True, autoincrement=True),
            sa.Column("model_run_id", sa.Integer(), sa.ForeignKey("model_runs.model_run_id"), nullable=False),
            sa.Column("game_id", sa.Integer(), sa.ForeignKey("games.game_id"), nullable=False),
            sa.Column("player_id", sa.Integer(), sa.ForeignKey("players.player_id"), nullable=False),
            sa.Column("market_id", sa.Integer(), sa.ForeignKey("prop_markets.market_id"), nullable=False),
            sa.Column("predicted_at", sa.DateTime(timezone=True), nullable=False),
            sa.Column("projected_mean", sa.Float(), nullable=False),
            sa.Column("projected_variance", sa.Float(), nullable=False),
            sa.Column("projected_median", sa.Float(), nullable=False),
            sa.Column("confidence_interval_low", sa.Float(), nullable=False),
            sa.Column("confidence_interval_high", sa.Float(), nullable=False),
            sa.Column("percentile_25", sa.Float(), nullable=False, server_default="0.0"),
            sa.Column("percentile_75", sa.Float(), nullable=False, server_default="0.0"),
            sa.Column("dnp_risk", sa.Float(), nullable=False, server_default="0.0"),
            sa.Column("boom_probability", sa.Float(), nullable=False, server_default="0.0"),
            sa.Column("bust_probability", sa.Float(), nullable=False, server_default="0.0"),
            sa.Column("availability_branches", sa.Integer(), nullable=False, server_default="1"),
            sa.Column("data_sufficiency_tier", sa.String(8), nullable=False, server_default="A"),
            sa.Column("data_confidence_score", sa.Float(), nullable=False, server_default="1.0"),
            sa.Column("feature_attribution_summary", sa.JSON(), nullable=False, server_default=sa.text("'{}'")),
            sa.Column("meta", sa.JSON(), nullable=False, server_default=sa.text("'{}'")),
            sa.UniqueConstraint(
                "model_run_id",
                "game_id",
                "player_id",
                "market_id",
                name="uq_stat_projections_model_game_player_market",
            ),
        )
        op.create_index("ix_stat_projections_model_run_id", "stat_projections", ["model_run_id"])
        op.create_index("ix_stat_projections_game_id", "stat_projections", ["game_id"])
        op.create_index("ix_stat_projections_player_id", "stat_projections", ["player_id"])
        op.create_index("ix_stat_projections_market_id", "stat_projections", ["market_id"])
        op.create_index("ix_stat_projections_predicted_at", "stat_projections", ["predicted_at"])

    if not _table_exists("prop_line_evaluations"):
        op.create_table(
            "prop_line_evaluations",
            sa.Column("prop_line_evaluation_id", sa.Integer(), primary_key=True, autoincrement=True),
            sa.Column(
                "stat_projection_id",
                sa.Integer(),
                sa.ForeignKey("stat_projections.stat_projection_id"),
                nullable=False,
            ),
            sa.Column("model_run_id", sa.Integer(), sa.ForeignKey("model_runs.model_run_id"), nullable=False),
            sa.Column("game_id", sa.Integer(), sa.ForeignKey("games.game_id"), nullable=False),
            sa.Column("player_id", sa.Integer(), sa.ForeignKey("players.player_id"), nullable=False),
            sa.Column("market_id", sa.Integer(), sa.ForeignKey("prop_markets.market_id"), nullable=False),
            sa.Column("line_snapshot_id", sa.Integer(), sa.ForeignKey("line_snapshots.snapshot_id"), nullable=True),
            sa.Column("source_type", sa.String(32), nullable=False),
            sa.Column("source_id", sa.String(128), nullable=False),
            sa.Column("evaluated_at", sa.DateTime(timezone=True), nullable=False),
            sa.Column("line_value", sa.Float(), nullable=False),
            sa.Column("threshold_value", sa.Float(), nullable=True),
            sa.Column("over_probability", sa.Float(), nullable=False),
            sa.Column("under_probability", sa.Float(), nullable=False),
            sa.Column("push_probability", sa.Float(), nullable=False, server_default="0.0"),
            sa.Column("calibration_adjusted_probability", sa.Float(), nullable=False),
            sa.Column("calibrated_under_probability", sa.Float(), nullable=False),
            sa.Column("fair_over_price", sa.Float(), nullable=False),
            sa.Column("fair_under_price", sa.Float(), nullable=False),
            sa.Column("market_probability", sa.Float(), nullable=True),
            sa.Column("no_vig_market_probability", sa.Float(), nullable=True),
            sa.Column("ev", sa.Float(), nullable=True),
            sa.Column("edge_bps", sa.Integer(), nullable=True),
            sa.Column("recommendation", sa.String(16), nullable=True),
            sa.Column("hit_probability", sa.Float(), nullable=True),
            sa.Column("meta", sa.JSON(), nullable=False, server_default=sa.text("'{}'")),
            sa.UniqueConstraint(
                "stat_projection_id",
                "source_type",
                "source_id",
                "line_value",
                name="uq_prop_line_evaluations_projection_source_line",
            ),
        )
        op.create_index(
            "ix_prop_line_evaluations_stat_projection_id",
            "prop_line_evaluations",
            ["stat_projection_id"],
        )
        op.create_index("ix_prop_line_evaluations_model_run_id", "prop_line_evaluations", ["model_run_id"])
        op.create_index("ix_prop_line_evaluations_game_id", "prop_line_evaluations", ["game_id"])
        op.create_index("ix_prop_line_evaluations_player_id", "prop_line_evaluations", ["player_id"])
        op.create_index("ix_prop_line_evaluations_market_id", "prop_line_evaluations", ["market_id"])
        op.create_index("ix_prop_line_evaluations_line_snapshot_id", "prop_line_evaluations", ["line_snapshot_id"])
        op.create_index("ix_prop_line_evaluations_source_type", "prop_line_evaluations", ["source_type"])
        op.create_index("ix_prop_line_evaluations_source_id", "prop_line_evaluations", ["source_id"])
        op.create_index("ix_prop_line_evaluations_evaluated_at", "prop_line_evaluations", ["evaluated_at"])

    if not _table_exists("trading_kill_switch"):
        op.create_table(
            "trading_kill_switch",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("killed", sa.Boolean(), nullable=False, server_default=sa.text("0")),
            sa.Column("flatten", sa.Boolean(), nullable=False, server_default=sa.text("0")),
            sa.Column("set_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
            sa.Column("set_by", sa.String(64), nullable=False, server_default="migration"),
            sa.CheckConstraint("id = 1", name="ck_kill_switch_singleton"),
        )
    elif "flatten" not in _columns("trading_kill_switch"):
        op.add_column(
            "trading_kill_switch",
            sa.Column("flatten", sa.Boolean(), nullable=False, server_default=sa.text("0")),
        )

    op.execute(
        sa.text(
            """
            INSERT INTO trading_kill_switch (id, killed, flatten, set_at, set_by)
            VALUES (1, 0, 0, CURRENT_TIMESTAMP, 'migration')
            ON CONFLICT(id) DO NOTHING
            """
        )
    )


def downgrade() -> None:
    op.drop_table("prop_line_evaluations")
    op.drop_table("stat_projections")
