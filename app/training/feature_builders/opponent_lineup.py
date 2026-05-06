from __future__ import annotations

import pandas as pd


def add_opponent_lineup_disruption(frame: pd.DataFrame) -> pd.DataFrame:
    team_column = "player_team_id" if "player_team_id" in frame.columns else "team_id"
    if "lineup_instability_score" not in frame.columns or team_column not in frame.columns or "opponent_team_id" not in frame.columns:
        frame["opponent_lineup_disruption"] = 0.0
        return frame

    opponent_disruption = (
        frame.groupby(["game_id", team_column])["lineup_instability_score"]
        .mean()
        .reset_index()
        .rename(
            columns={
                team_column: "opponent_team_id",
                "lineup_instability_score": "opponent_lineup_disruption",
            }
        )
    )
    opponent_disruption["game_id"] = opponent_disruption["game_id"].astype(frame["game_id"].dtype)
    opponent_disruption["opponent_team_id"] = pd.to_numeric(opponent_disruption["opponent_team_id"], errors="coerce")
    frame["opponent_team_id"] = pd.to_numeric(frame["opponent_team_id"], errors="coerce")
    merged = frame.merge(opponent_disruption, on=["game_id", "opponent_team_id"], how="left")
    merged["opponent_lineup_disruption"] = merged["opponent_lineup_disruption"].fillna(0.0)
    return merged
