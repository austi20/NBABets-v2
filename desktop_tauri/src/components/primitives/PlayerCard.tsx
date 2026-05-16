import { ConfidenceBar } from "./ConfidenceBar";
import { EdgeBadge } from "./EdgeBadge";
import { FreshnessBadge } from "./FreshnessBadge";
import { InjuryPill } from "./InjuryPill";
import { VolatilityBadge, type VolatilityTier } from "./VolatilityBadge";

type PlayerCardProps = {
  playerName: string;
  team: string;
  position: string;
  marketLabel: string;
  lineLabel: string;
  edge: number;
  confidence: number;
  injuryStatus: "Q" | "D" | "O" | "Healthy";
  fetchedAt: string | Date | null;
  avatarLabel?: string;
  bookLabel?: string;
  variant?: "default" | "featured";
  selected?: boolean;
  onClick?: () => void;
  volatilityTier?: VolatilityTier;
  volatilityCoefficient?: number;
};

export function PlayerCard({
  playerName,
  team,
  position,
  marketLabel,
  lineLabel,
  edge,
  confidence,
  injuryStatus,
  fetchedAt,
  avatarLabel,
  bookLabel,
  variant = "default",
  selected = false,
  onClick,
  volatilityTier,
  volatilityCoefficient,
}: PlayerCardProps) {
  return (
    <article
      className={`player-card ${variant === "featured" ? "featured" : ""} ${selected ? "selected" : ""}`}
      onClick={onClick}
      role={onClick ? "button" : undefined}
      tabIndex={onClick ? 0 : undefined}
      onKeyDown={(event) => {
        if (!onClick) {
          return;
        }
        if (event.key === "Enter" || event.key === " ") {
          event.preventDefault();
          onClick();
        }
      }}
    >
      <div className="player-main">
        <div className="player-profile">
          {avatarLabel ? <span className="player-avatar" aria-hidden="true">{avatarLabel}</span> : null}
          <div className="player-id">
            <h3>{playerName}</h3>
            <p>{team}</p>
          </div>
        </div>
        <span className="player-pos micro-label">{position}</span>
      </div>

      <div className="player-metrics">
        <span className="tabular">{marketLabel}</span>
        <span className="tabular">{lineLabel}</span>
        <EdgeBadge edge={edge} />
      </div>

      <ConfidenceBar value={confidence} max={100} />

      <div className="player-badges">
        <InjuryPill status={injuryStatus} />
        <FreshnessBadge fetchedAt={fetchedAt} />
        {volatilityTier ? (
          <VolatilityBadge
            tier={volatilityTier}
            coefficient={volatilityCoefficient ?? 0}
          />
        ) : null}
        {bookLabel ? <span className="player-book">{bookLabel}</span> : null}
        {selected ? <span className="selected-badge">Selected</span> : null}
      </div>
    </article>
  );
}
