type HeaderBarProps = {
  boardDateText: string;
  statusText: string;
  dailyPnlText: string;
  onRefreshBoard: () => void;
  refreshDisabled: boolean;
};

export function HeaderBar({
  boardDateText,
  statusText,
  dailyPnlText,
  onRefreshBoard,
  refreshDisabled,
}: HeaderBarProps) {
  return (
    <header className="header-bar">
      <div className="header-title-group">
        <h1 className="header-title">NBA Prop Probability Engine</h1>
        <p className="header-status">{statusText}</p>
      </div>
      <p className="header-board-date">{boardDateText}</p>
      <div className="header-actions">
        <span className="header-pill tabular">{dailyPnlText}</span>
        <button
          type="button"
          className="header-refresh-btn"
          onClick={onRefreshBoard}
          disabled={refreshDisabled}
        >
          Refresh Board
        </button>
      </div>
    </header>
  );
}
