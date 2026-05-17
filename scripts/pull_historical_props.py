"""Pull historical player props + box scores from BallDontLie for date range,
cache to JSON for offline grading. Run once, then run grade_historical_props.py.
"""
from __future__ import annotations

import asyncio
import json
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import httpx

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from app.config.settings import get_settings  # noqa: E402  # sys.path bootstrap above

OUT_PATH = _REPO_ROOT / "historical_props.json"
START = date(2026, 3, 15)
END = date(2026, 5, 15)
THROTTLE_SECONDS = 0.6  # ~100/min, well below BDL limit


async def get_json(client: httpx.AsyncClient, url: str, params: dict) -> dict:
    last_err = None
    for attempt in range(4):
        try:
            r = await client.get(url, params=params, timeout=30)
            if r.status_code == 429:
                wait = 2 ** attempt
                print(f"  429 backoff {wait}s")
                await asyncio.sleep(wait)
                continue
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_err = e
            await asyncio.sleep(1)
    raise RuntimeError(f"giving up on {url} after retries: {last_err}")


async def pull_games(client, headers, start: date, end: date) -> list[dict]:
    d = start
    out = []
    while d <= end:
        data = await get_json(
            client,
            "https://api.balldontlie.io/v1/games",
            params={"dates[]": [d.isoformat()], "per_page": 100, "Authorization": None},
        )
        for g in data.get("data") or []:
            if g.get("status") == "Final":
                out.append(g)
        d += timedelta(days=1)
        await asyncio.sleep(THROTTLE_SECONDS)
    return out


async def pull_props_for_game(client, headers, game_id: int) -> list[dict]:
    rows = []
    cursor = None
    while True:
        params = {"game_id": game_id, "per_page": 100}
        if cursor:
            params["cursor"] = cursor
        try:
            data = await get_json(
                client,
                "https://api.balldontlie.io/v2/odds/player_props",
                params=params | {"_h": ""},  # placeholder; httpx will ignore
            )
        except RuntimeError:
            return []
        rows.extend(data.get("data") or [])
        cursor = (data.get("meta") or {}).get("next_cursor")
        if not cursor:
            break
    return rows


async def pull_box_scores_for_date(client, headers, d: date) -> list[dict]:
    rows = []
    cursor = None
    while True:
        params = {"dates[]": [d.isoformat()], "per_page": 100}
        if cursor:
            params["cursor"] = cursor
        data = await get_json(
            client,
            "https://api.balldontlie.io/v1/stats",
            params=params,
        )
        rows.extend(data.get("data") or [])
        cursor = (data.get("meta") or {}).get("next_cursor")
        if not cursor:
            break
        await asyncio.sleep(THROTTLE_SECONDS)
    return rows


async def main():
    key = get_settings().balldontlie_api_key
    if not key:
        raise SystemExit("BALLDONTLIE_API_KEY not configured")

    if OUT_PATH.exists():
        cache = json.loads(OUT_PATH.read_text(encoding="utf-8"))
    else:
        cache = {"games": [], "props_by_game": {}, "box_scores_by_date": {}, "fetched_at": None}

    async with httpx.AsyncClient(headers={"Accept": "application/json", "Authorization": key}) as client:
        if not cache["games"]:
            print(f"Pulling games {START}..{END}...")
            cache["games"] = await pull_games(client, None, START, END)
            print(f"  {len(cache['games'])} final games")
            OUT_PATH.write_text(json.dumps(cache, indent=1), encoding="utf-8")

        # Props
        total = len(cache["games"])
        for i, g in enumerate(cache["games"], 1):
            gid = str(g["id"])
            if gid in cache["props_by_game"]:
                continue
            t0 = time.time()
            rows = await pull_props_for_game(client, None, int(gid))
            cache["props_by_game"][gid] = rows
            if i % 20 == 0 or i == total:
                print(f"  props [{i}/{total}] gid={gid} props={len(rows)} ({time.time()-t0:.1f}s)")
                OUT_PATH.write_text(json.dumps(cache, indent=1), encoding="utf-8")
            await asyncio.sleep(THROTTLE_SECONDS)

        # Box scores per date
        dates = sorted({g["date"][:10] for g in cache["games"]})
        for i, d in enumerate(dates, 1):
            if d in cache["box_scores_by_date"]:
                continue
            t0 = time.time()
            d_obj = date.fromisoformat(d)
            rows = await pull_box_scores_for_date(client, None, d_obj)
            cache["box_scores_by_date"][d] = rows
            print(f"  box [{i}/{len(dates)}] {d} logs={len(rows)} ({time.time()-t0:.1f}s)")
            OUT_PATH.write_text(json.dumps(cache, indent=1), encoding="utf-8")
            await asyncio.sleep(THROTTLE_SECONDS)

    cache["fetched_at"] = datetime.utcnow().isoformat()
    OUT_PATH.write_text(json.dumps(cache, indent=1), encoding="utf-8")
    print(f"DONE. {len(cache['games'])} games, "
          f"{sum(len(v) for v in cache['props_by_game'].values())} prop snapshots, "
          f"{sum(len(v) for v in cache['box_scores_by_date'].values())} box rows.")


if __name__ == "__main__":
    asyncio.run(main())
