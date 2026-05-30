from __future__ import annotations

# Reconnect hint (milliseconds) sent on the SSE ``retry:`` field so an
# EventSource client auto-reconnects after a dropped stream instead of using
# the browser default. Shared by every server-sent-events endpoint.
SSE_RECONNECT_MS = 3000
