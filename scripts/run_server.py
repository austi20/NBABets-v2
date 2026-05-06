from __future__ import annotations

import uvicorn


def main() -> None:
    uvicorn.run(
        "app.server.main:create_app",
        host="127.0.0.1",
        port=8765,
        reload=True,
        factory=True,
    )


if __name__ == "__main__":
    main()

