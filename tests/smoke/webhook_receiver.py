from typing import Any

from fastapi import FastAPI, Response

app = FastAPI()
notifications: list[dict[str, Any]] = []


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/webhook", status_code=204)
async def receive(payload: dict[str, Any]) -> Response:
    notifications.append(payload)
    return Response(status_code=204)


@app.get("/notifications")
async def received_notifications() -> list[dict[str, Any]]:
    return notifications
