from uuid import UUID

from fastapi import FastAPI, Response

from payment_processor.messaging.schemas import PaymentResultPayload

app = FastAPI()
deliveries: dict[UUID, list[dict]] = {}


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/webhooks", status_code=204)
async def receive(payload: PaymentResultPayload) -> Response:
    deliveries.setdefault(payload.payment_id, []).append(
        payload.model_dump(mode="json")
    )
    return Response(status_code=204)


@app.get("/deliveries/{payment_id}")
async def get_deliveries(payment_id: UUID) -> list[dict]:
    return deliveries.get(payment_id, [])
