from fastapi import FastAPI


def create_app() -> FastAPI:
    return FastAPI(title="Payment Processing Service")


app = create_app()


@app.get("/health", tags=["health"])
async def healthcheck() -> dict[str, str]:
    return {"status": "ok"}
