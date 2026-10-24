"""API application entry point."""

from fastapi import FastAPI


def create_app() -> FastAPI:
    """Create and configure the API application."""
    return FastAPI(title="Payment Processing Service")


app = create_app()
