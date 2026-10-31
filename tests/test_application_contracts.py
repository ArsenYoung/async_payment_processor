import subprocess
import sys

import pytest

pytestmark = pytest.mark.unit


@pytest.mark.parametrize("package", ["application", "services"])
def test_application_contracts_import_without_frameworks_or_adapters(
    package: str,
) -> None:
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import importlib, pkgutil, sys; "
            f"application = importlib.import_module('payment_processor.{package}'); "
            "[importlib.import_module(module.name) "
            "for module in pkgutil.walk_packages("
            "application.__path__, application.__name__ + '.')]; "
            "forbidden = ('sqlalchemy', 'fastapi', 'faststream', "
            "'payment_processor.db', 'payment_processor.messaging', "
            "'payment_processor.adapters', 'httpx'); "
            "assert not [name for name in sys.modules "
            "if any(name == prefix or name.startswith(prefix + '.') "
            "for prefix in forbidden)]",
        ],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
