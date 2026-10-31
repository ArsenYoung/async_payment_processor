import subprocess
import sys


def test_application_contracts_import_without_frameworks_or_adapters() -> None:
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import importlib, pkgutil, sys; "
            "import payment_processor.application as application; "
            "[importlib.import_module(module.name) "
            "for module in pkgutil.walk_packages("
            "application.__path__, application.__name__ + '.')]; "
            "forbidden = ('sqlalchemy', 'fastapi', 'faststream', "
            "'payment_processor.db', 'payment_processor.messaging'); "
            "assert not [name for name in sys.modules "
            "if any(name == prefix or name.startswith(prefix + '.') "
            "for prefix in forbidden)]",
        ],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
