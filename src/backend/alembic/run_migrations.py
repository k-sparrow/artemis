"""Alembic migration runner — entry point for the artemis-db-migrations container."""

from pathlib import Path

from alembic import command
from alembic.config import Config


def main() -> None:
    alembic_dir = Path(__file__).parent
    cfg = Config()
    cfg.set_main_option("script_location", str(alembic_dir))
    command.upgrade(cfg, "head")


if __name__ == "__main__":
    main()
