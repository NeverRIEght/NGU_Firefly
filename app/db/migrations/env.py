import logging
from logging.config import fileConfig

from alembic import context
from alembic.script import ScriptDirectory

from app.db.database_manager import DatabaseManager
from app.model.entity.entities import Base

# Alembic Config object
config = context.config

# Interpret the config file for Python logging.
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

logger = logging.getLogger("alembic.env")

target_metadata = Base.metadata


def _process_revision_directives(context, revision, directives) -> None:
    if not directives:
        return
    script = directives[0]
    script_dir = ScriptDirectory.from_config(config)
    heads = script_dir.get_heads()
    if not heads:
        next_num = 1
    else:
        try:
            next_num = int(heads[0]) + 1
        except ValueError:
            next_num = 1
    script.rev_id = f"{next_num:05d}"


def _do_run_migrations(connection) -> None:
    context.configure(
        connection=connection,
        target_metadata=target_metadata,
        render_as_batch=True,
        process_revision_directives=_process_revision_directives,
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode."""
    db_engine = DatabaseManager.get_instance()._engine
    url = str(db_engine.url)

    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        render_as_batch=True,
        process_revision_directives=_process_revision_directives,
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode."""
    connectable = config.attributes.get("connection", None)

    if connectable is None:
        connectable = DatabaseManager.get_instance()._engine

    if hasattr(connectable, "connect"):
        with connectable.connect() as connection:
            _do_run_migrations(connection)
    else:
        _do_run_migrations(connectable)


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
