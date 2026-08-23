"""Database engine, session management, and migration execution utilities."""

import logging
from contextlib import contextmanager
from pathlib import Path
from typing import Generator, Optional

from alembic.config import Config
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, declarative_base, sessionmaker

from alembic import command
from src.common.config import get_settings

logger = logging.getLogger(__name__)

Base = declarative_base()

_engine: Optional[Engine] = None
_session_factory: Optional[sessionmaker] = None


def get_engine(url: Optional[str] = None) -> Engine:
    """Get or create the singleton SQLAlchemy database engine."""
    global _engine
    if _engine is None or url is not None:
        db_url = url or get_settings().database_url
        _engine = create_engine(
            db_url,
            pool_size=10,
            max_overflow=20,
            pool_pre_ping=True,
            pool_recycle=300,
        )
    return _engine


def get_session_factory(engine: Optional[Engine] = None) -> sessionmaker:
    """Get or create the singleton SQLAlchemy sessionmaker factory."""
    global _session_factory
    if _session_factory is None or engine is not None:
        eng = engine or get_engine()
        _session_factory = sessionmaker(
            autocommit=False, autoflush=False, bind=eng, expire_on_commit=False
        )
    return _session_factory


@contextmanager
def get_db_session() -> Generator[Session, None, None]:
    """Context manager yielding a transactional database session."""
    factory = get_session_factory()
    session: Session = factory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def check_database_connection(engine: Optional[Engine] = None) -> bool:
    """Verify that PostgreSQL is reachable and executing queries."""
    eng = engine or get_engine()
    try:
        with eng.connect() as conn:
            result = conn.execute(text("SELECT 1")).scalar()
            return result == 1
    except Exception as e:
        logger.error(f"Database connection check failed: {e}")
        return False


def run_migrations(
    alembic_ini_path: Optional[str] = None,
    database_url: Optional[str] = None,
    target_revision: str = "head",
) -> None:
    """Run Alembic database migrations programmatically to the target revision."""
    if alembic_ini_path is None:
        # Default to root alembic.ini relative to project root
        root_dir = Path(__file__).resolve().parent.parent.parent
        alembic_ini_path = str(root_dir / "alembic.ini")

    config = Config(alembic_ini_path)

    if database_url:
        config.set_main_option("sqlalchemy.url", database_url)
    else:
        config.set_main_option("sqlalchemy.url", get_settings().database_url)

    logger.info(f"Running database migrations to revision: {target_revision}")
    command.upgrade(config, target_revision)
    logger.info("Database migrations completed successfully.")
