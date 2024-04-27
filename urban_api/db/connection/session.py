"""
Session manager class and get_connection function are defined here.
"""
from loguru import logger
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, create_async_engine

from urban_api.config.app_settings_global import app_settings


class SessionManager:
    """
    A class that implements the necessary functionality for working with the database:
        issuing sessions, storing and updating connection
    """

    def __init__(self) -> None:
        """
        Perform base initialization once in an application run.
        """
        if not hasattr(self, "initialized"):
            self.initialized = False
            self.engine: AsyncEngine = None  # type: ignore

    def __new__(cls):
        """
        Every constructed entity will be one object.
        """
        if not hasattr(cls, "instance"):
            cls.instance = super(SessionManager, cls).__new__(cls)
        return cls.instance

    async def refresh(self) -> None:
        """
        Recreate a connection pool and initialize.
        """
        if self.engine is not None:
            await self.engine.dispose()
        logger.info(
            "Creating pool with max_size = {} on postgresql://{}@{}:{}/{}",
            app_settings.db_pool_size,
            app_settings.db_user,
            app_settings.db_addr,
            app_settings.db_port,
            app_settings.db_name,
        )
        self.engine = create_async_engine(
            app_settings.database_uri,
            future=True,
            pool_size=min(2, app_settings.db_pool_size - 5),
            max_overflow=5,
        )
        async with self.engine.connect() as conn:
            cur = await conn.execute(select(1))
            assert (res := cur.fetchone()[0]) == 1, f"something wrong with database connection: {res} != 1"
        self.initialized = True

    async def shutdown(self) -> None:
        """
        Dispose connection pool and deinitialize.
        """
        if self.initialized and self.engine is not None:
            await self.engine.dispose()
            self.initialized = False


async def get_connection() -> AsyncConnection:
    """
    Get an async connection to the database with a parameters set by global settings in SessionManager.
    """
    manager = SessionManager()
    if manager.engine is not None:
        async with manager.engine.connect() as conn:
            await conn.execute(text(f'SET application_name TO "{app_settings.application_name}"'))
            yield conn
    else:
        raise RuntimeError("session manager was not initialized for some reason")
