from collections.abc import AsyncGenerator

from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)


def make_session_factory(database_url: str) -> async_sessionmaker[AsyncSession]:
    """Create an async session factory bound to *database_url*.

    The caller (typically a FastAPI lifespan) is responsible for holding the
    returned factory and exposing it via a dependency.
    """
    engine = create_async_engine(database_url, echo=False, pool_pre_ping=True)
    return async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)


async def get_session(
    factory: async_sessionmaker[AsyncSession],
) -> AsyncGenerator[AsyncSession, None]:
    """Yield a single :class:`AsyncSession` from *factory*.

    Intended for use as a FastAPI dependency via ``functools.partial``::

        session_dep = Annotated[AsyncSession, Depends(partial(get_session, factory))]
    """
    async with factory() as session:
        yield session
