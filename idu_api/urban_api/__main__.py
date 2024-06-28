import itertools
import os
import sys
import typing as tp

import click
import uvicorn
from loguru import logger

from .config import UrbanAPIConfig
from .utils.dotenv import try_load_envfile

LogLevel = tp.Literal["TRACE", "DEBUG", "INFO", "WARNING", "ERROR"]


def logger_from_str(logger_text: str) -> list[tuple[LogLevel, str]]:
    """
    Helper function to deconstruct string input argument(s) to logger configuration.

    Examples:
    - logger_from_str("ERROR,errors.log") -> [("ERROR", "errors.log)]
    - logger_from_str("ERROR,errors.log;INFO,info.log") -> [("ERROR", "errors.log), ("INFO", "info.log")]
    """
    res = []
    for item in logger_text.split(";"):
        assert "," in item, f'logger text must be in format "LEVEL,filename" - current value is "{logger_text}"'
        level, filename = item.split(",", 1)
        level = level.upper()
        res.append((level, filename))  # type: ignore
    return res


@click.command("Run urban api service")
@click.option(
    "--db_addr",
    "-H",
    envvar="DB_ADDR",
    default="localhost",
    show_default=True,
    show_envvar=True,
    help="Postgres DBMS address",
)
@click.option(
    "--db_port",
    "-P",
    envvar="DB_PORT",
    type=int,
    default=5432,
    show_default=True,
    show_envvar=True,
    help="Postgres DBMS port",
)
@click.option(
    "--db_name",
    "-D",
    envvar="DB_NAME",
    default="urban_db",
    show_default=True,
    show_envvar=True,
    help="Postgres database name",
)
@click.option(
    "--db_user",
    "-U",
    envvar="DB_USER",
    default="postgres",
    show_default=True,
    show_envvar=True,
    help="Postgres database user",
)
@click.option(
    "--db_pass",
    "-W",
    envvar="DB_PASS",
    default="postgres",
    show_default=True,
    show_envvar=True,
    help="Postgres user password",
)
@click.option(
    "--db_pool_size",
    "-s",
    envvar="DB_POOL_SIZE",
    type=int,
    default=15,
    show_default=True,
    show_envvar=True,
    help="asyncpg database pool maximum size",
)
@click.option(
    "--port",
    "-p",
    envvar="PORT",
    type=int,
    default=8000,
    show_default=True,
    show_envvar=True,
    help="Service port number",
)
@click.option(
    "--host",
    envvar="HOST",
    default="0.0.0.0",
    show_default=True,
    show_envvar=True,
    help="Service HOST address",
)
@click.option(
    "--logger_verbosity",
    "-v",
    type=click.Choice(("TRACE", "DEBUG", "INFO", "WARNING", "ERROR")),
    envvar="LOGGER_VERBOSITY",
    default="DEBUG",
    show_default=True,
    show_envvar=True,
    help="Logger verbosity",
)
@click.option(
    "--add_logger",
    "-l",
    "additional_loggers",
    type=logger_from_str,
    envvar="ADDITIONAL_LOGGERS",
    multiple=True,
    default=[],
    show_default="[]",
    show_envvar=True,
    help="Add logger in format LEVEL,path/to/logfile",
)
@click.option(
    "--debug",
    envvar="DEBUG",
    is_flag=True,
    help="Enable debug mode (auto-reload on change, traceback returned to user, etc.)",
)
def main(  # pylint: disable=too-many-arguments
    db_addr: str,
    db_port: int,
    db_name: str,
    db_user: str,
    db_pass: str,
    db_pool_size: int,
    port: int,
    host: str,
    logger_verbosity: LogLevel,
    additional_loggers: list[tuple[LogLevel, str]],
    debug: bool,
):
    """
    Urban api backend service main function, performs configuration
    via command line parameters and environment variables.
    """
    additional_loggers = list(itertools.chain.from_iterable(additional_loggers))
    settings = UrbanAPIConfig(
        host=host,
        port=port,
        db_addr=db_addr,
        db_port=db_port,
        db_name=db_name,
        db_user=db_user,
        db_pass=db_pass,
        db_pool_size=db_pool_size,
        debug=debug,
    )
    config = UrbanAPIConfig.try_from_env()
    config.update(settings)
    config.to_env()
    if __name__ in ("__main__", "urban_api.__main__"):
        if debug:
            try:
                uvicorn.run("urban_api:app", host=host, port=port, reload=True, log_level=logger_verbosity.lower())
            except:  # pylint: disable=bare-except
                print("Debug reload is disabled")
                uvicorn.run("urban_api:app", host=host, port=port, log_level=logger_verbosity.lower())
        else:
            uvicorn.run("urban_api:app", host=host, port=port, log_level=logger_verbosity.lower())
    else:
        if logger_verbosity != "DEBUG":
            logger.remove()
            logger.add(sys.stderr, level=logger_verbosity)
        for log_level, filename in additional_loggers:
            logger.add(filename, level=log_level)


if __name__ in ("__main__", "urban_api.__main__"):
    try_load_envfile(os.environ.get("ENVFILE", ".env"))
    main()  # pylint: disable=no-value-for-parameter
