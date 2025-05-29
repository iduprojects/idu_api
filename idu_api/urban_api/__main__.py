import os
import tempfile
import typing as tp

import click
import uvicorn

from .config import AppConfig, LoggingConfig, UrbanAPIConfig
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


def _run_uvicorn(configuration: dict[str, tp.Any]) -> tp.NoReturn:
    uvicorn.run(
        "idu_api.urban_api:app",
        **configuration,
    )


@click.command("Run urban api service")
@click.option(
    "--port",
    "-p",
    envvar="PORT",
    type=int,
    show_envvar=True,
    help="Service port number",
)
@click.option(
    "--host",
    envvar="HOST",
    show_envvar=True,
    help="Service HOST address",
)
@click.option(
    "--logger_verbosity",
    "-v",
    type=click.Choice(("TRACE", "DEBUG", "INFO", "WARNING", "ERROR")),
    envvar="LOGGER_VERBOSITY",
    show_envvar=True,
    help="Logger verbosity",
)
@click.option(
    "--debug",
    envvar="DEBUG",
    is_flag=True,
    help="Enable debug mode (auto-reload on change, traceback returned to user, etc.)",
)
@click.option(
    "--config_path",
    envvar="CONFIG_PATH",
    default="urban-api.config.yaml",
    type=click.Path(exists=True, dir_okay=False, path_type=str),
    show_default=True,
    show_envvar=True,
    help="Path to YAML configuration file",
)
def main(
    port: int,
    host: str,
    logger_verbosity: LogLevel,
    debug: bool,
    config_path: str,
):
    """
    Urban api backend service main function, performs configuration
    via config and command line + environment variables overrides.
    """
    config = UrbanAPIConfig.load(config_path)
    logging_section = config.logging if logger_verbosity is None else LoggingConfig(level=logger_verbosity)
    config = UrbanAPIConfig(
        app=AppConfig(
            host=host or config.app.host,
            port=port or config.app.port,
            debug=debug or config.app.debug,
            name=config.app.name,
        ),
        db=config.db,
        auth=config.auth,
        fileserver=config.fileserver,
        external=config.external,
        logging=logging_section,
        prometheus=config.prometheus,
        broker=config.broker,
    )
    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        temp_yaml_config_path = temp_file.name
    config.dump(temp_yaml_config_path)
    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        temp_envfile_path = temp_file.name
    with open(temp_envfile_path, "w", encoding="utf-8") as env_file:
        env_file.write(f"CONFIG_PATH={temp_yaml_config_path}\n")
    try:
        uvicorn_config = {
            "host": config.app.host,
            "port": config.app.port,
            "log_level": config.logging.level.lower(),
            "env_file": temp_envfile_path,
        }
        if config.app.debug:
            try:
                _run_uvicorn(uvicorn_config | {"reload": True})
            except:  # pylint: disable=bare-except
                print("Debug reload is disabled")
                _run_uvicorn(uvicorn_config)
        else:
            _run_uvicorn(uvicorn_config)
    finally:
        if os.path.exists(temp_envfile_path):
            os.remove(temp_envfile_path)
        if os.path.exists(temp_yaml_config_path):
            os.remove(temp_yaml_config_path)


if __name__ in ("__main__", "idu_api.urban_api.__main__"):
    try_load_envfile(os.environ.get("ENVFILE", ".env"))
    main()  # pylint: disable=no-value-for-parameter
