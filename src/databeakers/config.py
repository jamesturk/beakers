import sys
import structlog
import pydantic
import toml  # type: ignore
from pydantic_settings import BaseSettings, SettingsConfigDict


class Config(BaseSettings):
    pipeline_path: str = pydantic.Field(
        description="Path to the pipeline instance.",
    )
    log_level: str = pydantic.Field(
        "warning",
        description="Logging level.",
    )
    log_file: str = pydantic.Field(
        "STDERR",
        description="Path to the log file.",
    )
    log_format: str = pydantic.Field(
        "text",
        description="Log format.",
    )
    model_config = SettingsConfigDict(env_prefix="databeakers_")


def load_config(**overrides):
    try:
        tomldata = toml.load("databeakers.toml")
        overrides.update(tomldata["databeakers"])  # pragma: no cover
    except FileNotFoundError:
        pass
    config = Config(**overrides)
    # configure log output
    processors = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.format_exc_info,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        # structlog.processors.StackInfoRenderer(),
    ]

    # either JSON renderer or console renderer
    if config.log_format == "json":
        processors.append(structlog.processors.JSONRenderer())

    if config.log_file == "STDERR":
        if config.log_format != "json":
            processors.append(structlog.dev.ConsoleRenderer())
        factory = structlog.PrintLoggerFactory(sys.stderr)
    else:
        if config.log_format != "json":
            processors.append(
                structlog.processors.KeyValueRenderer(
                    key_order=["level", "event", "timestamp"]
                )
            )
        factory = structlog.PrintLoggerFactory(file=open(config.log_file, "w"))

    structlog.configure(
        # TODO: figure out why this was failing
        # cache_logger_on_first_use=True,
        processors=processors,
        wrapper_class=structlog.make_filtering_bound_logger(
            structlog.processors._NAME_TO_LEVEL[config.log_level]
        ),
        logger_factory=factory,
    )
    return config
