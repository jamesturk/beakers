import structlog
import pydantic
from pydantic_settings import BaseSettings, SettingsConfigDict


class Config(BaseSettings):
    pipeline_path: str = pydantic.Field(
        description="Path to the pipeline instance.",
    )
    log_level: str = pydantic.Field(
        "info",
        description="Logging level.",
    )
    log_file: str = pydantic.Field(
        "STDOUT",
        description="Path to the log file.",
    )
    log_format: str = pydantic.Field(
        "text",
        description="Log format.",
    )
    model_config = SettingsConfigDict(env_prefix="databeakers_")


def load_config(**overrides):
    # TODO: toml, env, command line & test
    config = Config(**overrides)
    # configure log output
    processors = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.StackInfoRenderer(),
        structlog.dev.set_exc_info,
        structlog.processors.TimeStamper(),
    ]
    if config.log_format == "json":
        processors.append(structlog.processors.JSONRenderer())
    else:
        processors.append(structlog.dev.ConsoleRenderer())

    if config.log_file == "STDOUT":
        factory = structlog.PrintLoggerFactory()
    else:
        factory = structlog.PrintLoggerFactory(file=open(config.log_file, "a"))

    structlog.configure(
        processors=processors,
        wrapper_class=structlog.make_filtering_bound_logger(
            structlog.processors._NAME_TO_LEVEL[config.log_level]
        ),
        logger_factory=factory,
    )
    return config
