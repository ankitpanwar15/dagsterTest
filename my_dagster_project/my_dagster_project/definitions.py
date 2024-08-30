from dagster import Definitions, load_assets_from_modules

from .assets import movies_assets
from .io import file_io, db_io_manager
from .jobs import movies_job
from .schedules import movies_schedule

movies_assets = load_assets_from_modules([movies_assets])

defs = Definitions(
    assets=[*movies_assets],
    resources={
        "file_io": file_io.LocalFileSystemIOManager(),
        "db_io": db_io_manager.postgres_pandas_io_manager.configured(
                {
                "server": {"env": "server"},
                "db": {"env": "db"},
                "uid": {"env": "uid"},
                "pwd": {"env": "pwd"},
                "port": {"env": "port"},
            }
        ),
    },
    jobs=[movies_job],
    schedules=[movies_schedule]
)
