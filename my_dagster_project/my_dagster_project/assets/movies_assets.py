import pandas as pd
from dagster import asset, AssetCheckResult, multi_asset_check, Definitions, AssetCheckSpec, AssetCheckSeverity, Output, asset_check, HourlyPartitionsDefinition, MetadataValue, AssetMaterialization, AssetExecutionContext
from sqlalchemy import create_engine
from datetime import datetime
from ..partitions import Daily_partition

db_user = "sql12728197"
db_password = "7fz7Efym5T"
db_host = "sql12.freesqldatabase.com"
db_port = 3306
db_name = "sql12728197"

connection_string = f"mysql+mysqlconnector://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

engine = create_engine(connection_string)

@asset(
    check_specs=[AssetCheckSpec(name="check_count", asset="extract_movie_genres")],
    group_name="extract", 
    compute_kind="pandas", 
    io_manager_key="file_io"
    )
def extract_movie_genres() -> pd.DataFrame:
    # Read the CSV file using Pandas
    df = pd.read_csv("./data/summer_movie_genres.csv")
    
    # Log some metadata
    metadata = {
        "row_count": len(df),
        "preview": MetadataValue.md(df.head().to_markdown())
    }
    
    # Return the DataFrame as an output
    yield Output(df, metadata=metadata)
    
    # count check
    yield AssetCheckResult(
        passed=bool(df.shape[0] == 1585),
        severity=AssetCheckSeverity.WARN,
        description="Count check",
    )
    
@asset(
    check_specs=[AssetCheckSpec(name="check_count", asset="extract_movie")],
    group_name="extract", 
    compute_kind="pandas", 
    io_manager_key="file_io"
    )
def extract_movie() -> pd.DataFrame:
    # Read the CSV file using Pandas
    df = pd.read_csv("./data/summer_movies.csv")
    
    print(df['timestamp'].unique())
    
    # Log some metadata
    metadata = {
        "row_count": len(df),
        "preview": MetadataValue.md(df.head().to_markdown())
    }
    # Return the DataFrame as an output
    yield Output(df, metadata=metadata)
    
    # Count check
    yield AssetCheckResult(
        passed=bool(df.shape[0] == 905),
        severity=AssetCheckSeverity.WARN,
        description="Count check",
    )

@asset(
    check_specs=[AssetCheckSpec(name="key_notnull", asset="summer_movie_genres")],
    group_name="stg",
    compute_kind="pandas", 
    io_manager_key="db_io"
    )
def summer_movie_genres(context, extract_movie_genres: pd.DataFrame) -> pd.DataFrame:
    """Transform and Stage Data into Postgres."""
    try:
        context.log.info(extract_movie_genres.head())
        df = extract_movie_genres
        
        metadata = {
            "row_count": len(df),
            "preview": MetadataValue.md(df.head().to_markdown())
        }
        yield Output(df, metadata=metadata)
        
        yield AssetCheckResult(
            passed=bool(df["tconst"].notnull().all()),
            severity=AssetCheckSeverity.WARN,
            description="key notnull",
            metadata = {
                "num_rows": len(df),
                "num_empty": len(df["tconst"].notnull())
            }
        )
    except Exception as e:
        context.log.info(str(e))

@asset(
    check_specs=[AssetCheckSpec(name="key_notnull", asset="summer_movies")],
    group_name="stg", 
    compute_kind="pandas", 
    io_manager_key="db_io",
    partitions_def=Daily_partition
    )
def summer_movies(context, extract_movie: pd.DataFrame) -> pd.DataFrame:
    """Transform and Stage Data into Postgres."""
    try:
        context.log.info(extract_movie.head())
        df = extract_movie
        
        metadata = {
            "row_count": len(df),
            "preview": MetadataValue.md(df.head().to_markdown())
        }
        
        df['runtime_minutes'] = df['runtime_minutes'].fillna(df['runtime_minutes'].mean())
        df['year'] = df['year'].fillna(9999)
        
        yield Output(df, metadata=metadata)
        
        yield AssetCheckResult(
            passed=bool(df["tconst"].notnull().all()),
            severity=AssetCheckSeverity.WARN,
            description="key notnull",
            metadata = {
                "num_rows": len(df),
                "num_empty": len(df["tconst"].notnull())
            }
        )
    except Exception as e:
        context.log.info(str(e))

@asset (
    check_specs=[AssetCheckSpec(name="check_count", asset="transform_movie_data")],
    group_name="transformation", 
    compute_kind="pandas",
    deps = ["summer_movie_genres", "summer_movies"], 
    io_manager_key="file_io"
    )
def transform_movie_data(context: AssetExecutionContext) -> pd.DataFrame:
    
    # Read data from the first table
    
    # partition_date = context.partition_key
    
    # sql = """
    #     select
    #         movies.title_type,
    #         movies.primary_title,
    #         movies.original_title,
    #         movies.year AS year_released,
    #         movies.runtime_minutes,
    #         movies.average_rating,
    #         movies.num_votes,
    #         genres.genres,
    #         movies.release_date
    #     from summer_movies movies
    #     join summer_movie_genres genres 
    #     on movies.tconst = genres.tconst
    #     where movies.release_date >= '{partition_date}'::date
    #     and movies.release_date < '{partition_date}'::date
    # """
    
    # # Perform the join operation
    # with engine.connect() as connection:
    #     joined_data = pd.read_sql(sql, connection)

    
    with engine.connect() as connection:
        summer_movie_genres_data = pd.read_sql("SELECT * FROM summer_movie_genres", connection)
    
    # Read data from the second table
    with engine.connect() as connection:
        summer_movies_data = pd.read_sql("SELECT * FROM summer_movies", connection)
    
    # Perform the join operation
    joined_data = pd.merge(summer_movie_genres_data, summer_movies_data, on="tconst")
    
    # Log some metadata about the joined data
    metadata = {
            "row_count": len(joined_data),
            "preview": MetadataValue.md(joined_data.head().to_markdown())
    }
        
    yield Output(joined_data, metadata=metadata)
    
    yield AssetCheckResult(
        passed=bool(joined_data.shape[0] == 1585),
        severity=AssetCheckSeverity.WARN,
        description="Count check",
    )

def parse_date(date_str):
    return datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')

@asset(
    check_specs=[AssetCheckSpec(name="check_count", asset="joined_movie_genres")],
    group_name="load", 
    compute_kind="pandas", 
    io_manager_key="db_io"
    )
def joined_movie_genres(context, transform_movie_data: pd.DataFrame) -> pd.DataFrame:
    """Transform and Stage Data into Postgres."""
    try:
        context.log.info(transform_movie_data.head())
        df = transform_movie_data
        
        # df['timestamp'] = df['timestamp'].apply(parse_date)
        
        metadata = {
            "row_count": len(df),
            "preview": MetadataValue.md(df.head().to_markdown())
        }
        
        yield Output(df, metadata=metadata)
        
        yield AssetCheckResult(
            passed=bool(df.shape[0] == 1585),
            severity=AssetCheckSeverity.WARN,
            description="Count check",
        )
    except Exception as e:
        context.log.info(str(e))
        