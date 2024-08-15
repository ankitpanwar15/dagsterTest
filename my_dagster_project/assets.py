import pandas as pd
from dagster import asset, Output, HourlyPartitionsDefinition, MetadataValue, AssetMaterialization, AssetExecutionContext
from sqlalchemy import create_engine
from dagster_pandas import create_dagster_pandas_dataframe_type, PandasColumn

mysql_path = 'mysql://apanwar:apanwar@192.168.1.100/test' #TODO: read from file or secret store

def dbConnection(mysql_path):
    return create_engine(mysql_path)

engine = dbConnection(mysql_path)

@asset
def load_genres_asset():
    # Load the CSV file into a DataFrame
    df = pd.read_csv("data/summer_movie_genres.csv")
    
    df.to_sql('summer_movie_genres', con=engine, if_exists='replace')
    
    metadata = {
        "row_count": len(df),
        "preview": MetadataValue.md(df.head().to_markdown())
    }
    
    asset_materialization=AssetMaterialization(
            description="Genres CSV data materialized to relational database",
            metadata={"table_name": "summer_movie_genres"}
        )
    
    return Output(df, metadata=metadata, asset_materialization= asset_materialization)

@asset(partitions_def=HourlyPartitionsDefinition(start_date="2024-01-01"))
def load_movies_asset(context: AssetExecutionContext):
    # Load the CSV file into a DataFrame
    data = pd.read_csv("data/summer_movies.csv")
    
    partition_hour = context.partition_key
    data['timestamp'] = pd.to_datetime(data['timestamp'])
    df = data[data['timestamp'].dt.strftime('%Y-%m-%d %H') == partition_hour]
    
    df.to_sql('summer_movies', con=engine, if_exists='replace')
    
    metadata = {
        "row_count": len(df),
        "preview": MetadataValue.md(df.head().to_markdown())
    }
    
    asset_materialization=AssetMaterialization(
            description="Movies CSV data materialized to relational database",
            metadata={"table_name": "summer_movies", "partition_hour": partition_hour}
        )
    
    return Output(df, metadata=metadata, asset_materialization= asset_materialization)

@asset
def join_tables(load_movies_asset, load_genres_asset):
    # Perform the join operation
    joined_table = pd.merge(load_movies_asset, load_genres_asset, on='tconst')
    
    # Write the joined table to the database
    joined_table.to_sql('joined_table', con=engine, if_exists='replace')
    
    # Return materialization information
    return Output(
        value=None,
        metadata={
            "num_records": len(joined_table),
            "preview": joined_table.head().to_markdown()
        },
        asset_materialization=AssetMaterialization(
            description="Joined table1 and table2",
            metadata={"table_name": "joined_table"}
        )
    )
