from dagster import materialize
from assets import load_genres_asset, load_movies_asset, join_tables

def test_assets():
    assets = [load_genres_asset, load_movies_asset, join_tables]
    result = materialize(assets)
    assert result.success  # Check if the materialization was successful
    output_df = result.output_for_node("load_genres_asset")
    assert len(output_df) > 0  # Check if the output DataFrame is not empty
    output_df = result.output_for_node("load_movies_asset")
    assert len(output_df) > 0  # Check if the output DataFrame is not empty
    output_df = result.output_for_node("join_tables")
    assert len(output_df) > 0  # Check if the output DataFrame is not empty


