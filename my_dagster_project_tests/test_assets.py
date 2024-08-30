from dagster import materialize
from .assets import movies_assets

def test_assets():
    assets = [*movies_assets]
    result = materialize(assets)
    assert result.success  # Check if the materialization was successful
    output_df = result.output_for_node("extract_movie_genres")
    assert len(output_df) > 0  # Check if the output DataFrame is not empty
    output_df = result.output_for_node("extract_movie")
    assert len(output_df) > 0  # Check if the output DataFrame is not empty
    output_df = result.output_for_node("joined_movie_genres")
    assert len(output_df) > 0  # Check if the output DataFrame is not empty







