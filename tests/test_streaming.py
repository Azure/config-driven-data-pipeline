import cddp
import pytest

@pytest.fixture(scope="session")
def create_spark():
    if 'spark' not in globals():
        globals()['spark'] = cddp.create_spark_session()
    return globals()['spark']

def test_example_pipeline_fruit_streaming(create_spark):
    serving_df = cddp.run_pipeline(create_spark, './example/pipeline_fruit_streaming.json', './tmp', None, None, True, True, 60, True)
    assert len(serving_df)== 1
    list = serving_df[0].toPandas().sort_values(by='fruit', ascending=True).to_records().tolist()
    assert len(list)==7
    assert "[(3, 1, 'Red Grape', 24.0), (6, 2, 'Peach', 39.0), (4, 3, 'Orange', 28.0), (0, 4, 'Green Apple', 45.0), (2, 5, 'Fiji Apple', 56.0), (5, 6, 'Banana', 17.0), (1, 7, 'Green Grape', 36.0)]" == str(list)
