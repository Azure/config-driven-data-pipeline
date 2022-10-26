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
    list = serving_df[0].toPandas()[['fruit','total']].sort_values(by='fruit', ascending=True).to_records().tolist()
    assert len(list)==7
    assert "[(5, 'Banana', 17.0), (2, 'Fiji Apple', 56.0), (1, 'Green Apple', 45.0), (0, 'Green Grape', 36.0), (4, 'Orange', 28.0), (6, 'Peach', 39.0), (3, 'Red Grape', 24.0)]" == str(list)
            