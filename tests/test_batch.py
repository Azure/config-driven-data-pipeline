import cddp
import pytest

@pytest.fixture(scope="session")
def create_spark():
    if 'spark' not in globals():
        globals()['spark'] = cddp.create_spark_session()
    return globals()['spark']

def test_example_pipeline_fruit_batch(create_spark):
    serving_df = cddp.run_pipeline(create_spark, './example/pipeline_fruit_batch.json', './tmp', None, None, True, True, 0, True)
    assert len(serving_df)== 1
    list = serving_df[0].toPandas().sort_values(by='id', ascending=True).to_records(index=False).tolist()
    assert len(list)==7
    assert [(1, 'Red Grape', 24.0),\
            (2, 'Peach', 39.0),\
            (3, 'Orange', 28.0),\
            (4, 'Green Apple', 45.0),\
            (5, 'Fiji Apple', 56.0),\
            (6, 'Banana', 17.0),\
            (7, 'Green Grape', 36.0)] == list
