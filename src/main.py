import cddp
import cddp.dbxapi as dbxapi

if __name__ == "__main__":
    cddp.entrypoint()
    # config = cddp.load_config('./example/pipeline_fruit.json')
    # landing_path = f"/FileStore/cddp_apps/{config['name']}/landing"
    # working_dir = f"/FileStore/cddp_apps/{config['name']}/"
    # resp = dbxapi.deploy_pipeline(config, "pipeline_fruit_test_2", landing_path, working_dir, True)
    # print(resp)