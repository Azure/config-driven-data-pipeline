import json
import requests
import uuid


class purviewapi():
    """Calling Purview REST APIs to complete below tasks:
    1. Add data collections to Purview account
    2. Register data sources to data collection
    3. Create scans for data sources
    4. Run scans
    5. Get scan status
    6. Create/update lineage for Purview entities/assets
    """

    FAILED_FETCHING_ACCESS_TOKEN_MSG = "[Purview Exception] Failed fetching access token."
    FAILED_CREATING_COLLECTION_MSG = "[Purview Exception] Failed creating Purview collection."
    FAILED_REGISTERING_DATA_SOURCE_MSG = "[Purview Exception] Failed registering data source."
    FAILED_CREATING_DATA_SCAN_MSG = "[Purview Exception] Failed creating data scan."
    FAILED_RUNNING_DATA_SCAN_MSG = "[Purview Exception] Failed running data scan."
    FAILED_GETTING_SCAN_RUN_STATUS_MSG = "[Purview Exception] Failed getting scan run status."
    FAILED_UPDATING_ENTITY_LINEAGE_MSG = "[Purview Exception] Failed updating lineage of entity."
    FAILED_GETTING_ENTITY_DETAILS_MSG = "[Purview Exception] Failed getting entity details."

    def __init__(self, tenant_id, client_id, client_secret, purview_account):
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.purview_account = purview_account
        self.purview_rest_api_base_url = f"https://{purview_account}.purview.azure.com"

    def _get_access_token(self):
        """Get access token by service principals
        """
        fetch_token_url = f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/v2.0/token"
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        body = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "scope": "https://purview.azure.net/.default"
        }

        response = requests.post(
            url=fetch_token_url,
            headers=headers,
            data=body)

        if response.status_code == 200:
            access_token = json.loads(response.text)["access_token"]
            return access_token
        else:
            errors = response.content
            raise Exception(f"{self.FAILED_FETCHING_ACCESS_TOKEN_MSG} error_message: {errors}")

    def _create_adls_collection(self, collection_name):
        """Create ADLS Gen2 collection

        Parameters
        ----------
        collection_name: str
            Target Purview collection name
        """
        access_token = self._get_access_token()

        create_collection_url = (f"{self.purview_rest_api_base_url}"
                                 f"/collections/{collection_name}?api-version=2019-11-01-preview")
        headers = {"Authorization": f"Bearer {access_token}",
                   "Content-Type": "application/json"}
        body = {
            "parentCollection": {
                "referenceName": self.purview_account
            }
        }

        response = requests.put(
            url=create_collection_url,
            headers=headers,
            data=json.dumps(body, default=str))

        if response.status_code == 200:
            created_collection_name = json.loads(response.text)["name"]
            return created_collection_name
        else:
            errors = response.content
            raise Exception(f"{self.FAILED_CREATING_COLLECTION_MSG} error_message: {errors}")

    def _register_data_source(self,
                              collection_name,
                              data_source_name,
                              storage_account_name):
        """Register ADLS Gen2 data source to existing collection

        Parameters
        ----------
        collection_name: str
            Target Purview collection name

        data_source_name: str
            Targer registering data source name

        storage_account_name: str
            Target storage account name
        """
        access_token = self._get_access_token()
        self._create_adls_collection(collection_name)

        register_data_source_url = (f"{self.purview_rest_api_base_url}"
                                    f"/scan/datasources/{data_source_name}?api-version=2022-02-01-preview")
        headers = {"Authorization": f"Bearer {access_token}",
                   "Content-Type": "application/json"}
        body = {
                    "kind": "AdlsGen2",
                    "properties": {
                        "endpoint": f"https://{storage_account_name}.dfs.core.windows.net/",
                        "collection": {
                            "referenceName": collection_name,
                            "type": "CollectionReference"
                        }
                    }
               }

        response = requests.put(
            url=register_data_source_url,
            headers=headers,
            data=json.dumps(body, default=str))

        status_code = response.status_code
        if status_code == 201 or status_code == 200:
            data_source_id = json.loads(response.text)["id"]
            return data_source_id
        elif status_code == 409:
            return "Data source has been registered previously."
        else:
            errors = response.content
            raise Exception(f"{self.FAILED_REGISTERING_DATA_SOURCE_MSG} error_message: {errors}")

    def ceate_data_scan(self,
                        collection_name,
                        data_source_name,
                        storage_account_name,
                        scan_name):
        """Create scan for registered data source

        Parameters
        ----------
        collection_name: str
            Target Purview collection name

        data_source_name: str
            Targer registering data source name

        storage_account_name: str
            Target storage account name

        scan_name: str
            Name of data source scan
        """
        access_token = self._get_access_token()
        self._create_adls_collection(collection_name)

        self._register_data_source(collection_name,
                                   data_source_name,
                                   storage_account_name)

        create_data_scan_url = (f"{self.purview_rest_api_base_url}"
                                f"/scan/datasources/{data_source_name}/scans/{scan_name}?api-version=2022-02-01-preview")
        headers = {"Authorization": f"Bearer {access_token}",
                   "Content-Type": "application/json"}
        body = {
                    "kind": "AdlsGen2Msi",
                    "properties": {
                        "connectedVia": None,
                        "scanRulesetName": "AdlsGen2",
                        "scanRulesetType": "System",
                        "collection": {
                            "referenceName": collection_name,
                            "type": "CollectionReference"
                        }
                    }
               }

        response = requests.put(
            url=create_data_scan_url,
            headers=headers,
            data=json.dumps(body, default=str))

        if response.status_code == 201 or response.status_code == 200:
            data_scan_id = json.loads(response.text)["id"]
            return data_scan_id
        else:
            errors = response.content
            raise Exception(f"{self.FAILED_CREATING_DATA_SCAN_MSG} error_message: {errors}")

    def run_data_scan(self, data_source_name, scan_name):
        """Run a data source scan

        Parameters
        ----------
        data_source_name: str
            Targer registering data source name

        scan_name: str
            Name of data source scan
        """
        access_token = self._get_access_token()
        run_id = str(uuid.uuid4())
        run_data_scan_url = (f"{self.purview_rest_api_base_url}"
                             f"/scan/datasources/{data_source_name}/scans/{scan_name}"
                             f"/runs/{run_id}?api-version=2022-02-01-preview")
        headers = {"Authorization": f"Bearer {access_token}"}

        response = requests.put(
            url=run_data_scan_url,
            headers=headers)

        if response.status_code == 202:
            scanResultId = json.loads(response.text)["scanResultId"]
            return scanResultId
        else:
            errors = response.content
            raise Exception(f"{self.FAILED_RUNNING_DATA_SCAN_MSG} error_message: {errors}")

    def get_data_scan_status(self, data_source_name, scan_name, scan_run_id):
        """Get scan run status by given scan_run_id

        Parameters
        ----------
        data_source_name: str
            Targer registering data source name

        scan_name: str
            Name of data source scan

        scan_run_id: str
            Scan run ID
        """
        access_token = self._get_access_token()
        get_data_scan_status_url = (f"{self.purview_rest_api_base_url}"
                                    f"/scan/datasources/{data_source_name}/scans/{scan_name}"
                                    "/runs?api-version=2022-02-01-preview")
        headers = {"Authorization": f"Bearer {access_token}"}

        response = requests.get(
            url=get_data_scan_status_url,
            headers=headers)

        if response.status_code == 200:
            run_status = None
            run_history = json.loads(response.text)["value"]
            for run in run_history:
                if run["id"] == scan_run_id:
                    run_status = run["status"]
                    break
            return run_status
        else:
            errors = response.content
            raise Exception(f"{self.FAILED_GETTING_SCAN_RUN_STATUS_MSG} error_message: {errors}")

    def get_entity_by_type_and_qualified_name(self,
                                              type_name,
                                              qualified_name):
        """Get entity details by entity type and its qualifiedName, and the entity guid is required
        when creating/updating lineage

        Parameters
        ----------
        type_name: str
            Entity type

        qualified_name: str
            Entity qualified name
        """
        access_token = self._get_access_token()
        update_entity_url = (f"{self.purview_rest_api_base_url}"
                             f"/catalog/api/atlas/v2/entity/uniqueAttribute/type"
                             f"/{type_name}?attr:qualifiedName={qualified_name}")
        headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}

        response = requests.get(
            url=update_entity_url,
            headers=headers)

        if response.status_code == 200:
            guid = None
            entity_details = json.loads(response.text).get("entity", None)
            if entity_details:
                guid = entity_details["guid"]
            return guid
        else:
            errors = response.content
            raise Exception(f"{self.FAILED_GETTING_ENTITY_DETAILS_MSG} error_message: {errors}")

    def maintain_lineage_of_entity(self,
                                   entity_qualified_name,
                                   entity_name,
                                   source_entity_guid,
                                   sink_entity_guid):
        """Add/update lineage of a Purview entity

        Parameters
        ----------
        entity_qualified_name: str
            Entity qualified name

        entity_name: str
            Entity name

        source_entity_guid: str
            Purview entity guid of source/input entity

        sink_entity_guid: str
            Purview entity guid of sink/output entity
        """
        access_token = self._get_access_token()
        update_entity_url = (f"{self.purview_rest_api_base_url}"
                             "/catalog/api/atlas/v2/entity")
        headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}
        body = {
                   "entity": {
                       "status": "ACTIVE",
                       "version": 0,
                       "typeName": "azure_datalake_gen2_resource_set",
                       "attributes": {
                           "qualifiedName": entity_qualified_name,
                           "name": entity_name
                       },
                       "relationshipAttributes": {
                           "sources": [
                               {
                                   "guid": source_entity_guid,
                                   "relationshipType": "direct_lineage_dataset_dataset",
                                   "relationshipAttributes": {
                                       "typeName": "direct_lineage_dataset_dataset"
                                   }
                               }
                           ],
                           "sinks": [
                               {
                                   "guid": sink_entity_guid,
                                   "relationshipType": "direct_lineage_dataset_dataset",
                                   "relationshipAttributes": {
                                       "typeName": "direct_lineage_dataset_dataset"
                                   }
                               }
                           ]
                       }
                   }
                }

        response = requests.post(
            url=update_entity_url,
            headers=headers,
            data=json.dumps(body, default=str))

        if response.status_code == 200:
            updated_entity_guid = "Not detected updates to target entity."
            mutated_entities = json.loads(response.text).get("mutatedEntities", None)
            if mutated_entities and "UPDATE" in mutated_entities:
                updated_details = mutated_entities["UPDATE"][0]
                updated_entity_guid = updated_details["guid"]
            return updated_entity_guid
        else:
            errors = response.content
            raise Exception(f"{self.FAILED_UPDATING_ENTITY_LINEAGE_MSG} error_message: {errors}")
