import cddp.ingestion.autoloader
import cddp.ingestion.azure_eventhub
import cddp.ingestion.azure_adls_gen1
import cddp.ingestion.azure_adls_gen2
import cddp.ingestion.azure_adls_gen2_syn
import cddp.ingestion.azure_adls_adf_syn
import cddp.ingestion.filestore
import cddp.ingestion.deltalake

import cddp.utils as utils

def start_ingestion_task(task, spark):
    type = task['input']['type']
    if type == 'autoloader':
        return autoloader.start_ingestion_task(task, spark)
    elif type == 'azure_eventhub':
        return azure_eventhub.start_ingestion_task(task, spark)
    elif type == 'jdbc':
        return jdbc.start_ingestion_task(task, spark)
    elif type == 'azure_adf':
        return azure_adls_adf_syn.start_ingestion_task(task, spark)
    elif type == 'deltalake':
        return deltalake.start_ingestion_task(task, spark)
    elif type == 'filestore':
        return filestore.start_ingestion_task(task, spark)
    elif type == 'azure_adls_gen2':
        if utils.is_running_on_synapse(spark):
            return azure_adls_gen2_syn.start_ingestion_task(task, spark)

        return azure_adls_gen2.start_ingestion_task(task, spark)
    elif type == 'azure_adls_gen1':
        return azure_adls_gen1.start_ingestion_task(task, spark)
    else:
        raise Exception('Unknown ingestion type: ' + type)


if __name__ == '__main__':
    task = {
        'type': 'autoloader',
        'name': 'test',
        'format': 'csv',
        'location': 'test',
        'delimiter': ',',
        'header': True,
        'quote': '"',
        'escape': '\\',
        'encoding': 'utf-8',
        'schema': {
            'fields': [
                {
                    'name': 'id',
                    'type': 'string',
                    'nullable': False,
                    'metadata': {}
                },
                {
                    'name': 'name',
                    'type': 'string',
                    'nullable': False,
                    'metadata': {}
                },
                {
                    'name': 'age',
                    'type': 'integer',
                    'nullable': False,
                    'metadata': {}
                }
            ]
        }
    }
    start_ingest_task(task)