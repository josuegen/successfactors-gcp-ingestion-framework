import os
import sqlparse
from typing import Dict

from google.cloud import bigquery_datatransfer
from google.cloud import bigquery
from google.cloud.exceptions import NotFound


def create_merge_query(metadata: Dict, raw_table_id: str, rf_table_id: str) -> str:
    entity = metadata['name']
    fields = metadata['fields']
    keys = metadata['keys']

    base_query = """
    MERGE INTO
    {rf_table_id} AS target
    USING(
    SELECT
    {columns}
    FROM {raw_table_id}
    WHERE TIMESTAMP_TRUNC(_PARTITIONTIME, DAY) = TIMESTAMP(FORMAT_DATE("%Y-%m-%d",CURRENT_DATE()))
    ) AS source
    ON {condition}
    WHEN MATCHED
    THEN UPDATE
    SET
    {column_mapping}
    WHEN NOT MATCHED BY TARGET
    THEN INSERT(
    {columns}
    )
    VALUES(
    {columns}
    );
    """

    column_names = [field['name'] for field in fields]

    columns = ', '.join(column_names)
    condition = ' AND '.join([f'source.{key}=target.{key}' for key in keys])
    mapping = ', '.join([f'target.{column}=source.{column}' for column in column_names])

    merge_query = sqlparse.format(
        base_query.format(
            entity=entity,
            raw_table_id=raw_table_id,
            rf_table_id=rf_table_id,
            columns=columns,
            condition=condition,
            column_mapping=mapping
        ),
        reindent=True,
        keyword_case='upper'
    )

    return merge_query


def create_bq_module_dataset(client, dataset_id: str):
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = "US"

    dataset = client.create_dataset(dataset, timeout=120)
    print(f'Created dataset {client.project}.{dataset.dataset_id}')


def create_bq_refined_table(metadata: Dict) -> str:
    project = os.getenv('RF_PROJECT_ID')

    entity = metadata['name']
    module = metadata['module']
    fields = metadata['fields']
    keys = metadata['keys']
    entity_description = metadata.get('description', '')

    dataset_module = module[module.find("(")+1: module.find(")")].lower().strip()

    client = bigquery.Client(project=project)
    dataset_id = f'{project}.ds_sfsf_{dataset_module}'

    try:
        client.get_dataset(dataset_id)
        print(f'Found dataset {dataset_id}. Skipping creation')
    except NotFound:
        print(f'Dataset {dataset_id} is not found. Creating ...')
        create_bq_module_dataset(client=client, dataset_id=dataset_id)

    table_id = f'{dataset_id}.{entity}'

    ddl = f"""
        CREATE TABLE IF NOT EXISTS {table_id}(
            {','.join([field['name'] + " " + field['type'] + " " + field['mode'] + " " + "OPTIONS(description='" + field['description'] + "')" for field in fields])}
        )
        CLUSTER BY {','.join(keys)}
        OPTIONS (
            description='{entity_description}'
        );
    """

    table = client.query_and_wait(ddl)
    print(f'Created refined table {table_id}')

    return table_id


def run_merge_query_once(query: str):
    project = os.getenv('RF_PROJECT_ID')
    client = bigquery.Client(project=project)

    result = client.query_and_wait(query, wait_timeout=600)

    print(f'MERGE query affected: {result._num_dml_affected_rows} rows in the refined table')


def create_scheduled_query(query: str, entity: str):
    project = os.getenv('RF_PROJECT_ID')
    sq_service_account = os.getenv('BQ_SQ_SA')

    transfer_client = bigquery_datatransfer.DataTransferServiceClient(
        client_options={
            'quota_project_id': project
        }
    )

    service_account_name = sq_service_account

    parent = f'projects/{project}/locations/us'

    transfer_config = bigquery_datatransfer.TransferConfig(
        display_name=f'{entity}_consulta_programada',
        data_source_id='scheduled_query',
        params={
            'query': query,
        },
        schedule='every day 14:30',
    )

    transfer_config = transfer_client.create_transfer_config(
        bigquery_datatransfer.CreateTransferConfigRequest(
            parent=parent,
            transfer_config=transfer_config,
            service_account_name=service_account_name,
        )
    )

    print(f'Created scheduled query: {transfer_config.name}')
