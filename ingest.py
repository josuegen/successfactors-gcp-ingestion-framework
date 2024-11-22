import os
import json
import argparse
import requests
import xmltodict
from datetime import datetime
from typing import Dict, List

from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.cloud import storage
from google.cloud.storage import transfer_manager

from ssff_utils import build_ssff_json_pipeline
import bigquery_sq_utils as bq_utils


SSFF_USER = os.getenv('SSFF_USER')
SSFF_PASSWORD = os.getenv('SSFF_PASSWORD')


data_type_mapping = {
    # Per documentation: https://help.sap.com/docs/SAP_BUSINESSOBJECTS_BUSINESS_INTELLIGENCE_PLATFORM/aa4cb9ab429349e49678e146f05d7341/ec3302ce6fdb101497906a7cb0e91070.html
    # https://www.odata.org/documentation/odata-version-2-0/overview/
    'Edm.Binary': 'BYTES',
    'Edm.Boolean': 'BOOL',
    'Edm.DateTime': 'DATETIME',
    'Edm.Time': 'TIMESTAMP',
    'Edm.Int64': 'INTEGER',
    'Edm.Decimal': 'DECIMAL',
    'Edm.Double': 'FLOAT64',
    'Edm.Float': 'FLOAT64',
    'Edm.Single': 'FLOAT64',
    'Edm.Int32': 'INTEGER',
    'Edm.Byte': 'INTEGER',
    'Edm.Int16': 'INTEGER',
    'Edm.SByte': 'INTEGER',
    'Edm.DateTimeOffset': 'TIMESTAMP',
    'Edm.Guid': 'STRING',
    'Edm.String': 'STRING'
}

data_type_conversion_mapping = {
    'STRING': '{field_name}',
    'DATETIME': 'EXTRACT(DATETIME FROM TIMESTAMP_MILLIS(CAST(REGEXP_EXTRACT({field_name}, r"/Date\((-?\d+)\)") AS INT64))) AS {field_name}',
    'TIMESTAMP': 'TIMESTAMP_MILLIS(CAST(REGEXP_EXTRACT({field_name}, r"/Date\(([-+]?\d+)\+0000\)/") AS INT64)) AS {field_name}',
    'FLOAT': 'CAST({field_name} AS FLOAT64) AS {field_name}',
    'BYTES': 'SELECT CAST(cast(REGEXP_EXTRACT({field_name}, r"[X|binary]\'([A-Fa-f0-9][A-Fa-f0-9]*)\'") AS BYTES) AS STRING)'
}


def get_entity_count(entity: str) -> Dict:
    response = requests.get(
        url=f"https://api19.sapsf.com/odata/v2/{entity}/$count",
        auth=requests.auth.HTTPBasicAuth(
            SSFF_USER,
            SSFF_PASSWORD
        )
    )

    try:
        count = int(response.text)
    except Exception as e:
        print(e)
        print(response.text)

    return count


def get_entity_metadata(entity: str) -> Dict:
    response = requests.get(
        url=f"https://api19.sapsf.com/odata/v2/{entity}/$metadata",
        auth=requests.auth.HTTPBasicAuth(
            SSFF_USER,
            SSFF_PASSWORD
        )
    )

    try:
        data_dict = xmltodict.parse(response.text)
    except Exception as e:
        print(e)
        print(response.text)

    return data_dict


def get_columns_description(field: Dict) -> str:
    if field.get('@sap:picklist', False):
        description = field['@sap:label'] + '. PickList: ' + field['@sap:picklist'] + '.'
    else:
        description = field['@sap:label'] + '.'

    return description


def process_metadata(metadata: Dict) -> Dict:
    doc_schema = metadata['edmx:Edmx']['edmx:DataServices']['Schema'][0]
    fields_schema = metadata['edmx:Edmx']['edmx:DataServices']['Schema'][1]
    documentation = doc_schema['EntityContainer']['EntitySet']['Documentation']

    # Entity name
    name = fields_schema['EntityType']['@Name']

    # Entity description
    entity_description = documentation['LongDescription']

    # Entity keys
    if isinstance(fields_schema['EntityType']['Key']['PropertyRef'], Dict):
        keys = [fields_schema['EntityType']['Key']['PropertyRef']['@Name']]
    else:
        keys = [key['@Name']for key in fields_schema['EntityType']['Key']['PropertyRef']]

    # Entity fields
    fields = [
        {
            'name': field['@Name'],
            'type': data_type_mapping.get(field['@Type']),
            'mode': '' if field['@Nullable'] == "true" else "NOT NULL",
            'description': get_columns_description(field)
        } for field in metadata['edmx:Edmx']['edmx:DataServices']['Schema'][1]['EntityType']['Property'] if field['@sap:visible'] == 'true'
    ]

    # Entity module
    tags = documentation['sap:tagcollection']['sap:tag']
    if isinstance(tags, List):
        module = tags[0]
    elif isinstance(tags, str):
        module = tags
    else:
        raise Exception('Could not extract SSFF module name from the metadata')

    entity_dict = {
        'name': name,
        'keys': keys,
        'fields': fields,
        'module': module,
        'description': entity_description
    }

    return entity_dict


def get_next_page_url(page: Dict):
    if '__next' not in page.keys():
        return None

    next = page.get('__next')

    return next


def make_odata_request(url: str) -> Dict:
    response = requests.get(
        url=url,
        auth=requests.auth.HTTPBasicAuth(
            SSFF_USER,
            SSFF_PASSWORD
        )
    )

    json_response = response.json()
    data = json_response.get('d', None)

    if data is not None:
        return data
    else:
        raise Exception(response.text)


def get_entity_data(metadata: Dict) -> List:
    page_number = 0
    fields = metadata['fields']
    metadata_fields = [field['name'] for field in fields]

    entity = metadata['name']

    entity_row_count = get_entity_count(entity=entity)
    print(f'Processing {entity_row_count} rows from entity: {entity}')
    sf_odata_url = f"https://api19.sapsf.com/odata/v2/{entity}?$select={','.join(metadata_fields)}&paging=snapshot&$format=json"

    next_page = 'null'

    while next_page is not None:
        response = make_odata_request(url=sf_odata_url)
        results = response.get('results')

        print(f'Iterating page: {page_number}. Rows: {len(results)}')
        store_data(entity=entity, data=results, page=page_number)
        next_page = get_next_page_url(response)
        sf_odata_url = next_page
        page_number += 1


def create_bq_module_dataset(client, dataset_id: str):
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = "US"

    dataset = client.create_dataset(dataset, timeout=120)
    print(f'Created dataset {client.project}.{dataset.dataset_id}')


def create_bq_final_table(metadata: Dict) -> str:
    project = os.getenv('PROJECT_ID')

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
        PARTITION BY DATE_TRUNC(_PARTITIONTIME, DAY)
        CLUSTER BY {','.join(keys)}
        OPTIONS (
            description='{entity_description}'
        );
    """

    table = client.query_and_wait(ddl)
    print(f'Created table {table_id}')

    return table_id


def create_bq_raw_table(metadata: Dict) -> str:
    project = os.getenv('PROJECT_ID')

    entity = metadata['name']
    module = metadata['module']
    fields = metadata['fields']

    for field in fields:
        if field['type'] != 'BOOL':
            field.update({'type': 'STRING'})

    dataset_module = module[module.find("(")+1: module.find(")")].lower().strip()

    client = bigquery.Client(project=project)
    dataset_id = f'{project}.ds_sfsf_{dataset_module}'

    table_id = f'{dataset_id}.temp_{entity}'

    ddl = f"""
        CREATE TABLE IF NOT EXISTS {table_id}(
            {','.join([field['name'] + " " + field['type'] for field in fields])}
        );
    """

    table = client.query_and_wait(ddl)
    print(f'Created table {table_id}')

    return table_id


def store_data(entity: str, data: List, page: int):
    out_file = open(
        f"./data/{entity}_data_{page}.json",
        "w"
    )

    for line in data:
        line.pop('__metadata')
        out_file.write(json.dumps(line, indent=None)+'\n')
    
    out_file.close()


def store_metadata(entity: str, metadata: List):
    out_file = open(
        f"./metadata/{entity}_metadata.json",
        "w"
    )

    json.dump(metadata, out_file, indent=2)
    out_file.close()


def move_data_local_to_gcs(prefix: str):
    path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'data')
    filenames = [filename for filename in os.listdir(path) if filename.endswith('.json')]
    bucket_name = os.getenv('TEMP_BUCKET')
    workers = 8

    project = os.getenv('PROJECT_ID')
    storage_client = storage.Client(project=project)
    bucket = storage_client.bucket(bucket_name)

    results = transfer_manager.upload_many_from_filenames(
        bucket, filenames,
        source_directory=path,
        blob_name_prefix=prefix+'/',
        max_workers=workers
    )

    for name, result in zip(filenames, results):
        if isinstance(result, Exception):
            print("Failed to upload {} due to exception: {}".format(name, result))
        else:
            print("Uploaded {} to {}.".format(name, bucket.name))


def insert_data_into_bq(metadata: Dict, prefix: str, table_id: str):
    bucket_name = os.getenv('TEMP_BUCKET')
    project = os.getenv('PROJECT_ID')

    fields = metadata['fields']

    client = bigquery.Client(project=project)

    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField(field['name'], "STRING" if field['type']!='BOOL' else field['type']) for field in fields
        ],
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    )
    uri = f'gs://{bucket_name}/{prefix}/*.json'

    load_job = client.load_table_from_uri(
        uri,
        table_id,
        location='US',
        job_config=job_config,
    )

    load_job.result()

    destination_table = client.get_table(table_id)
    print(f'Loaded {destination_table.num_rows} rows into {destination_table.table_id}.')


def insert_raw_into_final_bq(formatted_table: str, raw_table: str):
    project = os.getenv('PROJECT_ID')

    client = bigquery.Client(project=project)

    base_query = """
    INSERT INTO {target_table}(
    {target_columns}
    )
    SELECT
    {select_statement}
    FROM {source_table};
    """

    select_lines = []
    formatted_table = client.get_table(table=formatted_table)
    raw_table = client.get_table(table=raw_table)

    formatted_schema = formatted_table.schema
    raw_schema = raw_table.schema

    field_mapping = {
        field.name: field.field_type for field in formatted_schema
    }

    default_conversion_expr = 'CAST({field_name} AS {default_type}) AS {field_name}'

    for field in raw_schema:
        type = field_mapping.get(field.name)

        select_line = ""
        try:
            select_line = data_type_conversion_mapping[type]
            select_line = select_line.format(field_name=field.name)
        except KeyError:
            print(f'Type {type} not in the initial mapping. Defaulting to straight cast: CAST(x as A_TYPE).')
            select_line = default_conversion_expr.format(
                field_name=field.name,
                default_type=type
            )

        select_line = '\t'+select_line

        select_lines.append(select_line)

    final_insert_query = base_query.format(
        table_name=raw_table,
        select_statement=',\n'.join(select_lines),
        source_table=raw_table,
        target_table=formatted_table,
        target_columns=',\n'.join([field.name for field in raw_schema])
    )

    results = client.query_and_wait(final_insert_query)
    print(f'Insert Job with ID: {results._job_id} affected {results._num_dml_affected_rows}')


def gcp_clean_up(temp_table_id: str, prefix: str):
    project = os.getenv('PROJECT_ID')
    bucket_name = os.getenv('TEMP_BUCKET')

    print('Cleaning up in GCP ...')
    # Delete BQ temp table
    client = bigquery.Client(project=project)
    client.delete_table(temp_table_id, not_found_ok=True)
    print(f'Deleted table {temp_table_id}')

    # Delete prefix in GCS
    storage_client = storage.Client(project=project)

    bucket = storage_client.bucket(bucket_name)
    try:
        bucket.delete_blobs(blobs=list(bucket.list_blobs(prefix=prefix)))
        print(f'Prefix {prefix} deleted')
    except NotFound:
        print('Prefix {prefix} not found. Skipping')
    except Exception as e:
        print(str(e.message))


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('-e', '--entity', required=True)
    args = parser.parse_args()
    entity = args.entity

    metadata = get_entity_metadata(entity=entity)
    parsed_metadata = process_metadata(metadata=metadata)
    store_metadata(entity=entity, metadata=parsed_metadata)

    timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    prefix = entity.lower() + '_' + timestamp

    # Historical load and raw infra provision
    final_table_id = create_bq_final_table(metadata=parsed_metadata)
    rf_table_id = bq_utils.create_bq_refined_table(
        metadata=parsed_metadata
    )
    temp_table_id = create_bq_raw_table(metadata=parsed_metadata)
    dataset_id = final_table_id.split('.')[1]

    get_entity_data(metadata=parsed_metadata)
    move_data_local_to_gcs(prefix=prefix)
    insert_data_into_bq(
        metadata=parsed_metadata,
        prefix=prefix,
        table_id=temp_table_id
    )

    insert_raw_into_final_bq(
        formatted_table=final_table_id,
        raw_table=temp_table_id
    )

    gcp_clean_up(temp_table_id=temp_table_id, prefix=prefix)

    # Builds SSFF JSON pipeline
    build_ssff_json_pipeline(metadata=metadata, dataset=dataset_id)

    # Creates BQ scheduled query
    merge_query = bq_utils.create_merge_query(
        metadata=parsed_metadata,
        raw_table_id=final_table_id,
        rf_table_id=rf_table_id
    )

    bq_utils.run_merge_query_once(query=merge_query)
    bq_utils.create_scheduled_query(query=merge_query, entity=entity)
