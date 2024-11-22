import os
import json
from typing import Dict, List

from google.cloud import storage


data_type_mapping = {
    "Edm.SByte": 'int',
    "Edm.Byte": 'bytes',
    "Edm.Int16": 'int',
    "Edm.Int32": 'int',
    "Edm.Int64": 'long',
    "Edm.Double": 'double',
    "Edm.Float": 'float',
    "Edm.Decimal": {'type': 'bytes', 'logicalType': 'decimal', 'precision': 15, 'scale': 2},
    "Edm.String": 'string',
    "Edm.Binary": 'bytes',
    "Edm.Boolean": 'boolean',
    "Edm.DateTime": {'type': 'string', 'logicalType': 'datetime'},
    "Edm.Time": {'type': 'long', 'logicalType': 'time-micros'},
    "Edm.DateTimeOffset": {'type': 'long', 'logicalType': 'timestamp-micros'},
}


def set_default(obj) -> List:
    if isinstance(obj, set):
        return list(obj)
    raise TypeError


def get_filter(fields: Dict) -> str:
    base_filter = "lastModifiedDateTime ge datetime'${logicalStartTime(yyyy-MM-dd,1d)}T00:00:00' and lastModifiedDateTime le datetime'${logicalStartTime(yyyy-MM-dd'T'HH:mm:ss)}'"
    if 'lastModifiedDateTime' in [field['name'] for field in fields]:
        return base_filter
    else:
        base_filter = base_filter.replace('lastModifiedDateTime', 'lastModifiedOn')
    return base_filter
    

def get_upsert_keys(keys: List) -> str:
    entity_keys = keys
    datetime_key = 'lastModifiedDateTime' if 'lastModifiedDateTime' in entity_keys else 'lastModifiedOn'
    entity_keys.append(datetime_key)

    str_upsert_keys = ','.join(entity_keys)

    return str_upsert_keys


def get_select_fields(fields: Dict) -> str:
    select_fields = ','.join([field['name'] for field in fields])

    return select_fields


def format_metadata(metadata: Dict) -> Dict:
    fields_schema = metadata['edmx:Edmx']['edmx:DataServices']['Schema'][1]

    # Entity name
    name = fields_schema['EntityType']['@Name']

    # Entity keys
    if isinstance(fields_schema['EntityType']['Key']['PropertyRef'], Dict):
        keys = [fields_schema['EntityType']['Key']['PropertyRef']['@Name']]
    else:
        keys = [key['@Name']for key in fields_schema['EntityType']['Key']['PropertyRef']]

    # Entity fields
    fields = [
        {
            'name': field['@Name'],
            'type': field['@Type'],
            'nullable': field['@Nullable'] == "true",
        } for field in fields_schema['EntityType']['Property'] if field['@sap:visible'] == 'true'
    ]

    formatted_metadata = {
        'name': name,
        'keys': keys,
        'fields': fields
    }

    return formatted_metadata


def build_ssff_json_pipeline(metadata: Dict, dataset: str):
    formatted_metadata = format_metadata(metadata=metadata)
    metadata_fields = formatted_metadata['fields']
    entity = formatted_metadata['name']
    keys = formatted_metadata['keys']

    ssff_connection = os.getenv('SSFF_CONNECTION_ID')
    temp_bucket = os.getenv('TEMP_BUCKET')

    json_fields = []

    for field in metadata_fields:
        new_field = {}
        if field['nullable']:
            new_field = {
                'name': field['name'],
                'type': [data_type_mapping.get(field['type']), 'null']
            }
        else:
            new_field = {
                'name': field['name'],
                'type': data_type_mapping.get(field['type'])
            }

        json_fields.append(new_field)

    base_schema = {
        "type": "record",
        "name": "SuccessFactorsColumnMetadata",
        "fields": json_fields
    }

    base_schema_json = json.dumps(base_schema, default=set_default)

    pipeline_json_file = open('base_pipeline_ssff.json')
    pipeline = json.load(pipeline_json_file)

    output_schema = {
        "name": "etlSchemaBody",
        "schema": base_schema_json
    }
    input_schema = {
        "name": "SAP SuccessFactors",
        "schema": base_schema_json
    }

    # Pipeline
    pipeline['name'] = f'{entity}_SuccessFactors'

    # Source
    pipeline['config']['stages'][0]['plugin']['properties']['referenceName'] = entity
    pipeline['config']['stages'][0]['plugin']['properties']['entityName'] = entity
    pipeline['config']['stages'][0]['plugin']['properties']['schema'] = base_schema_json
    pipeline['config']['stages'][0]['plugin']['properties']['connection'] = ssff_connection
    pipeline['config']['stages'][0]['plugin']['properties']['filterOption'] = get_filter(fields=metadata_fields)
    pipeline['config']['stages'][0]['plugin']['properties']['selectOption'] = get_select_fields(fields=metadata_fields)
    pipeline['config']['stages'][0]['outputSchema'] = [output_schema]

    # Target
    pipeline['config']['stages'][1]['plugin']['properties']['table'] = entity
    
    pipeline['config']['stages'][1]['plugin']['properties']['relationTableKey'] = get_upsert_keys(keys=keys)
    pipeline['config']['stages'][1]['plugin']['properties']['connection'] = '${conn(BigQuery-Raw)}'
    pipeline['config']['stages'][1]['plugin']['properties']['schema'] = base_schema_json
    pipeline['config']['stages'][1]['plugin']['properties']['bucket'] = temp_bucket
    pipeline['config']['stages'][1]['plugin']['properties']['dataset'] = dataset
    pipeline['config']['stages'][1]['outputSchema'] = [output_schema]
    pipeline['config']['stages'][1]['inputSchema'] = [input_schema]

    store_ssff_json_pipeline(entity=entity, pipeline=pipeline)
    upload_ssff_json_pipeline(entity=entity)


def store_ssff_json_pipeline(entity: str, pipeline: Dict):

    out_file = open(
        f"./out/{entity}_SuccessFactors-cdap-data-pipeline.json",
        "w"
    )

    json.dump(pipeline, out_file, indent=2)
    out_file.close()


def upload_ssff_json_pipeline(entity: str):
    bucket_name = os.getenv('PIPELINES_BUCKET')
    source_file_name = f"./out/{entity}_SuccessFactors-cdap-data-pipeline.json"
    destination_blob_name = F"{entity}_SuccessFactors-cdap-data-pipeline.json"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(filename=source_file_name)

    print(
        f'File {source_file_name} uploaded to {destination_blob_name}.'
    )
