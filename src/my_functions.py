import streamlit as st
import requests
import json
import decimal
import boto3
from botocore.exceptions import ClientError


def get_books(list_name):
    # list_name pode ser hardcover-fiction, hardcover-nonfiction, trade-fiction-paperback, mass-market-paperback, paperback-nonfiction, e-book-fiction, e-book-nonfiction, combined-print-and-e-book-fiction, combined-print-and-e-book-nonfiction, advice-how-to-and-miscellaneous
    api_key = "CzCVZ054sGG6xuH6efLCfK4docR7Tu3A"  # Substitua pela sua chave de API em https://developer.nytimes.com/get-started
    url = f"https://api.nytimes.com/svc/books/v3/lists/current/{list_name}.json?api-key={api_key}"

    payload = {}
    headers = {
        'User-Agent': 'Apidog/1.0.0 (https://apidog.com)'
    }

    response = requests.request("GET", url, headers=headers, data=payload)
    return response.json()


def delete_all_items(table):
    # Escanear a tabela para obter todos os itens
    response = table.scan()
    items = response['Items']

    # Continuar a escanear se todos os dados não forem retornados de uma vez
    while 'LastEvaluatedKey' in response:
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        items.extend(response['Items'])

    # Deletar cada item individualmente
    for item in items:
        table.delete_item(
            Key={
                'id': item['id']
            }
        )


def create_dynamodb_table(dynamodb_resource, table_name):
    try:
        table = dynamodb_resource.create_table(
            TableName=table_name,
            KeySchema=[
                {
                    'AttributeName': 'id',
                    'KeyType': 'HASH'  # Partition key
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'id',
                    'AttributeType': 'S'
                }
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            }
        )
        # st.write(f'Table {table_name} created successfully.')
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceInUseException':
            # st.write(f'Table {table_name} already exists.')
            # deletar todas as linhas da tabela:
            delete_all_items(table_name)
            table = dynamodb_resource.Table(table_name)
        else:
            raise
    return table


def load_data_to_dynamodb(table, json_file_path):
    # with open(json_file_path, 'r') as json_file:
    #     data = json.load(json_file)

    data = json_file_path

    # Adapte a estrutura do JSON para acessar os itens desejados
    if 'results' in data and 'books' in data['results']:
        items = data['results']['books']
    else:
        st.write('Invalid JSON structure.')
        return

    for item in items:
        if not isinstance(item, dict):
            st.write(f"Skipping invalid item: {item}")
            continue

        # Adicionando um campo 'id' único para cada item (opcional, baseado na rank)
        item['id'] = str(item['rank'])

        try:
            table.put_item(
                Item=item,
                ConditionExpression='attribute_not_exists(id)'
            )
            # print(f'Added item: {item["id"]}')
        except ClientError as e:
            if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
                st.write(f'Item {item["id"]} already exists. Skipping.')
            else:
                st.write(f'Error adding item: {e}')


def load_data_to_s3(books, key):
    # Criar um cliente S3
    s3 = boto3.client(
        's3',
        region_name='us-east-1',  # Altere para a região desejada
        endpoint_url='http://localhost:4566',  # Endpoint do LocalStack
        aws_access_key_id='fakeAccessKeyId',  # Valores fictícios
        aws_secret_access_key='fakeSecretAccessKey'  # Valores fictícios
    )

    json_data = json.dumps(books)

    # Cria o bucket no S3 (caso não exista)
    try:
        s3.create_bucket(Bucket='my-bucket')
    except ClientError as e:
        if e.response['Error']['Code'] == 'BucketAlreadyOwnedByYou':
            pass
        else:
            raise

    # Carregar o arquivo JSON para o S3
    s3.put_object(
        Bucket='my-bucket',
        Key=key + '.json',
        Body=json_data
    )

    # st.write(f'{key} loaded to S3 successfully.')


def scan_dynamodb_table(dynamodb_resource, table_name):
    table = dynamodb_resource.Table(table_name)
    try:
        response = table.scan()
        items = response.get('Items', [])
        return items
    except Exception as e:
        st.error(f"Error scanning table: {e}")
        return []


def decimal_default(obj):
    if isinstance(obj, decimal.Decimal):
        return float(obj)
    raise TypeError


def convert_decimal(obj):
    if isinstance(obj, list):
        return [convert_decimal(i) for i in obj]
    elif isinstance(obj, dict):
        return {k: convert_decimal(v) for k, v in obj.items()}
    elif isinstance(obj, decimal.Decimal):
        return float(obj)
    return obj


def download_s3_file(s3, bucket, key):
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        data = response['Body'].read().decode('utf-8')
        return json.loads(data, parse_float=decimal.Decimal)
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            st.error(f'File not found: {key}')
        else:
            st.error(f'Error downloading file: {e}')
        return None


# Função para listar arquivos e gerar URLs pré-assinadas
def generate_presigned_urls(bucket_name):
    # Configurações do AWS S3
    ACCESS_KEY = 'fakeAccessKeyId'
    SECRET_KEY = 'fakeSecretAccessKey'

    # Inicializa o cliente S3
    s3_client = boto3.client('s3', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)
    urls = []
    response = s3_client.list_objects_v2(Bucket=bucket_name)

    if 'Contents' in response:
        for obj in response['Contents']:
            file_key = obj['Key']
            url = s3_client.generate_presigned_url('get_object', Params={'Bucket': bucket_name, 'Key': file_key}, ExpiresIn=3600)
            urls.append((file_key, url))

    return urls
