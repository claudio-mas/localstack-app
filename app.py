import streamlit as st
import boto3
import json
import sys
from time import sleep
from src import (
    get_books,
    create_dynamodb_table,
    load_data_to_dynamodb,
    load_data_to_s3,
    scan_dynamodb_table,
    decimal_default,
    convert_decimal,
    generate_presigned_urls,
)


def main():
    st.set_page_config(page_title="LocalStack - The NYT Books Best Sellers", page_icon=":books:", layout="wide")

    st.title("The NYT Books Best Sellers")
    st.write("Select the Best Sellers lists you want to upload to DynamoDB and S3:")

    col1, col2, col3 = st.columns(3)

    categories_col1 = [
        "advice-how-to-and-miscellaneous",
        "combined-print-and-e-book-fiction",
        "combined-print-and-e-book-nonfiction",
        "e-book-fiction",
        "e-book-nonfiction"
    ]

    categories_col2 = [
        "hardcover-fiction",
        "hardcover-nonfiction",
        "mass-market-paperback",
        "paperback-nonfiction",
        "trade-fiction-paperback"
    ]

    # Criando o dicionário checkboxes
    checkboxes = {}

    # Adicionando checkboxes na primeira coluna
    with col1:
        for category in categories_col1:
            checkboxes[category] = st.checkbox(category, value=False)

    # Adicionando checkboxes na segunda coluna
    with col2:
        for category in categories_col2:
            checkboxes[category] = st.checkbox(category, value=False)

    with col3:
        st.image("./img/localstack.png")

    with col1:
        if st.button("Load Data to DynamoDB"):
            dynamodb_resource = boto3.resource(
                'dynamodb',
                region_name='us-east-1',  # Altere para a região desejada
                endpoint_url='http://localhost:4566',  # Endpoint do LocalStack
                aws_access_key_id='fakeAccessKeyId',  # Valores fictícios
                aws_secret_access_key='fakeSecretAccessKey'  # Valores fictícios
            )

            # deletar todas as tabelas existentes
            for key, value in checkboxes.items():
                if value:
                    try:
                        dynamodb_resource.Table(key).delete()
                    except dynamodb_resource.meta.client.exceptions.ResourceNotFoundException:
                        pass

            x = sum(1 for value in checkboxes.values() if value)
            if x == 0:
                st.warning("Selecione pelo menos uma categoria.")
                sys.exit()

            progress_text = "Loading table... "
            my_bar = st.progress(0, text=progress_text)

            x = y = int(100 / x)
            for key, value in checkboxes.items():
                if value:
                    books = get_books(key)
                    table = create_dynamodb_table(dynamodb_resource, key)
                    load_data_to_dynamodb(table, books)
                    my_bar.progress(y, text=f"{progress_text} \n\n {key} loaded.")
                    y += x
                    sleep(6)
            my_bar.progress(100, text="Data loaded successfully.")

            for key, value in checkboxes.items():
                if value:
                    items = scan_dynamodb_table(dynamodb_resource, key)
                    items = convert_decimal(items)  # Convert Decimal objects
                    st.write(f"{key}:")
                    st.json(json.loads(json.dumps(items, default=decimal_default)), expanded=False)

    with col2:
        if st.button("Load Data to S3"):
            x = sum(1 for value in checkboxes.values() if value)
            if x == 0:
                st.warning("Selecione pelo menos uma categoria.")
            else:
                x = y = int(100 / x)
                progress_text = "Loading table... "
                my_bar = st.progress(0, text=progress_text)
                for key, value in checkboxes.items():
                    if value:
                        books = get_books(key)
                        load_data_to_s3(books, key)
                        my_bar.progress(y, text=f"{progress_text} \n\n {key} loaded.")
                        y += x
                        sleep(6)
                my_bar.progress(100, text="Data loaded successfully.")
                urls = generate_presigned_urls('my-bucket')
                for file_key, url in urls:
                    st.markdown(f"[Baixar {file_key}]({url})")

if __name__ == "__main__":
    main()
