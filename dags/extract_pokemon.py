from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import requests
import pandas as pd
import os

# Diretório onde os arquivos CSV serão salvos no Windows
DATA_DIR = "C:/Users/leand/projects/pipeline/ruup-airflow/data"

# Garante que a pasta existe
os.makedirs(DATA_DIR, exist_ok=True)

# Função para buscar os dados da API e salvar os dados
def fetch_pokemon_data(**kwargs):
    base_url = "https://pokeapi.co/api/v2/pokemon/"
    pokemon_data = []
    
    for i in range(1, 152):  # Pegamos os primeiros 151 Pokémons
        response = requests.get(f"{base_url}{i}")
        if response.status_code == 200:
            data = response.json()
            name = data['name']
            height = data['height']
            types = ", ".join([t['type']['name'] for t in data['types']])

            # Adicionar ao dataset
            pokemon_data.append({"name": name, "height": height, "types": types})
        else:
            print(f"Erro ao obter dados do Pokémon ID {i}")

    # Criar DataFrame
    df = pd.DataFrame(pokemon_data)

    # Nome do arquivo baseado na data de execução
    execution_date = kwargs['ds']  # Formato YYYY-MM-DD
    file_path = os.path.join(DATA_DIR, f"pokemon_data_{execution_date}.csv")

    # Print para depuração - Verificar onde está salvando
    print(f"✅ Tentando salvar arquivo em: {file_path}")

    # Salvar como CSV no Windows
    df.to_csv(file_path, index=False)

    # Print para confirmar salvamento
    print(f"✅ Arquivo salvo em: {file_path}")

# Função para visualizar os primeiros dados extraídos nos logs do Airflow
def preview_pokemon_data(**kwargs):
    execution_date = kwargs['ds']
    file_path = os.path.join(DATA_DIR, f"pokemon_data_{execution_date}.csv")

    if os.path.exists(file_path):
        df = pd.read_csv(file_path)
        print("📊 Primeiras linhas do arquivo extraído:")
        print(df.head(10))  # Mostra 10 linhas no log do Airflow
    else:
        print(f"⚠️ Arquivo não encontrado: {file_path}")

# Definir argumentos padrão da DAG
default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 1
}

# Criar a DAG
with DAG(
    dag_id="extract_pokemon",
    default_args=default_args,
    schedule_interval="@daily",  # Executa todo dia
    catchup=False
) as dag:

    start = DummyOperator(task_id="start")

    extract_task = PythonOperator(
        task_id="extract_pokemon_data",
        python_callable=fetch_pokemon_data,
        provide_context=True  # Para acessar `execution_date`
    )

    preview_task = PythonOperator(
        task_id="preview_pokemon_data",
        python_callable=preview_pokemon_data,
        provide_context=True
    )

    end = DummyOperator(task_id="end")

    # Definir a ordem das tasks na DAG
    start >> extract_task >> preview_task >> end

