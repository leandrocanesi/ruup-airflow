from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import requests
import pandas as pd
import os

# Definir o diretório onde os arquivos serão salvos
DATA_DIR = os.path.join(os.getcwd(), "data")
os.makedirs(DATA_DIR, exist_ok=True)  # Garante que a pasta existe

# Função para buscar os dados da API
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

    # Salvar como CSV
    df.to_csv(file_path, index=False)
    print(f"Arquivo salvo em {file_path}")

# Criando a DAG
default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 1
}

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

    end = DummyOperator(task_id="end")

    start >> extract_task >> end
