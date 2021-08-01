# Módulos do Airflow
from datetime import datetime, timedelta  
from airflow import DAG
from airflow.operators.bash_operator import BashOperator


# Criar um objeto DAG

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    # Exemplo: Inicia em 20 de Janeiro de 2021
    'start_date': datetime(2021, 7, 6),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    # Em caso de erros, tente rodar novamente apenas 1 vez
    'retries': 1,
    # Tente novamente após 30 segundos depois do erro
    'retry_delay': timedelta(seconds=30),
    # Execute uma vez a cada 5 minutos 
    # 'schedule_interval': '*/5 * * * *'
}



# Definimos nossos parâmetros. 
# Vamos agora informar ao nosso DAG o que ele deve fazer. 
# Fazemos isso declarando tarefas diferentes - T1 e T2. 
# Devemos também definir qual tarefa depende da outra.

with DAG(    
    dag_id='etl_internet',
    default_args=default_args,
    schedule_interval=None,
    tags=['etl', 'internet', 'worldmeters', 'wikipedia'],
) as dag:    

    # Vamos Definir a nossa Primeira Tarefa 
    t1 = BashOperator(
    	bash_command='python3 /root/airflow/scripts/python/worldometers.py', 
    	task_id="worldometers"
    )

    t2 = BashOperator(
    	bash_command='python3 /root/airflow/scripts/python/wikipedia.py', 
    	task_id="wikipedia"
    )


    # Configurar T1
    t1 >> t2
    
    
    
