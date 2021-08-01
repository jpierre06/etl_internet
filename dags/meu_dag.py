# Módulos do Airflow
from datetime import datetime, timedelta  
from airflow import DAG
from airflow.operators.bash_operator import BashOperator


# Criar um objeto DAG

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    # Exemplo: Inicia em 20 de Janeiro de 2021
    'start_date': datetime(2021, 6, 25),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    # Em caso de erros, tente rodar novamente apenas 1 vez
    'retries': 1,
    # Tente novamente após 30 segundos depois do erro
    'retry_delay': timedelta(seconds=30),
    # Execute uma vez a cada 15 minutos 
    # 'schedule_interval': '*/15 * * * *'
}



# Definimos nossos parâmetros. 
# Vamos agora informar ao nosso DAG o que ele deve fazer. 
# Fazemos isso declarando tarefas diferentes - T1 e T2. 
# Devemos também definir qual tarefa depende da outra.

with DAG(    
    dag_id='meu_dag',
    default_args=default_args,
    schedule_interval=None,
    tags=['exemplo'],
) as dag:    

    # Vamos Definir a nossa Primeira Tarefa 
    t1 = BashOperator(
    	bash_command='echo "..... Executando Tarefa 1"', 
    	task_id="t01"
    )

    # Vamos definir a nossa segunda tarefa
    t2 = BashOperator(
    	bash_command='echo "..... Executando Tarefa 2"', 
    	task_id="t02"
    )    

    t3 = BashOperator(
    	bash_command='echo "..... Executando Tarefa 3"', 
    	task_id="t03"
    )    


    t4 = BashOperator(
    	bash_command='echo "..... Executando Tarefa 4"', 
    	task_id="t04"
    )    


    t5 = BashOperator(
    	bash_command='echo "..... Executando Tarefa 5"', 
    	task_id="t05"
    )    


    t6 = BashOperator(
    	bash_command='echo "..... Executando Tarefa 6"', 
    	task_id="t06"
    )    



    # Configurar a tarefa T2 para ser dependente da tarefa T1
    t1, t2 , t3
    t4  >> t6
    t3 >> t5
