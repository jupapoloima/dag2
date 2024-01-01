from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# Definir los argumentos del DAG
default_args = {
    'owner': 'usuario',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Crear un objeto DAG
dag = DAG(
    'ejemplo_dag',
    default_args=default_args,
    description='Un DAG simple de ejemplo',
    schedule_interval=timedelta(days=1),  # Frecuencia de ejecución (cada día)
)

# Definir una tarea
def imprimir_mensaje():
    print("Hola, Airflow")

# Crear un operador Python
tarea_python = PythonOperator(
    task_id='tarea_imprimir',
    python_callable=imprimir_mensaje,
    dag=dag,
)

# Establecer la relación entre tareas (si es necesario)
# En este caso, no hay dependencias entre tareas, ya que es un DAG simple.

# Configurar el flujo de ejecución del DAG
tarea_python

if __name__ == "__main__":
    dag.cli()
