#------------------------------------------------------------------------------
# Hands-On Lab: Intro to Data Engineering with Snowpark Python
# Script:       07_deploy_task_dag.py
# Author:       Jeremiah Hansen
# Last Updated: 9/26/2023
#------------------------------------------------------------------------------

# SNOWFLAKE ADVANTAGE: Snowpark Python API
# SNOWFLAKE ADVANTAGE: Snowpark Python Task DAG API


from datetime import timedelta

#from snowflake.connector import connect
from snowflake.snowpark import Session
from snowflake.snowpark import functions as F

from snowflake.core import Root
from snowflake.core.task import StoredProcedureCall, Task
from snowflake.core.task.dagv1 import DAGOperation, DAG, DAGTask


# Alternative way to create the tasks
def create_tasks_procedurally(session: Session) -> str:
    database_name = "HOL_DB"
    schema_name = "HOL_SCHEMA"
    warehouse_name = "HOL_WH"

    api_root = Root(session)
    schema = api_root.databases[database_name].schemas[schema_name]
    tasks = schema.tasks

    # Define the tasks
    task1_entity = Task(
        "LOAD_ORDER_DETAIL_TASK",
        definition="CALL LOAD_EXCEL_WORKSHEET_TO_TABLE_SP(BUILD_SCOPED_FILE_URL(@FROSTBYTE_RAW_STAGE, 'intro/order_detail.xlsx'), 'order_detail', 'ORDER_DETAIL')"
    )
    task2_entity = Task(
        "LOAD_LOCATION_TASK",
        definition="CALL LOAD_EXCEL_WORKSHEET_TO_TABLE_SP(BUILD_SCOPED_FILE_URL(@FROSTBYTE_RAW_STAGE, 'intro/location.xlsx'), 'location', 'LOCATION')"
    )
    task3_entity = Task(
        "LOAD_DAILY_CITY_METRICS_TASK",
        definition="CALL LOAD_DAILY_CITY_METRICS_SP()"
    )
    task2_entity.predecessors = [task1_entity.name]
    task3_entity.predecessors = [task2_entity.name]

    # Create the tasks in Snowflake
    task1 = tasks.create(task1_entity, mode="orReplace")
    task2 = tasks.create(task2_entity, mode="orReplace")
    task3 = tasks.create(task3_entity, mode="orReplace")

    # List the tasks in Snowflake
    for t in tasks.iter(like="%task"):
        print(f"Definition of {t.name}: \n\n", t.name, t.definition, sep="", end="\n\n--------------------------\n\n")

    task1.execute()

#    task1.get_current_graphs()

#    task1.suspend()
#    task2.suspend()
#    task3.suspend()
#    task3.delete()
#    task2.delete()
#    task1.delete()


# Create the tasks using the DAG API
def main(session: Session) -> str:
    database_name = "HOL_DB"
    schema_name = "HOL_SCHEMA"
    warehouse_name = "HOL_WH"

    api_root = Root(session)
    schema = api_root.databases[database_name].schemas[schema_name]
    tasks = schema.tasks

    # Define the DAG
    dag_name = "HOL_DAG"
    dag = DAG(dag_name, schedule=timedelta(days=1))
    with dag:
        dag_task1 = DAGTask("LOAD_ORDER_DETAIL_TASK", definition="CALL LOAD_EXCEL_WORKSHEET_TO_TABLE_SP(BUILD_SCOPED_FILE_URL(@FROSTBYTE_RAW_STAGE, 'intro/order_detail.xlsx'), 'order_detail', 'ORDER_DETAIL')")
        dag_task2 = DAGTask("LOAD_LOCATION_TASK", definition="CALL LOAD_EXCEL_WORKSHEET_TO_TABLE_SP(BUILD_SCOPED_FILE_URL(@FROSTBYTE_RAW_STAGE, 'intro/location.xlsx'), 'location', 'LOCATION')")
        dag_task3 = DAGTask("LOAD_DAILY_CITY_METRICS_TASK", definition="CALL LOAD_DAILY_CITY_METRICS_SP()")

        dag_task3 >> dag_task1
        dag_task3 >> dag_task2

    # Create the DAG in Snowflake
    dag_op = DAGOperation(schema)
    dag_op.deploy(dag, mode="orreplace")

    dagiter = dag_op.iter_dags(like='hol_dag%')
    for dag_name in dagiter:
        print(dag_name)

    dag_op.run(dag)

#    dag_op.delete(dag)

    return f"Successfully created and started the DAG"


# For local debugging
# Be aware you may need to type-convert arguments if you add input parameters
if __name__ == '__main__':
    with Session.builder.getOrCreate() as session:
        main(session)
