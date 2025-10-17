mkdir -p "${AIRFLOW_HOME:-$HOME/airflow}/dags"
ln -s "/Users/dung.ho/Documents/Training/Python/simple-etl-pipeline-1/dags/etl_pipeline_dag.py" \
"${AIRFLOW_HOME:-$HOME/airflow}/dags/etl_pipeline_dag.py"



# show where Airflow expects DAGs
echo "Effective AIRFLOW DAGs folder: ${AIRFLOW_HOME:-$HOME/airflow}/dags"

# confirm symlink target
readlink "${AIRFLOW_HOME:-$HOME/airflow}/dags/etl_pipeline_dag.py"

# print the top of the file Airflow will parse
sed -n '1,160p' "${AIRFLOW_HOME:-$HOME/airflow}/dags/etl_pipeline_dag.py"


poetry run airflow dags list-import-errors   # should be empty or no entry for etl_pipeline_dag.py
poetry run airflow dags list                 # should show the dag_id `etl_pipeline`
poetry run airflow tasks list etl_pipeline # should list start/process/end


poetry run airflow scheduler &    # run in background or separate terminal
poetry run airflow api-server      # keep in foreground or background as you prefer