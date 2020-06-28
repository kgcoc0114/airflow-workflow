# airflow-workflow

### Install Airflow
```shell=
pip install airflow
```
### Setting Configuration
```shell=
sudo vi ~/.bashrc

# Airflow Variables - by default
export AIRFLOW_HOME=~/airflow 
# Airflow Variables

source ~/.bashrc
```

### Airflow Initial
```shell=
airflow initdb
```

### Start Service
```shell=
airflow webserver -p 8080 -D
airflow scheduler -D
```

## DAGs (Workflow)
```shell=
airflow
└─ dags
```



### Add DAG
```shell=
python {{dag_file.py}}
```