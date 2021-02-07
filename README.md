# Data Challenge

This project run with 2 postgres images, Postgres 9.6 as airflow database and Postgres 10.5 as a source database.


This project is used for running data pipelines. Each pipeline is defined as a DAG (direct acyclic graph) and each job is defined as a task.
All DAGs definitions are under the `dags` directory.

![ data_challenge](https://user-images.githubusercontent.com/2475570/107158998-81780900-695b-11eb-8af2-c1d4e2b3b93f.png)

## Usage

Run the project locally as follows:
Create the image
```sh
docker build -t jdaarevalo/docker-local-airflow:1.0.0 .
```
Upload the containers
```sh
docker-compose up -d
```
This will run a postgres and airflow containers (ports 5432 and 8080 accordingly) and Postgres 10.5 in the port 54321
