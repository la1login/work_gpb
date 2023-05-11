"""
Первый DAG для знакомства с Airflow
"""

from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow import DAG 
from airflow.decorators import task
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.dates import days_ago
import logging
import os
import pathlib

default_args = {
	'owner': 'Loginov',
	'start_date': datetime(2023, 5, 10, 9, 0),
    'end_date': datetime(2023, 5, 15, 9, 0), 
	'email': ['sergey.loginov.work@gmail.com'],
	'catchup': False
}
   
with DAG(
	default_args=default_args,
	dag_id='loginov',
	tags=['examples'],
	start_date= datetime(2023, 5, 10),
	schedule_interval=timedelta(days=1)
) as dag:

	pg_hook = PostgresHook(
		postgres_conn_id='postgres'
		, schema='postgres'
	)
	pg_conn = pg_hook.get_conn()
	cursor = pg_conn.cursor()

	def drop_tbl(**kwargs):

		sql_drop = '''
		drop table if exists postgres.public.loginov_tab; 
		commit; '''
		cursor.execute(sql_drop)

		today_date = kwargs['ds']
		logging.info(f'''Удалена таблица за дату: {today_date}. ''')

	def create_tbl(**kwargs):
		#Создаем таблицу и sequence
		sql_create = '''
		create table if not exists postgres.public.loginov_tab
			(id  serial, surname text, university text, grad_year int, t_changed_dttm timestamp); 
			commit; 
		'''
		cursor.execute(sql_create)

	def insert_from_airflow(**kwargs):
		# Запуск sql скрипта напрямую из airflow
		sql_from_airflow = ''' insert into postgres.public.loginov_tab(id, surname, university, grad_year, t_changed_dttm) 
		values (DEFAULT,  'Musk', 'University of Pennsylvania', 1995, now())
			, (DEFAULT,  'Gates', 'Harvard College', 1975, now())
			, (DEFAULT,  'Brin', 'Stanford University', 1995, now())
			, (DEFAULT,  'Loginov', 'RUDN', 2024, now());
		 commit; '''
		cursor.execute(sql_from_airflow)


# Определение Tasks (Задач - вершин дага)
	task_drop_tbl = PythonOperator(
		task_id='task_drop_tbl',
		python_callable=drop_tbl,
		do_xcom_push=True
		)

	task_create_tbl = PythonOperator(
		task_id='task_create_tbl',
		python_callable=create_tbl,
		do_xcom_push=True
		)

	task_insert_from_airflow = PythonOperator(
		task_id='task_insert_from_airflow',
		python_callable=insert_from_airflow,
		do_xcom_push=True
		)	

# Определение ацикличного графа
	task_drop_tbl >> task_create_tbl >> task_insert_from_airflow
