update public.dag
set is_active = false
where
	fileloc like '/usr/local/lib/python3.8/dist-packages/airflow/example_dags/%'
	and is_active = true 
;

