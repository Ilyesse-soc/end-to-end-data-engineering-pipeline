DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'airflow') THEN
    CREATE ROLE airflow LOGIN PASSWORD 'airflow';
  END IF;

  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'warehouse') THEN
    CREATE ROLE warehouse LOGIN PASSWORD 'warehouse';
  END IF;
END $$;

SELECT 'CREATE DATABASE airflow OWNER airflow'
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'airflow')\gexec

SELECT 'CREATE DATABASE warehouse OWNER warehouse'
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'warehouse')\gexec
