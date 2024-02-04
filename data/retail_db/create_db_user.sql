CREATE DATABASE retail_db;

CREATE USER retail_user WITH ENCRYPTED PASSWORD 'retail';

GRANT ALL ON DATABASE retail_db TO retail_user;