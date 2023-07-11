-- alter user postgres replication;

create database test_db;
\c test_db;
CREATE PUBLICATION dbz_publication FOR ALL TABLES;
-- select * from pg_publication_tables;

CREATE TABLE users (
  user_id INTEGER NOT NULL PRIMARY KEY,
  name VARCHAR(45) NOT NULL,
  age INTEGER,
  locked BOOLEAN NOT NULL DEFAULT false,
  created_at TIMESTAMP NOT NULL
);
INSERT INTO users (user_id, name, age, created_at) VALUES (1, 'Jim', 18, NOW());
