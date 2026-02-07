-- Debezium CDC connector requires the wal_level to be set to logical.
-- ALTER SYSTEM SET wal_level = logical;


---------------------------- Debezium Owned Tables ----------------------------
-- The following SQL script initializes the database for use with Debezium CDC connectors.
-- It creates the necessary tables, indexes, and publications for capturing changes in tables owned by Debezium.

-- Create a role for Debezium to use for CDC
-- The 'debezium' user will have LOGIN and REPLICATION privileges.
-- LOGIN is automatically granted by the CREATE USER command.
CREATE USER debezium WITH ENCRYPTED PASSWORD 'debezium';
ALTER USER debezium REPLICATION;

CREATE TABLE IF NOT EXISTS file_operations (
    task_id UUID NOT NULL,
    bucket VARCHAR(256) NOT NULL,
    object TEXT NOT NULL,
    content_type TEXT NOT NULL,
    upload_type VARCHAR(32) NOT NULL,
    upload_action VARCHAR(32) NOT NULL,
    info TEXT NOT NULL,
    PRIMARY KEY (task_id)
);

-- Create indexes for files__library and files__private tables to enable debezium to collect DELETE events appropriately
-- The default REPLICA IDENTITY is DEFAULT, which means it will replicate only the primary keyes; 
-- However, as downstream consumers need to know the bucket, info, and upload_action, they should be included
-- as well, and for this we create a unique index which will serve as the REPLICA IDENTITY.
CREATE UNIQUE INDEX IF NOT EXISTS idx_file_operations_dbz ON public.file_operations (task_id, object, bucket, content_type, upload_action, info);
ALTER TABLE public.file_operations REPLICA IDENTITY USING INDEX idx_file_operations_dbz;

-- Create the necessary publications for Debezium CDC connectors
CREATE PUBLICATION files_operation_publication FOR TABLE public.file_operations;

-- Allow Debezium connectors to modify above publications
-- This is relevant when configuring Debezium CDC connectors with "publication.autocreate.mode": "filtered"
GRANT SELECT ON public.file_operations TO debezium;

-- Grant ownership of the tables to debezium user
CREATE ROLE replication_group;
GRANT replication_group TO postgres;
GRANT replication_group TO debezium;
ALTER TABLE public.file_operations OWNER TO replication_group;
---------------------------- End of Debezium Owned Tables ----------------------------


---------------------------- Celery Owned Tables ----------------------------
-- The following SQL script initializes the database for use with Celery.
-- It creates the necessary tables for task management and result storage.

-- change this to celery
CREATE USER celery WITH ENCRYPTED PASSWORD 'celery';
ALTER USER celery REPLICATION;

CREATE TABLE IF NOT EXISTS apollo_celery_taskmeta
(
    id INTEGER NOT NULL,
    task_id VARCHAR(155) COLLATE pg_catalog."default",
    status VARCHAR(50) COLLATE pg_catalog."default",
    result TEXT COLLATE pg_catalog."default",
    date_done TIMESTAMP WITHOUT TIME ZONE,
    traceback TEXT COLLATE pg_catalog."default",
    name VARCHAR(155) COLLATE pg_catalog."default",
    args BYTEA,
    kwargs BYTEA,
    worker VARCHAR(155) COLLATE pg_catalog."default",
    retries INTEGER,
    queue VARCHAR(155) COLLATE pg_catalog."default",
    CONSTRAINT apollo_celery_taskmeta_pkey PRIMARY KEY (id),
    CONSTRAINT apollo_celery_taskmeta_task_id_key UNIQUE (task_id)
);

ALTER TABLE IF EXISTS public.apollo_celery_taskmeta OWNER to celery;

CREATE SEQUENCE IF NOT EXISTS public.apollo_task_id_sequence
    INCREMENT 1
    START 1
    MINVALUE 1
    MAXVALUE 9223372036854775807
    CACHE 1;

ALTER SEQUENCE public.apollo_task_id_sequence OWNER TO celery;


-- CDC Setup for celery_taskmeta table, used by debezium user
ALTER TABLE public.apollo_celery_taskmeta REPLICA IDENTITY FULL;
CREATE PUBLICATION celery_results_publication FOR TABLE public.apollo_celery_taskmeta;

GRANT SELECT ON public.apollo_celery_taskmeta TO debezium;

-- Grant ownership of the tables to debezium user
CREATE ROLE celery_replication_group;
GRANT celery_replication_group TO postgres;
GRANT celery_replication_group TO debezium;
GRANT celery_replication_group TO celery;
ALTER TABLE public.apollo_celery_taskmeta OWNER TO celery_replication_group;

-- celery_tasksetmeta table setup
CREATE TABLE IF NOT EXISTS public.apollo_celery_tasksetmeta
(
    id integer NOT NULL,
    taskset_id character varying(155) COLLATE pg_catalog."default",
    result text COLLATE pg_catalog."default",
    date_done timestamp without time zone,
    CONSTRAINT apollo_celery_tasksetmeta_pkey PRIMARY KEY (id),
    CONSTRAINT apollo_celery_tasksetmeta_taskset_id_key UNIQUE (taskset_id)
);

ALTER TABLE IF EXISTS public.apollo_celery_tasksetmeta OWNER to celery;

CREATE SEQUENCE IF NOT EXISTS public.apollo_taskset_id_sequence
    INCREMENT 1
    START 1
    MINVALUE 1
    MAXVALUE 9223372036854775807
    CACHE 1;

ALTER SEQUENCE public.apollo_taskset_id_sequence OWNER TO celery;

-- Create an index for celery_taskmeta table to enable debezium to collect CDC events
-- Only the primary key is replicated by default, but we need to replicate task_id, result, status, and date_done
--CREATE UNIQUE INDEX IF NOT EXISTS idx_celery_tasks ON public.celery_taskmeta (id, task_id, result, status, date_done);
--ALTER TABLE public.celery_taskmeta REPLICA IDENTITY USING INDEX idx_celery_tasks;


-- Create the necessary publications for Debezium CDC connectors
--CREATE PUBLICATION celery_taskmeta_publication FOR TABLE public.celery_taskmeta;
--GRANT SELECT ON public.celery_taskmeta TO debezium;
--GRANT SELECT ON public.celery_taskmeta TO celery;

--CREATE ROLE celery_replication_group;
--GRANT celery_replication_group TO postgres;
--GRANT celery_replication_group TO debezium;
--GRANT celery_replication_group TO celery;
--ALTER TABLE public.celery_taskmeta OWNER TO celery_replication_group;


-- Tables for private users, chats and files
-- Every user has multiple chats
-- Every chat has multiple files attached to it
-- Deletion of a user cascades to chats
-- Deletion of a chat cascades to files
CREATE TABLE IF NOT EXISTS public.user
(
    name character varying(100) NOT NULL,
    id uuid NOT NULL,
    email character varying(320) NOT NULL,
    hashed_password character varying(1024) NOT NULL,
    is_active boolean NOT NULL,
    is_superuser boolean NOT NULL,
    is_verified boolean NOT NULL,
    CONSTRAINT user_pkey PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS public.chats
(
    id UUID NOT NULL,
    user_id UUID NOT NULL,
    created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW(),
    PRIMARY KEY (id),
    FOREIGN KEY (user_id) REFERENCES public.user(id) ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE TABLE IF NOT EXISTS public.messages
(
    id INTEGER GENERATED ALWAYS AS IDENTITY,
    chat_id UUID NOT NULL,
    message TEXT NOT NULL,
    created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW(),
    PRIMARY KEY (id),
    FOREIGN KEY (chat_id) REFERENCES public.chats(id) ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE TABLE IF NOT EXISTS public.files
(
    id UUID NOT NULL,
    chat_id UUID NOT NULL,
    user_id UUID NOT NULL,
    filename TEXT NOT NULL,
    content_type VARCHAR(2048) NOT NULL,
    __marked_for_delete BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY (id, chat_id, user_id),
    FOREIGN KEY (user_id) REFERENCES public.user(id) ON DELETE CASCADE ON UPDATE CASCADE,
    FOREIGN KEY (chat_id) REFERENCES public.chats(id) ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_files_dbz ON public.files (id, chat_id, user_id, __marked_for_delete);
ALTER TABLE public.files REPLICA IDENTITY USING INDEX idx_files_dbz;

-- Create the necessary publications for Debezium CDC connectors
CREATE PUBLICATION files_publication FOR TABLE public.files;

-- Allow Debezium connectors to modify above publications
-- This is relevant when configuring Debezium CDC connectors with "publication.autocreate.mode": "filtered"
GRANT SELECT ON public.files TO debezium;

-- Grant ownership of the tables to debezium user
CREATE ROLE files_replication_group;
GRANT files_replication_group TO postgres;
GRANT files_replication_group TO debezium;
ALTER TABLE public.files OWNER TO files_replication_group;

-- library files table
CREATE TABLE IF NOT EXISTS public.catalog
(
    namespace TEXT NOT NULL,
    path TEXT NOT NULL,
    filename TEXT NOT NULL,
    date_done timestamp without time zone NOT NULL,
    content_type VARCHAR(2048) NOT NULL,
    PRIMARY KEY (namespace, path)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_catalog_dbz ON public.catalog (namespace, filename, path, date_done);
ALTER TABLE public.catalog REPLICA IDENTITY USING INDEX idx_catalog_dbz;

-- Create the necessary publications for Debezium CDC connectors
CREATE PUBLICATION catalog_publication FOR TABLE public.catalog;

-- Allow Debezium connectors to modify above publications
-- This is relevant when configuring Debezium CDC connectors with "publication.autocreate.mode": "filtered"
GRANT SELECT ON public.catalog TO debezium;

-- Grant ownership of the tables to debezium user
CREATE ROLE catalog_replication_group;
GRANT catalog_replication_group TO postgres;
GRANT catalog_replication_group TO debezium;
ALTER TABLE public.catalog OWNER TO catalog_replication_group;