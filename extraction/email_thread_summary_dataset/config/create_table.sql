-- Create table for email thread details
CREATE TABLE public.email_thread_details (
    thread_id INTEGER NOT NULL,
    subject VARCHAR(255) NULL,
    "timestamp" TIMESTAMPTZ NOT NULL,
    "from" VARCHAR(255) NOT NULL,
    "to" TEXT NOT NULL, -- to is stored as text to support multiple or long recipients lists
    body TEXT NULL,
    
-- Defining the Composite Primary Key here
CONSTRAINT pk_email_thread_details PRIMARY KEY (thread_id, "timestamp")
);