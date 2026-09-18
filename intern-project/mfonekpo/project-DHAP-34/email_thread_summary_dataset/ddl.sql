CREATE TABLE IF NOT EXISTS public.email_thread_details (
    record_id CHAR(64) PRIMARY KEY,
    thread_id INTEGER NOT NULL,
    subject VARCHAR(255),
    "timestamp" TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    "from" VARCHAR(255) NOT NULL,
    "to" TEXT NOT NULL,
    body TEXT
);

CREATE INDEX IF NOT EXISTS email_thread_details_thread_timestamp_idx
    ON public.email_thread_details (thread_id, "timestamp");
