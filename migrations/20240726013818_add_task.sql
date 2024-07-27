-- Create "xlsx_task" table
CREATE TABLE "public"."xlsx_task" (
  "task_id" uuid NOT NULL DEFAULT gen_random_uuid(),
  "task_status" character varying NULL,
  "request" jsonb NULL,
  "xslx" bytea NULL,
  "etag_version" uuid NOT NULL DEFAULT gen_random_uuid(),
  "last_updated" timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY ("task_id")
);
