-- Create "async_task" table
CREATE TABLE "public"."async_task" (
  "task_id" uuid NOT NULL DEFAULT gen_random_uuid(),
  "task_status" character varying NULL,
  "request" jsonb NULL,
  "etag_version" uuid NOT NULL DEFAULT gen_random_uuid(),
  "last_updated" timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY ("task_id")
);
