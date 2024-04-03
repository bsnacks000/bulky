-- Create "important_data" table
CREATE TABLE "public"."important_data" (
  "data_id" uuid NOT NULL DEFAULT gen_random_uuid(),
  "val_a" numeric NULL DEFAULT 42.0,
  "val_b" numeric NULL DEFAULT 43.0,
  "val_c" numeric NULL DEFAULT 44.0,
  "val_d" numeric NULL DEFAULT 45.0,
  "val_e" numeric NULL DEFAULT 46.0,
  "val_f" numeric NULL DEFAULT 47.0,
  "val_g" numeric NULL DEFAULT 48.0,
  "val_h" numeric NULL DEFAULT 49.0,
  "val_i" numeric NULL DEFAULT 50.0,
  "val_j" numeric NULL DEFAULT 51.0,
  "val_k" numeric NULL DEFAULT 52.0,
  "val_l" numeric NULL DEFAULT 53.0,
  "val_m" numeric NULL DEFAULT 54.0,
  "val_n" numeric NULL DEFAULT 55.0,
  "val_o" numeric NULL DEFAULT 56.0,
  "val_p" numeric NULL DEFAULT 57.0,
  "val_q" numeric NULL DEFAULT 58.0,
  "etag_version" uuid NOT NULL DEFAULT gen_random_uuid(),
  "last_updated" timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY ("data_id")
);
