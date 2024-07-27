

create table public.important_data (
    data_id uuid primary key not null default gen_random_uuid(), 
    val_a numeric default 42.0,
    val_b numeric default 43.0,
    val_c numeric default 44.0,
    val_d numeric default 45.0,
    val_e numeric default 46.0,
    val_f numeric default 47.0,
    val_g numeric default 48.0,
    val_h numeric default 49.0,
    val_i numeric default 50.0,
    val_j numeric default 51.0,
    val_k numeric default 52.0,
    val_l numeric default 53.0,
    val_m numeric default 54.0,
    val_n numeric default 55.0,
    val_o numeric default 56.0,
    val_p numeric default 57.0,
    val_q numeric default 58.0,
    etag_version uuid not null default gen_random_uuid(), 
    last_updated timestamptz not null default now()
);


create table public.xlsx_task(
    task_id uuid primary key not null default gen_random_uuid(), 
    task_status varchar,
    request jsonb, 
    xslx bytea, 
    etag_version uuid not null default gen_random_uuid(), 
    last_updated timestamptz not null default now()
);

