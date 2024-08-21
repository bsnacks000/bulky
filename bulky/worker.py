import asyncio
import os
import aioboto3.session
import minio
import zipfile
import pathlib
import asyncpg
import aiofiles

REDIS_URL = os.getenv("REDIS_URL", "")
assert REDIS_URL is not None

POSTGRES_URL = os.getenv("ASYNCPG_URL", "")

from arq import create_pool
from arq.connections import RedisSettings

from aiodal import dal
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import create_async_engine
import aioboto3

wdb = dal.DataAccessLayer()

from typing import Any

ArqContext = dict[Any, Any]


def minio_setup() -> minio.Minio:
    return minio.Minio(
        "minio:9000", access_key="minio", secret_key="abc123zxc123", secure=False
    )


def aio_minio_setup() -> aioboto3.Session:
    session = aioboto3.Session()
    return session


async def startup(ctx: ArqContext):
    db = dal.DataAccessLayer()
    engine = create_async_engine(POSTGRES_URL)
    await db.reflect(engine, sa.MetaData())
    ctx["db"] = db
    ctx["mio"] = minio_setup()
    ctx["aio"] = aio_minio_setup()


async def shutdown(ctx: ArqContext):
    db: dal.DataAccessLayer = ctx["db"]
    await db.engine.dispose()


import uuid


async def query(transaction: dal.TransactionManager, path: pathlib.Path):
    conn = await transaction.get_dbapi_connection()
    assert isinstance(conn, asyncpg.Connection)

    stmt = """ 
        select 
            data_id, 
            t.* 
        from public.important_data d
        cross join lateral(
            values
                (d.val_a, 'val_a'),
                (d.val_b, 'val_b'),
                (d.val_c, 'val_c'),
                (d.val_d, 'val_d')
        ) as t(value_name, value)
        order by data_id
        limit 10000
    """
    async with aiofiles.open(path, "w") as f:
        async with conn.transaction():
            _ = await conn.copy_from_query(
                stmt,
                output=f.buffer,
                format="csv",
                header=True,
                delimiter=",",
            )


async def update_state(
    transaction: dal.TransactionManager,
    task_id: uuid.UUID,
    state: str,
    response: dict[str, Any] | None = None,
):
    t = transaction.get_table("async_task")
    await transaction.execute(
        sa.update(t)
        .where(t.c.task_id == task_id)
        .values(task_status=state, response=response)
    )


def _do_zip_data(
    fpath: pathlib.Path,
    zpath: pathlib.Path,
):
    with zipfile.ZipFile(
        zpath, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=5
    ) as z:
        z.write(fpath)


async def zip_data(fpath: pathlib.Path, zpath: pathlib.Path):
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, _do_zip_data, fpath, zpath)


def _do_write_minio(mio: minio.Minio, zpath: pathlib.Path):
    bucket_name = "asynctasks"
    mio.fput_object(bucket_name, zpath.name, zpath.name)


async def write_minio(mio: minio.Minio, zpath: pathlib.Path):
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, _do_write_minio, mio, zpath)


async def job(ctx: ArqContext, task_id: uuid.UUID):
    db: dal.DataAccessLayer = ctx["db"]
    mio: minio.Minio = ctx["mio"]

    fpath = pathlib.Path(str(task_id) + ".csv")
    zpath = pathlib.Path(str(task_id) + ".zip")

    try:
        async with dal.transaction(db) as t:
            await update_state(t, task_id, "PROCESSING")

        # write the csv
        async with dal.transaction(db) as t:
            await query(t, fpath)

        # compress into zip (deflate algorithm)
        await zip_data(fpath, zpath)

        # write the file to mio
        await write_minio(mio, zpath)

        async with dal.transaction(db) as t:
            await update_state(
                t, task_id, "SUCCESS", response={"url": "file/" + zpath.name}
            )

    except Exception:
        async with dal.transaction(db) as t:
            await update_state(t, task_id, "FAILED")

    finally:
        if fpath.exists():
            os.remove(fpath)
        if zpath.exists():
            os.remove(zpath)


async def write_aio_minio(session_client, zpath: pathlib.Path):
    bucket = "asynctasks"
    print("writing file via aioboto3 for {zpath.name}")
    try:
        with zpath.open("rb") as spfp:
            await session_client.upload_fileobj(spfp, bucket, zpath.name)
    except Exception as e:
        print(f"Unable to s3 upload {zpath.name}: {e} ({type(e)})")
        raise


async def aio_job(ctx: ArqContext, task_id: uuid.UUID):
    db: dal.DataAccessLayer = ctx["db"]
    session: aioboto3.Session = ctx["aio"]

    fpath = pathlib.Path(str(task_id) + ".csv")
    zpath = pathlib.Path(str(task_id) + ".zip")

    try:
        async with dal.transaction(db) as t:
            await update_state(t, task_id, "PROCESSING")

        # write the csv
        async with dal.transaction(db) as t:
            await query(t, fpath)

        # compress into zip (deflate algorithm)
        await zip_data(fpath, zpath)

        async with session.client(
            "s3",
            endpoint_url="https://minio:9000",
            aws_access_key_id="minio",
            aws_secret_access_key="abc123zxc123",
            use_ssl=False,
            verify=False,
        ) as s3:
            await write_aio_minio(s3, zpath)

        async with dal.transaction(db) as t:
            await update_state(
                t, task_id, "SUCCESS", response={"url": "file/" + zpath.name}
            )

    except Exception:
        async with dal.transaction(db) as t:
            await update_state(t, task_id, "FAILED")

    finally:
        if fpath.exists():
            os.remove(fpath)
        if zpath.exists():
            os.remove(zpath)


settings = RedisSettings(host="redis", port=6379, max_connections=50)


class WorkerSettings:
    functions = [job, aio_job]
    on_startup = startup
    on_shutdown = shutdown
    redis_settings = settings
