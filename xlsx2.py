from __future__ import annotations

import anyio.to_thread
import pandas as pd
import anyio
import os
from anyio.streams import memory
import uuid

from typing import Any
import aiofiles
import asyncpg
from aiodal import dal
from sqlalchemy.ext.asyncio import create_async_engine
import sqlalchemy as sa

URL = "postgresql+asyncpg://postgres:postgres@localhost:5454/bulkydb"


async def setup() -> dal.DataAccessLayer:

    db = dal.DataAccessLayer()

    engine = create_async_engine(URL, pool_size=10, max_overflow=5)
    meta = sa.MetaData()
    await db.reflect(engine, meta)

    return db


from zipfile import ZipFile, ZIP_DEFLATED


def x_zipper_thread(zip_filename: str, x_filename: str):
    with ZipFile(zip_filename, "w", compression=ZIP_DEFLATED, compresslevel=5) as z:
        z.write(x_filename)


async def zip_writer(x_filename: str, to_minio: memory.MemoryObjectSendStream[str]):
    async with to_minio:
        zip_filename = x_filename.split(".")[0] + ".zip"
        await anyio.to_thread.run_sync(x_zipper_thread, zip_filename, x_filename)  # type: ignore
        await to_minio.send(zip_filename)


import pathlib
import minio


def minio_setup() -> minio.Minio:
    return minio.Minio(
        "localhost:9000", access_key="minio", secret_key="abc123zxc123", secure=False
    )


def minio_thread(client: minio.Minio, zip_filename: str, task_id: uuid.UUID):
    bucket_name = "xlsxstorage"
    dest = str(task_id) + ".zip"
    client.fput_object(bucket_name, dest, zip_filename)


async def minio_writer(
    client: minio.Minio,
    task_id: uuid.UUID,
    from_zip_file: memory.MemoryObjectReceiveStream[str],
):
    async with from_zip_file:
        async for fname in from_zip_file:
            await anyio.to_thread.run_sync(minio_thread, client, fname, task_id)  # type: ignore


def write_csv_thread(csv_fnames: list[str], x_name: str):
    with pd.ExcelWriter(path=x_name, mode="w", engine="xlsxwriter") as writer:
        for csv_ in csv_fnames:
            df = pd.read_csv(csv_, low_memory=True)
            df.to_excel(writer, sheet_name=csv_.split("___")[0])


async def write_csv(csv_fnames: list[str], x_name: str):
    await anyio.to_thread.run_sync(write_csv_thread, csv_fnames, x_name)


async def fetch(db: dal.DataAccessLayer, fname: str):
    async with dal.transaction(db) as transaction:
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
    limit 3000
"""

        async with aiofiles.open(fname, "w") as f:
            async with conn.transaction():
                result = await conn.copy_from_query(
                    stmt,
                    output=f.buffer,
                    format="csv",
                    header=True,
                    delimiter=",",
                )


# synchonous ... simulate that the req-res cycle made this task already under a difference transaction.
async def create_task(db: dal.DataAccessLayer) -> uuid.UUID:
    async with dal.transaction(db) as transaction:
        t = transaction.get_table("xlsx_task")
        id_ = uuid.uuid4()
        stmt = (
            sa.insert(t)
            .values(task_id=id_, task_status="PENDING")
            .returning(t.c.task_id)
        )
        result = await transaction.execute(stmt)
        task_id = result.scalar_one()
        return task_id


async def success(db: dal.DataAccessLayer, task_id: uuid.UUID):
    async with dal.transaction(db) as transaction:
        t = transaction.get_table("xlsx_task")
        await transaction.execute(
            sa.update(t).where(t.c.task_id == task_id).values(task_status="SUCCESS")
        )
    print(f"Finished: {task_id}")


async def task(db: dal.DataAccessLayer, mio: minio.Minio) -> None:
    n = 5
    fnames = []

    task_id = await create_task(db)
    print(f"starting: {task_id}")
    x_name = "check-" + str(task_id) + ".xlsx"

    try:
        async with anyio.create_task_group() as tg:
            for i in range(n):
                fname = "check-" + str(i) + "___" + str(task_id) + ".csv"
                fnames.append(fname)
                tg.start_soon(fetch, db, fname)  # type: ignore

        async with anyio.create_task_group() as tg:
            tg.start_soon(write_csv, fnames, x_name)  # type: ignore

        to_minio, from_zip_file = anyio.create_memory_object_stream[str]()

        async with anyio.create_task_group() as tg:
            tg.start_soon(zip_writer, x_name, to_minio)  # type: ignore
            tg.start_soon(minio_writer, mio, task_id, from_zip_file)  # type: ignore

        os.remove(f"check-{str(task_id)}.zip")

        await success(db, task_id)
    finally:

        os.remove(x_name)
        for name in fnames:
            os.remove(name)


import random


async def queue():

    db = await setup()  # this part would have already happend in fast api
    mio = minio_setup()

    n_concurrent = 20
    print(f"launching: {n_concurrent}")
    async with anyio.create_task_group() as tg:
        for _ in range(n_concurrent):
            await anyio.sleep(random.random())
            tg.start_soon(task, db, mio)

    print("finished demo.")


if __name__ == "__main__":
    anyio.run(queue)
