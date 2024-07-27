""" 
Read some important_data and do an important aggregation. Use concurrency like a big boi.

Store the file as a xlsx_task using xslxwriter API in a bytea column so we don't have to deal 
with object storage (or maybe we should?)

| |

"""

from pprint import pprint

import anyio
from anyio.streams import memory
from aiodal import dal
import anyio.to_thread
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import create_async_engine

import os
import asyncpg

import threading
from typing import Any

import xlsxwriter.worksheet
import xlsxwriter

import logging

logger = logging.getLogger(__name__)

URL = "postgresql+asyncpg://postgres:postgres@localhost:5454/bulkydb"


sheet_query = """ 
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
    limit 3000;
"""


async def setup() -> dal.DataAccessLayer:

    db = dal.DataAccessLayer()

    engine = create_async_engine(URL, pool_size=10, max_overflow=5, pool_timeout=60)
    meta = sa.MetaData()
    await db.reflect(engine, meta)

    return db


_Record = tuple[Any, ...]

from dataclasses import dataclass
import uuid


class WorksheetProxy:

    col_names = ["Data Id", "Value Name", "Value"]
    _lock = threading.Lock()

    def __init__(self, sheet: xlsxwriter.worksheet.Worksheet):
        self.sheet = sheet
        self._current_row = 1

    def write_header(self, *formats: Any):
        cols = self.__class__.col_names
        for i in range(len(cols)):
            self.sheet.write(0, i, cols[i], *formats)

    def write(self, record: _Record):
        with self.__class__._lock:
            for i in range(len(record)):
                if isinstance(record[i], uuid.UUID):
                    r = str(record[i])
                else:
                    r = record[i]
                self.sheet.write(self._current_row, i, r)
            self._current_row += 1


@dataclass
class Context:
    sheet: WorksheetProxy
    data: _Record


def x_writer_thread(ctx: Context):
    ctx.sheet.write(ctx.data)


async def x_writer(
    from_query: memory.MemoryObjectReceiveStream[Context],
):

    async with from_query:
        async for ctx in from_query:
            # await anyio.sleep(0.001)
            await anyio.to_thread.run_sync(x_writer_thread, ctx)  # type: ignore


async def query(
    db: dal.DataAccessLayer,
    q: str,
    sheet: WorksheetProxy,
    to_writer: memory.MemoryObjectSendStream[Context],
):

    async with to_writer:
        async with dal.transaction(db) as transaction:
            result = await transaction.conn.stream(
                sa.text(q), execution_options={"stream_results": True}
            )
            async for res in result:
                await to_writer.send(Context(sheet, res._tuple()))


from zipfile import ZipFile, ZIP_DEFLATED


def x_zipper_thread(zip_filename: str, x_filename: str):
    with ZipFile(zip_filename, "w", compression=ZIP_DEFLATED, compresslevel=5) as z:
        z.write(x_filename)


async def zip_file(x_filename: str, to_minio: memory.MemoryObjectSendStream[str]):
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


async def task(db: dal.DataAccessLayer, mio: minio.Minio):
    """
    * setup db
    * create a worker pool + channels ( 1 worker per sheet)
    * create writer pool
        * most offload to threadpool executor (xlsxwriter is blocking API)
        * writes need to be threadsafe since we are writing all data into one notebook

    Ideally this is launched as an asynchronous task somehow directly from fastapi ...
    https://fastapi.tiangolo.com/tutorial/background-tasks/#dependency-injection


    """
    n = 5  # n sheets
    # this should have happened a priori but for the demo in a different req res cycle
    # but for now it happens here to simplfy
    task_id = await create_task(db)
    print(f"starting: {task_id}")

    # this would actually need to be first and happen in a seperate thread ...
    book = xlsxwriter.Workbook(
        f"check-{str(task_id)}.xlsx", options={"constant_memory": True}
    )
    bold = book.add_format({"bold": True})

    sheets = []
    for i in range(n):
        sheets.append((WorksheetProxy(book.add_worksheet(str(i))), sheet_query))

    for w, _ in sheets:
        w.write_header(bold)

    to_writer, from_query = anyio.create_memory_object_stream[Context]()

    try:

        print(f"writing xslx: {task_id}")
        async with anyio.create_task_group() as tg:
            for s, q in sheets:
                tg.start_soon(query, db, q, s, to_writer.clone())  # type: ignore
                tg.start_soon(x_writer, from_query.clone())  # type: ignore

            await to_writer.aclose()
            await from_query.aclose()

        book.close()

        to_minio, from_zip_file = anyio.create_memory_object_stream[str]()

        print(f"zip + write to minio: {task_id}")
        async with anyio.create_task_group() as tg:
            tg.start_soon(zip_file, book.filename, to_minio)  # type: ignore
            tg.start_soon(minio_writer, mio, task_id, from_zip_file)  # type: ignore
        # raise Exception("boo")

        os.remove(f"check-{str(task_id)}.xlsx")
        os.remove(f"check-{str(task_id)}.zip")

        await success(db, task_id)
    except Exception as err:
        conn = await db.engine.connect()
        await conn.execute(
            sa.text(
                f"update public.xlsx_task set task_status = 'FAILED' where task_id = '{task_id}'"
            )
        )
        await conn.commit()
        logger.exception(err)
        await conn.close()

    # XXX now stream to database table that it is written.


async def queue():

    db = await setup()  # this part would have already happend in fast api
    mio = minio_setup()

    n_concurrent = 5
    print(f"launching: {n_concurrent}")
    async with anyio.create_task_group() as tg:
        for _ in range(n_concurrent):
            tg.start_soon(task, db, mio)

    print("finished demo.")


if __name__ == "__main__":
    anyio.run(queue)  # type: ignore
