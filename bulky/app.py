from fastapi import FastAPI, Depends, File, UploadFile, HTTPException
from fastapi.responses import FileResponse, StreamingResponse
from contextlib import asynccontextmanager

import asyncpg  # type: ignore
from starlette.types import Receive, Scope, Send

from sqlalchemy.ext.asyncio import create_async_engine
import sqlalchemy as sa

from typing import AsyncGenerator, Any, AsyncIterator

import os
import minio

from .worker import settings as redis_settings

from arq.connections import RedisSettings, ArqRedis, create_pool

POSTGRES_URL = os.getenv("ASYNCPG_URL", "")
ASYNCPG_DIRECT_URL = POSTGRES_URL.replace("postgresql+asyncpg://", "postgresql://")

from aiodal import dal

db = dal.DataAccessLayer()


def minio_setup() -> minio.Minio:
    return minio.Minio(
        "minio:9000", access_key="minio", secret_key="abc123zxc123", secure=False
    )


mio = minio_setup()


class ArqClient:
    def __init__(self):
        self.pool = None

    async def initialize(self, settings: RedisSettings):
        self.pool = await create_pool(settings)


arqc = ArqClient()


class Asyncpg:
    def __init__(self):
        self.pool = None

    async def initialize(self, url: str):
        self.pool = await asyncpg.create_pool(url, min_size=2, max_size=3)


apg_db = Asyncpg()


@asynccontextmanager
async def lifespan(
    app: FastAPI,
) -> AsyncGenerator[Any, Any]:

    await apg_db.initialize(ASYNCPG_DIRECT_URL)
    engine = create_async_engine(POSTGRES_URL, max_overflow=5, pool_size=5)
    metadata = sa.MetaData()
    await db.reflect(engine, metadata)
    await arqc.initialize(redis_settings)
    yield


app = FastAPI(lifespan=lifespan)


async def get_transaction() -> AsyncIterator[dal.TransactionManager]:

    async with db.engine.connect() as conn:
        transaction = dal.TransactionManager(conn, db)
        try:
            yield transaction
            await transaction.commit()
        except Exception:
            await transaction.rollback()
            raise


async def get_asyncpg_connection() -> AsyncIterator[asyncpg.Connection]:
    async with apg_db.pool.acquire() as conn:
        try:
            yield conn
        finally:
            await conn.close()


import aioboto3


# async def get_minio_session():
#     session = aioboto3.Session()
#     async with session.resource("s3") as s3:
#         yield s3


import io
import tempfile
import aiofiles

from aiofiles.threadpool.text import AsyncTextIOWrapper
from starlette.background import BackgroundTask
import typing
import tempfile


class TempFileResponse(FileResponse):

    def __init__(
        self,
        aio_wrapper: AsyncTextIOWrapper | tempfile._TemporaryFileWrapper,
        path: str | os.PathLike[str],
        status_code: int = 200,
        headers: typing.Mapping[str, str] | None = None,
        media_type: str | None = None,
        background: BackgroundTask | None = None,
        filename: str | None = None,
        stat_result: os.stat_result | None = None,
        method: str | None = None,
        content_disposition_type: str = "attachment",
    ) -> None:
        self.aio_wrapper = aio_wrapper
        super().__init__(
            path,
            status_code,
            headers,
            media_type,
            background,
            filename,
            stat_result,
            method,
            content_disposition_type,
        )

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        try:
            await super().__call__(scope, receive, send)
        finally:
            if isinstance(self.aio_wrapper, tempfile._TemporaryFileWrapper):
                self.aio_wrapper.close()
            else:
                await self.aio_wrapper.close()
            os.remove(self.aio_wrapper.name)


@app.get("/download")
async def download(conn: asyncpg.Connection = Depends(get_asyncpg_connection)):

    async with aiofiles.tempfile.NamedTemporaryFile("w", delete=False) as fp:
        await conn.copy_from_query(
            "select * from important_data limit 1000000",
            output=fp.name,
            format="csv",
            header=True,
        )
    return TempFileResponse(fp, fp.name)


import uuid


@app.post("/upload")
async def upload(
    conn: asyncpg.Connection = Depends(get_asyncpg_connection),
    f: UploadFile = File(...),
):
    async with aiofiles.tempfile.NamedTemporaryFile("wb") as fp:
        while content := await f.read(2048):  # async read chunk
            await fp.write(content)  # async write chunk
        await fp.flush()  # must call flush or we lose data

        async with conn.transaction():
            # create temp table
            tabname = "tmp_" + str(uuid.uuid4().hex[:10])
            await conn.execute(
                f""" 
                create temp table {tabname} (
                    val_a numeric, 
                    val_b numeric, 
                    val_c numeric,
                    val_d numeric,
                    val_e numeric,
                    val_f numeric,
                    val_g numeric,
                    val_h numeric
                ) on commit drop;
        """
            )
            await conn.copy_to_table(tabname, source=fp.name, format="csv", header=True)

            await conn.execute(
                f"""
                insert into important_data (val_a, val_b, val_c, val_d, val_e, val_f, val_g, val_h)
                select val_a, val_b, val_c, val_d, val_e, val_f, val_g, val_h from {tabname}
                on conflict do nothing;
        """
            )

    return "ok"


import csv


@app.post("/upload-iter")
async def upload(
    conn: asyncpg.Connection = Depends(get_asyncpg_connection),
    f: UploadFile = File(...),
):
    async with aiofiles.tempfile.NamedTemporaryFile("w") as fp:
        while content := await f.read(1024):  # async read chunk
            await fp.write(content.decode())  # async write chunk
        await fp.flush()  # must call flush or we lose data

        async with conn.transaction():
            # read line by line
            # can be used to perform validation but i'm not doing it here... see aiocsv for example parser
            # https://github.com/MKuranowski/aiocsv/blob/master/aiocsv/readers.py#L16
            async with aiofiles.open(fp.name, "r") as ff:
                count = 0
                async for line in ff:
                    if count == 0:
                        count += 1
                        continue
                    values = line.replace("\n", "")
                    await conn.execute(
                        f"insert into important_data (val_a, val_b, val_c, val_d, val_e, val_f, val_g, val_h) values ({values})"
                    )
    return "ok"


import pydantic
from typing import Annotated


class ImportantData(pydantic.BaseModel):
    val_a: float
    val_b: float
    val_c: float
    val_d: float
    val_e: float
    val_f: float
    val_g: float
    val_h: float


@app.post("/upload-iter-validate")
async def upload(
    transaction: dal.TransactionManager = Depends(get_transaction),
    f: UploadFile = File(...),
):
    async with aiofiles.tempfile.NamedTemporaryFile("w") as fp:
        while content := await f.read(1024):  # async read chunk
            await fp.write(content.decode())  # async write chunk
        await fp.flush()  # must call flush or we lose data

        async with aiofiles.open(fp.name, "r") as ff:
            header: list[str] = []
            async for line in ff:
                if len(header) == 0:
                    header = line.replace("\n", "").split(",")
                    continue
                cells = line.replace("\n", "").split(",")
                obj = dict(zip(header, cells))
                data = ImportantData.model_validate(obj).model_dump()
                t = transaction.get_table("important_data")

                stmt = sa.insert(t).values(**data)
                await transaction.execute(stmt)
    return "ok"

    # async with conn.transaction():
    #     # read line by line
    #     # can be used to perform validation but i'm not doing it here... see aiocsv for example parser
    #     # https://github.com/MKuranowski/aiocsv/blob/master/aiocsv/readers.py#L16
    #     async with aiofiles.open(fp.name, "r") as ff:
    #         count = 0
    #         async for line in ff:
    #             if count == 0:
    #                 count += 1
    #                 continue
    #             values = line.replace("\n", "")
    #             await conn.execute(
    #                 f"insert into important_data (val_a, val_b, val_c, val_d, val_e, val_f, val_g, val_h) values ({values})"
    #             )
    # return "ok"


class TaskResponse(pydantic.BaseModel):
    task_id: uuid.UUID
    task_status: str
    response: dict[str, Any] | None = None


async def get_arq_redis() -> AsyncIterator[ArqRedis]:
    assert arqc.pool
    yield arqc.pool


@app.post("/file", status_code=202)
async def post_file(
    transaction: dal.TransactionManager = Depends(get_transaction),
    arqr: ArqRedis = Depends(get_arq_redis),
) -> TaskResponse:

    t = transaction.get_table("async_task")
    id_ = uuid.uuid4()
    stmt = (
        sa.insert(t).values(task_id=id_, task_status="PENDING").returning(t.c.task_id)
    )
    result = await transaction.execute(stmt)
    task_id = result.scalar_one()

    # launch task here
    await arqr.enqueue_job("job", task_id, _job_id=str(id_))

    return TaskResponse(task_id=task_id, task_status="PENDING")


# poll
@app.get("/file/task/{task_id}")
async def get_task(
    task_id: str, transaction: dal.TransactionManager = Depends(get_transaction)
) -> TaskResponse:
    t = transaction.get_table("async_task")
    stmt = sa.select(t).where(t.c.task_id == task_id)
    result = await transaction.execute(stmt)
    r = result.one_or_none()
    if not r:
        raise HTTPException(status_code=404)

    return TaskResponse(
        task_id=r.task_id, task_status=r.task_status, response=r.response
    )


import tempfile
import io

# need dependency injection
# really should be able to stream directly out aioboto3 ... the
# with tempfile.NamedTemporaryFile("wb", delete=False) as tmp:
#     data = mio.fget_object(
#         bucket_name="asynctasks",
#         object_name=task_id + ".zip",
#         file_path=tmp.name,
#     )
#     return TempFileResponse(tmp, path=tmp.name)


@app.get("/file/{task_id}")
def get_file(task_id: str) -> StreamingResponse:

    f = mio.get_object(bucket_name="asynctasks", object_name=task_id + ".zip")
    return StreamingResponse(f.stream())


# b9bb9c09-c918-416f-8be8-333f687a635b
@app.get("/aiofile/{task_id}")
async def get_file_aio(task_id: str):
    session = aioboto3.Session()
    async with session.client(
        "s3",
        endpoint_url="https://minio:9000",
        aws_access_key_id="minio",
        aws_secret_access_key="abc123zxc123",
        use_ssl=False,
        verify=False,
    ) as s3:
        print("hello")
        s3_ob = await s3.get_object(Bucket="asynctasks", Key=task_id + ".zip")
        stream = s3_ob["Body"]
        # print(stream)
        # print(type(stream))
        # print(vars(stream))
        # print(type(stream.content))
        # print(vars(stream.content))
        # print("now streaming")

        # StreamingResponse
        async def _stream_file_data():
            while stream.content._size:
                yield await stream.read(10)

        return StreamingResponse(_stream_file_data())
