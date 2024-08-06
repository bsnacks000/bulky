import asyncio

from arq.connections import RedisSettings
from arq import create_pool

settings = RedisSettings(host="localhost", port=6380)


async def main():
    redis = await create_pool(settings)
    for i in range(10):
        await redis.enqueue_job("query")


if __name__ == "__main__":
    asyncio.run(main())
