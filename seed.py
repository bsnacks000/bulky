import asyncio
import asyncpg


async def main():
    conn = await asyncpg.connect(
        "postgresql://postgres:postgres@localhost:5454/bulkydb"
    )

    await conn.execute(
        f"""
        insert into important_data (val_a)
        select 42.0
        from generate_series(1,1500000)
        """
    )

    await conn.close()


asyncio.run(main())
