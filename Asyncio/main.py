import asyncio
import datetime
import requests
from aiohttp import ClientSession
from more_itertools import chunked
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from models import SwapiPeople, Base

CHUNK_SIZE = 10
URL_PERSON = 'https://swapi.dev/api/people/'
PEOPLE_NUMBER = requests.get(URL_PERSON).json()['count']

PG_DSN = 'postgresql+asyncpg://user:1234@127.0.0.1:5431/netology'
engine = create_async_engine(PG_DSN)
Session = sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)


async def chunked_async(async_iter, size):
    buffer = []
    while True:
        try:
            item = await async_iter.__anext__()
        except StopAsyncIteration:
            if buffer:
                yield buffer
            break
        buffer.append(item)
        if len(buffer) == size:
            yield buffer
            buffer = []


async def get_item(url: str, item_id: str, session: ClientSession):
    async with session.get(f'{url}{item_id}') as response:
        return await response.json()


async def item_final(item: dict, field: str, session: ClientSession):
    coroutines = [get_item(url=url, item_id='', session=session) for url in item[field]]
    result = await asyncio.gather(*coroutines)
    if field == 'films':
        name_field = 'title'
    else:
        name_field = 'name'
    item[field] = ', '.join([raw[name_field] for raw in result]).strip(', ')
    return item


async def get_people():
    async with ClientSession() as session:
        for chunk in chunked(range(1, PEOPLE_NUMBER + 1), CHUNK_SIZE):
            coroutines_people = [get_item(url=URL_PERSON, item_id=str(i), session=session) for i in chunk]
            result = await asyncio.gather(*coroutines_people)
            for item in result:
                try:
                    item_films = await item_final(item=item, field='films', session=session)
                    item_species = await item_final(item=item_films, field='species', session=session)
                    item_starships = await item_final(item=item_species, field='starships', session=session)
                    item_vehicles = await item_final(item=item_starships, field='vehicles', session=session)
                    item_vehicles['id'] = int(item['url'].split('/')[-2])
                    yield item_vehicles
                except KeyError:
                    pass


async def insert_people(people_chunk):
    async with Session() as session:
        session.add_all([SwapiPeople(id=item['id'],
                                     birth_year=item['birth_year'],
                                     eye_color=item['eye_color'],
                                     films=item['films'],
                                     gender=item['gender'],
                                     hair_color=item['hair_color'],
                                     height=item['height'],
                                     homeworld=item['homeworld'],
                                     mass=item['mass'],
                                     name=item['name'],
                                     skin_color=item['skin_color'],
                                     species=item['species'],
                                     starships=item['starships'],
                                     vehicles=item['vehicles'],
                                     ) for item in people_chunk])
        await session.commit()


async def main():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        await conn.commit()

    async for chunk in chunked_async(get_people(), CHUNK_SIZE):
        asyncio.create_task(insert_people(chunk))

    tasks = set(asyncio.all_tasks()) - {asyncio.current_task()}
    for task in tasks:
        await task


start = datetime.datetime.now()
asyncio.run(main())
print(datetime.datetime.now() - start)
