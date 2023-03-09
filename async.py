import asyncio, time
from aiohttp import ClientSession
from more_itertools import chunked
from db import Person, Session, engine, Base
from pprint import pprint
CHUNK_SIZE = 10


# async def get_sp(url_sp):
#     session = aiohttp.ClientSession()
#     response = await session.get(url_sp, ssl=False)
#     response_json = await response.json()
#     await session.close()
#     return response_json['name']
#
# async def get_vehicle(veh_url):
#     session = aiohttp.ClientSession()
#     response = await session.get(veh_url, ssl=False)
#     response_json = await response.json()
#     await session.close()
#     return response_json['name']
#
# async def get_starships(ship_url):
#     session = aiohttp.ClientSession()
#     response = await session.get(ship_url, ssl=False)
#     response_json = await response.json()
#     await session.close()
#     return response_json['name']
#
# async def get_films(film_url):
#     session = aiohttp.ClientSession()
#     response = await session.get(film_url, ssl=False)
#     response_json = await response.json()
#     await session.close()
#     return response_json['title']
#
# async def get_people(people_id):
#     session = aiohttp.ClientSession()
#     response = await session.get(f'https://swapi.dev/api/people/{people_id}', ssl=False)
#     response_json = await response.json()
#     await session.close()
#     try:
#         response_json['species'] = await asyncio.gather(*[get_sp(i) for i in response_json['species']])
#         response_json['films'] = await asyncio.gather(*[get_films(i) for i in response_json['films']])
#         response_json['starships'] = await asyncio.gather(*[get_starships(i) for i in response_json['starships']])
#         response_json['vehicles'] = await asyncio.gather(*[get_vehicle(i) for i in response_json['vehicles']])
#         return response_json
#     except:
#         pass
#
# async def main():
#     coro = [get_people(i) for i in range(1, 84)]
#     result = await asyncio.gather(*coro)
#     for i in result:
#         print(i)
#
# start = time.monotonic()
# asyncio.run(main())
# print(time.monotonic()-start)


async def get_url(url, session, name):
    async with session.get(url, ssl=False) as response:
        response_json = await response.json()
        return response_json[name]


async def get_people(people_id, ClientSession, sep_dict):
    async with ClientSession as session:
        response = await session.get(f'https://swapi.dev/api/people/{people_id}', ssl=False)
        response_json = await response.json()
        try:
            for key, item in sep_dict.items():
                response_json[key] = ', '.join(
                    await asyncio.gather(*(get_url(url, session, item) for url in response_json[key])))
            response_json['id'] = people_id
            return response_json
        except:
            pass


async def insert_people(people_chunk):
    pprint(people_chunk)
    persons = [Person(pers_id=int(i['id']),
                      birth_year=i['birth_year'],
                      films=i['films'],
                      gender=i['gender'],
                      hair_color=i['hair_color'],
                      height=i['height'],
                      homeworld=i['homeworld'],
                      mass=i['mass'],
                      name=i['name'],
                      species=i['species'],
                      starships=i['starships'],
                      vehicles=i['vehicles']
                      ) for i in people_chunk if i]
    async with Session() as session:
        session.add_all(persons)
        await session.commit()


async def main():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        await conn.commit()

    sep_dict = {'species': 'name', 'films': 'title', 'starships': 'name', 'vehicles': 'name'}
    coro = (get_people(i, ClientSession(), sep_dict) for i in range(1, 84))
    for coros_chunk in chunked(coro, CHUNK_SIZE):
        result = await asyncio.gather(*coros_chunk)
        await insert_people(result)



start = time.monotonic()
asyncio.run(main())
print(time.monotonic() - start)
