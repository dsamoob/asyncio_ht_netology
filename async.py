import asyncio, time

from aiohttp import ClientSession
from more_itertools import chunked
from db import Person, Session, engine, Base
from pprint import pprint
CHUNK_SIZE = 10

#
# async def get_sp(url_sp):
#     session = ClientSession()
#     response = await session.get(url_sp, ssl=False)
#     response_json = await response.json()
#     await session.close()
#     return response_json['name']
#
# async def get_vehicle(veh_url):
#     session = ClientSession()
#     response = await session.get(veh_url, ssl=False)
#     response_json = await response.json()
#     await session.close()
#     return response_json['name']
#
# async def get_starships(ship_url):
#     session = ClientSession()
#     response = await session.get(ship_url, ssl=False)
#     response_json = await response.json()
#     await session.close()
#     return response_json['name']
#
# async def get_films(film_url):
#     session = ClientSession()
#     response = await session.get(film_url, ssl=False)
#     response_json = await response.json()
#     await session.close()
#     return response_json['title']
#
# async def get_people(people_id):
#     session = ClientSession()
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
    start_get_url = time.monotonic()
    print(f'get_url start {url}')
    async with session.get(url, ssl=False) as response:
        response_json = await response.json()
        print(f'get_url finish {url} {time.monotonic()-start_get_url}')
        return response_json[name]


async def get_people(people_id, ClientSession, sep_dict):
    start_get_people = time.monotonic()
    print(f'get people start {people_id}')
    async with ClientSession as session:
        response = await session.get(f'https://swapi.dev/api/people/{people_id}', ssl=False)
        # если убрать ssl=False выдает:
        # aiohttp.client_exceptions.ClientConnectorCertificateError: Cannot connect to host swapi.dev:443 ssl:True [SSLCertVerificationError: (1, '[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed: unable to get local issuer certificate (_ssl.c:992)')]
        response_json = await response.json()

        try:
            for key, item in sep_dict.items():
                response_json[key] = ', '.join(
                    await asyncio.gather(*(get_url(url, session, item) for url in response_json[key])))
            response_json['id'] = people_id
            print(f'get_people finish {people_id} {time.monotonic() - start_get_people}')
            return response_json
        except:
            pass


async def insert_people(people_chunk):
    start_insert = time.monotonic()
    print(f'insert_people start')
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
        print(f'insert_people finish {time.monotonic() - start_insert}')


async def main():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        await conn.commit()

    sep_dict = {'species': 'name', 'films': 'title', 'starships': 'name', 'vehicles': 'name'}
    coro = (get_people(i, ClientSession(), sep_dict) for i in range(1, 84))
    for coros_chunk in chunked(coro, CHUNK_SIZE):
        asyncio.create_task(insert_people(await asyncio.gather(*coros_chunk)))
    tasks = set(asyncio.all_tasks()) - {asyncio.current_task()}
    for task in tasks:
        await task



start = time.monotonic()
asyncio.run(main())
print(f'total time spent: {time.monotonic() - start}')
