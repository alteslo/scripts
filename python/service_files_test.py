import aiohttp
import asyncio
from tqdm import tqdm
import os

# Конфигурация
BASE_URL = "http://127.0.0.1:8081/api/v1/upload_file_put_object_stream"
QUERY_PARAMS = {
    "bucket_name": "iset",
    "system_entity": "work_object",
    "row_entity_id": "6538bbe5-8494-4948-8ed2-bcc93b27bf6d",
    "type_attachment": "json"
}
HEADERS = {
    "accept": "application/json",
    "Token": "12345"  # токен из curl
}

# Файлы для загрузки
FILE_PATHS = [
    "python\Мезмай 2016-20240912T123457Z-001.zip",
    "python\Мезмай 2016-20240912T123457Z-002.zip",
    "python\Мезмай 2016-20240912T123457Z-003.zip"
]


async def upload_file(session, file_path):
    file_size = os.path.getsize(file_path)
    filename = os.path.basename(file_path)

    with tqdm(
        total=file_size,
        unit="B",
        unit_scale=True,
        desc=f"Загрузка {filename}",
        position=FILE_PATHS.index(file_path),
        leave=True
    ) as pbar:

        with open(file_path, "rb") as f:
            data = aiohttp.FormData()
            file_data = f.read()
            pbar.update(len(file_data))
            data.add_field("file", file_data, filename=filename,
                           content_type="application/zip")

        try:
            async with session.post(
                BASE_URL,
                params=QUERY_PARAMS,
                data=data,
                headers=HEADERS
            ) as response:
                result = await response.json()
                print(f"\n✅ {filename} загружен. Ответ: {result}")
                return result
        except Exception as e:
            print(f"\n❌ Ошибка при загрузке {filename}: {e}")
            return None


async def main():
    connector = aiohttp.TCPConnector(limit_per_host=3)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [upload_file(session, fp) for fp in FILE_PATHS]
        await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
