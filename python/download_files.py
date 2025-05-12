import aiohttp
import asyncio
from tqdm import tqdm
import os

# Конфигурация
DOWNLOAD_URL = "http://127.0.0.1:8081/api/v1/download_object_stream"

# Заголовки
HEADERS = {
    "accept": "application/json",
    "Token": "12345"  # токен из curl
}

# Параметры для скачивания
DOWNLOAD_PARAMS = {
    "bucket_name": "iset",
    "system_entity": "work_object",
    "row_entity_id": "0196c46c-6e0e-7a76-9ac0-919417815584",
    "type_attachment": "json",
    "file_name": "Мезмай 2016-20240912T123457Z-003.zip"
}

OUTPUT_DIR = "downloads"  # директория для сохранения


async def download_file(session, output_path):
    print(f"\n🔽 Начинаем скачивать файл...")
    params = DOWNLOAD_PARAMS

    try:
        async with session.get(
            DOWNLOAD_URL,
            params=params,
            headers=HEADERS,
            ssl=False  # если сервер не использует SSL
        ) as response:
            if response.status == 200:
                total_size = int(response.headers.get("Content-Length", 0))
                downloaded_size = 0

                os.makedirs(os.path.dirname(output_path), exist_ok=True)

                with open(output_path, "wb") as f, tqdm(
                    desc=f"Скачивание {os.path.basename(output_path)}",
                    total=total_size,
                    unit="B",
                    unit_scale=True,
                    unit_divisor=1024,
                ) as pbar:

                    # 64 KB
                    async for chunk in response.content.iter_chunked(64 * 1024):
                        if chunk:
                            f.write(chunk)
                            downloaded_size += len(chunk)
                            pbar.update(len(chunk))

                print(f"\n✅ Файл успешно сохранён: {output_path}")
            else:
                text = await response.text()
                print(
                    f"\n❌ Ошибка при скачивании файла: {response.status} — {text[:200]}...")

    except Exception as e:
        print(f"\n❌ Исключение при скачивании: {e}")


async def main():
    connector = aiohttp.TCPConnector(limit_per_host=3)
    async with aiohttp.ClientSession(connector=connector) as session:

        # 🔽 Сначала скачиваем один файл
        output_path = os.path.join(OUTPUT_DIR, DOWNLOAD_PARAMS["file_name"])
        await download_file(session, output_path)


if __name__ == "__main__":
    asyncio.run(main())
