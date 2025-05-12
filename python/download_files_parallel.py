import aiohttp
import asyncio
from tqdm import tqdm
import os
import time
import psutil

# Конфигурация
DOWNLOAD_URL = "http://127.0.0.1:8081/api/v1/download_object_stream"

HEADERS = {
    "accept": "application/json",
    "Token": "12345"
}

# Параметры для скачивания (можно сделать список)
DOWNLOAD_PARAMS_LIST = [
    {
        "bucket_name": "iset",
        "system_entity": "work_object",
        "row_entity_id": "0196c46c-6e0e-7a76-9ac0-919417815584",
        "type_attachment": "json",
        "file_name": f"Мезмай 2016-20240912T123457Z-00{i}.zip"
    } for i in range(1, 4)  # Скачаем 3 файла
]

OUTPUT_DIR = "downloads"
CHUNK_SIZE = 64 * 1024  # 64 KB


def get_system_usage() -> dict[str, float]:
    """Собирает текущее состояние системных ресурсов"""
    return {
        "timestamp": time.time(),
        "cpu_percent": psutil.cpu_percent(interval=None),
        "memory_used_mb": psutil.virtual_memory().used / (1024 ** 2),
        "memory_total_mb": psutil.virtual_memory().total / (1024 ** 2),
        "disk_read_mb": psutil.disk_io_counters().read_bytes / (1024 ** 2),
        "disk_write_mb": psutil.disk_io_counters().write_bytes / (1024 ** 2),
        "net_sent_mb": psutil.net_io_counters().bytes_sent / (1024 ** 2),
        "net_recv_mb": psutil.net_io_counters().bytes_recv / (1024 ** 2),
    }


def log_resource_usage(label: str):
    """
    Декоратор для измерения использования ресурсов до и после выполнения функции.
    """
    def decorator(func):
        async def wrapper(*args, **kwargs):
            start_usage = get_system_usage()
            print(f"[{label}] Начало: {start_usage}")

            result = await func(*args, **kwargs)

            end_usage = get_system_usage()
            delta = {key: end_usage[key] - start_usage[key]
                     for key in start_usage}

            print(f"[{label}] Завершено.")
            print(f"[{label}] Изменение ресурсов:")
            for k, v in delta.items():
                print(f"  {k}: {v:.2f}")
            return result
        return wrapper
    return decorator


async def download_file(session, params, output_path):
    print(f"\n🔽 Начинаем скачивать файл: {params['file_name']}")

    try:
        async with session.get(
            DOWNLOAD_URL,
            params=params,
            headers=HEADERS,
            ssl=False
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

                    async for chunk in response.content.iter_chunked(CHUNK_SIZE):
                        if chunk:
                            f.write(chunk)
                            downloaded_size += len(chunk)
                            pbar.update(len(chunk))

                print(f"\n✅ Файл сохранён: {output_path}")
                return downloaded_size
            else:
                text = await response.text()
                print(
                    f"\n❌ Ошибка при скачивании {params['file_name']}: {response.status} — {text[:200]}...")
                return 0
    except Exception as e:
        print(f"\n❌ Исключение при скачивании {params['file_name']}: {e}")
        return 0


@log_resource_usage("TotalDownload")
async def download_all_files(session, params_list):
    tasks = []
    for idx, params in enumerate(params_list):
        file_name = params["file_name"]
        output_path = os.path.join(OUTPUT_DIR, file_name)
        tasks.append(download_file(session, params, output_path))

    results = await asyncio.gather(*tasks)

    total_downloaded = sum(results)
    print(f"\n🏁 Загружено всего: {total_downloaded / (1024 ** 2):.2f} MB")


async def main():
    connector = aiohttp.TCPConnector(limit_per_host=3)
    async with aiohttp.ClientSession(connector=connector) as session:
        await download_all_files(session, DOWNLOAD_PARAMS_LIST)


if __name__ == "__main__":
    asyncio.run(main())
