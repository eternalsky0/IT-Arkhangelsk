# python3 polarview_downloader.py
import requests
import os
from tqdm import tqdm
from datetime import date, timedelta, datetime

# --- НАСТРОЙКИ ---
DOWNLOAD_FOLDER = "daily_sar_images"
# Укажите, за какую дату вы хотите скачать снимки.
# По умолчанию - "вчерашний день".
TARGET_DATE = date.today() - timedelta(days=1)

# --- КОД СКРИПТА ---

def download_file(url, folder):
    """Скачивает файл с прогресс-баром."""
    try:
        local_filename = url.split('/')[-1]
        path = os.path.join(folder, local_filename)
        if os.path.exists(path):
            print(f"Файл {local_filename} уже существует. Пропускаем.")
            return True
        print(f"Скачивание {local_filename}...")
        with requests.get(url, stream=True, timeout=60) as r:
            r.raise_for_status()
            total_size = int(r.headers.get('content-length', 0))
            with open(path, 'wb') as f, tqdm(
                total=total_size, unit='iB', unit_scale=True, desc=local_filename
            ) as bar:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
                    bar.update(len(chunk))
        print(f"Файл {local_filename} успешно скачан.")
        return True
    except Exception as e:
        print(f"Ошибка при скачивании {url}: {e}")
        return False

def main():
    """Главная функция для скачивания всех снимков за определенную дату."""
    # Создаем подпапку для конкретной даты
    date_str = TARGET_DATE.strftime("%Y-%m-%d")
    target_folder = os.path.join(DOWNLOAD_FOLDER, date_str)
    if not os.path.exists(target_folder):
        os.makedirs(target_folder)

    # --- ЭТАП 1: Формирование WFS запроса с фильтром по дате ---
    wfs_base_url = "https://geos.polarview.aq/geoserver/wfs"
    
    # Формируем временной интервал для фильтра (полные сутки)
    start_time = datetime.combine(TARGET_DATE, datetime.min.time()).strftime('%Y-%m-%dT%H:%M:%SZ')
    end_time = datetime.combine(TARGET_DATE, datetime.max.time()).strftime('%Y-%m-%dT%H:%M:%SZ')
    
    # CQL (Contextual Query Language) фильтр для WFS
    cql_filter = f"acqtime DURING {start_time}/{end_time}"
    
    params = {
        'service': 'WFS',
        'version': '1.1.0',
        'request': 'GetFeature',
        'typeName': 'polarview:vw_s1subsets_n',
        'outputFormat': 'application/json',
        'cql_filter': cql_filter # <-- НАШ ФИЛЬТР ПО ДАТЕ
    }

    print(f"Запрос снимков Sentinel-1 за {date_str}...")
    try:
        response = requests.get(wfs_base_url, params=params, timeout=120) # Увеличим таймаут
        response.raise_for_status()
        data = response.json()
    except Exception as e:
        print(f"Критическая ошибка: не удалось получить список снимков. {e}")
        return

    # --- ЭТАП 2: Извлечение имен файлов и построение ссылок ---
    features = data.get('features', [])
    if not features:
        print(f"За {date_str} не найдено ни одного снимка Sentinel-1.")
        return
        
    print(f"Найдено {len(features)} снимков. Формируем ссылки на скачивание...")
    
    download_links = []
    download_template = "https://www.polarview.aq/images/104_S1geotiff/{filename}.tif.tar.gz"

    for feature in features:
        properties = feature.get('properties', {})
        filename_with_ext = properties.get('filename')
        if filename_with_ext:
            base_filename = filename_with_ext.removesuffix('.tif')
            full_url = download_template.format(filename=base_filename)
            download_links.append(full_url)

    # --- ЭТАП 3: Скачивание всех найденных файлов ---
    print(f"\nНачинаем скачивание {len(download_links)} снимков...")
    
    for link in download_links:
        download_file(link, target_folder)
        
    print("\nСкачивание завершено.")
    print(f"Все файлы за {date_str} сохранены в папке: {target_folder}")

if __name__ == "__main__":
    main()
