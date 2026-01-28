def yadisk_file_gateway(arguments):
    """
    Яндекс.Диск helper (только ссылки):
      upload   — загрузка файла по URL на Яндекс.Диск (возвращает ссылку на загруженный файл)
      download — получение ссылки для скачивания (OAuth+disk_path ИЛИ public_key)
      rename   — переименование (через move)
      delete   — удаление
      list     — список элементов в папке
    
    Параметры:
      - limit: минимальное значение 10 (значения <= 0 автоматически заменяются на 10)
               Для Яндекс.Диска limit=0 означает "верни 0 элементов" и ломает логику
    """
    import os
    import json
    import sys
    import re  # Для парсинга HTML
    import time  # Для retry логики
    from typing import Optional, Dict, Any
    import requests

    BASE = "https://cloud-api.yandex.net/v1/disk"

    # ---------- helpers ----------
    def _auth_headers(token: str) -> Dict[str, str]:
        return {"Authorization": f"OAuth {token}", "Accept": "application/json"}

    def _norm_disk_path(path: str) -> str:
        if not path:
            return ""
        if path.startswith("disk:/"):
            return path
        return f"disk:/{path.lstrip('/')}"

    def _validate_inputs(arguments: Dict[str, Any]) -> Optional[str]:
        """
        Валидация входных параметров
        Возвращает None если все OK, иначе сообщение об ошибке
        """
        action = arguments.get("action")
        if not action:
            return "Не указан action"
        
        if action not in ["upload", "download", "rename", "delete", "list"]:
            return f"Неизвестное действие: {action}"
        
        # Валидация OAuth токена
        token = arguments.get("oauth_token", "").strip()
        if action in ["upload", "rename", "delete", "list"]:
            if not token:
                return f"Для действия '{action}' требуется oauth_token"
            if len(token) < 10:  # Минимальная длина токена
                return "OAuth токен слишком короткий"
            # Проверка на потенциально опасные символы в токене
            dangerous_chars = ['<', '>', '"', "'", '&', '\x00', '\n', '\r']
            if any(char in token for char in dangerous_chars):
                return "OAuth токен содержит недопустимые символы"
        
        # Валидация путей
        disk_path = arguments.get("disk_path", "")
        if action in ["upload", "rename", "delete", "list"]:
            if not disk_path:
                return f"Для действия '{action}' требуется disk_path"
            # Проверка на недопустимые символы в пути (исключаем ':' так как он нужен для disk:/)
            invalid_chars = ['<', '>', '"', '|', '?', '*']
            if any(char in disk_path for char in invalid_chars):
                return f"Путь содержит недопустимые символы: {invalid_chars}"
        
        # Валидация URL
        file_url = arguments.get("file_url", "")
        if action == "upload":
            if not file_url:
                return "Для upload требуется file_url"
            if not file_url.startswith(("http://", "https://")):
                return "file_url должен начинаться с http:// или https://"
            if len(file_url) > 2048:  # Ограничение длины URL
                return "URL слишком длинный (максимум 2048 символов)"
            # Проверка на потенциально опасные символы в URL
            dangerous_chars = ['<', '>', '"', "'", '\x00', '\n', '\r']
            if any(char in file_url for char in dangerous_chars):
                return "URL содержит недопустимые символы"
        
        # Валидация new_name для rename
        if action == "rename":
            new_name = arguments.get("new_name", "")
            if not new_name:
                return "Для rename требуется new_name"
            if len(new_name) > 255:  # Ограничение длины имени файла
                return "Имя файла слишком длинное (максимум 255 символов)"
            invalid_chars = ['<', '>', ':', '"', '|', '?', '*', '/', '\\']
            if any(char in new_name for char in invalid_chars):
                return f"Имя файла содержит недопустимые символы: {invalid_chars}"
        
        # Валидация числовых параметров
        # Примечание: limit <= 0 будет автоматически заменен на минимальное значение (10) в основном коде
        # Минимальный лимит для Яндекс.Диска: 10 (API не принимает 0, что означает "верни 0 элементов")
        limit = arguments.get("limit", 100)
        # Валидация limit перенесена в основной код для автоматической подмены проблемных значений
        
        offset = arguments.get("offset", 0)
        if isinstance(offset, (int, str)) and int(offset) < 0:
            return "offset не может быть отрицательным"
        
        chunk_size = arguments.get("chunk_size")
        if chunk_size is not None:
            try:
                chunk_size = int(chunk_size)
                if chunk_size < 1024 or chunk_size > 10 * 1024 * 1024:  # От 1KB до 10MB
                    return "chunk_size должен быть от 1024 до 10485760 байт"
            except (ValueError, TypeError):
                return "chunk_size должен быть числом"
        
        return None

    def _json_error(resp: requests.Response) -> str:
        try:
            j = resp.json()
            msg = j.get("message") or j.get("description") or json.dumps(j, ensure_ascii=False)
        except Exception:
            msg = resp.text
        return f"HTTP {resp.status_code}: {msg}".strip()

    def _choose_chunk_size(file_size: Optional[int], override: Optional[int]) -> int:
        if override and override > 0:
            return int(override)
        if not file_size or file_size <= 10 * 1024 * 1024:  # ≤ 10 MB
            return 512 * 1024
        if file_size <= 100 * 1024 * 1024:
            return 1 * 1024 * 1024
        if file_size <= 1024 * 1024 * 1024:
            return 2 * 1024 * 1024
        return 4 * 1024 * 1024

    def _make_request_with_retry(method: str, url: str, max_retries: int = 3, **kwargs) -> requests.Response:
        """
        Выполняет HTTP запрос с повторными попытками при ошибках
        """
        
        # Специальные заголовки для Google Colab
        colab_headers = {
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': '*/*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Cache-Control': 'max-age=0'
        }
        
        # Добавляем заголовки если их нет
        if 'headers' not in kwargs:
            kwargs['headers'] = colab_headers
        else:
            kwargs['headers'].update(colab_headers)
        
        for attempt in range(max_retries):
            try:
                if method.upper() == "GET":
                    response = requests.get(url, **kwargs)
                elif method.upper() == "POST":
                    response = requests.post(url, **kwargs)
                elif method.upper() == "PUT":
                    response = requests.put(url, **kwargs)
                elif method.upper() == "DELETE":
                    response = requests.delete(url, **kwargs)
                else:
                    raise ValueError(f"Неподдерживаемый HTTP метод: {method}")
                
                # Если получили успешный ответ или ошибку клиента (4xx), не повторяем
                if response.status_code < 500:
                    return response
                    
                # Для серверных ошибок (5xx) повторяем
                if attempt < max_retries - 1:
                    wait_time = 2 ** attempt  # Экспоненциальная задержка
                    time.sleep(wait_time)
                    continue
                    
            except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
                if attempt < max_retries - 1:
                    wait_time = 2 ** attempt
                    time.sleep(wait_time)
                    continue
                else:
                    raise e
        
        return response

    def _detect_environment() -> str:
        """
        Определяет среду выполнения (Google Colab, Jupyter, обычный Python)
        """
        try:
            import google.colab
            return "colab"
        except ImportError:
            try:
                import IPython
                if IPython.get_ipython() is not None:
                    return "jupyter"
            except ImportError:
                pass
        return "python"

    def _get_colab_cache_dir() -> str:
        """
        Получает путь к директории кэша для Google Colab
        """
        cache_dir = "/content/yadisk_cache"
        if not os.path.exists(cache_dir):
            os.makedirs(cache_dir)
        return cache_dir

    def _save_to_colab_cache(file_data: bytes, filename: str, show_progress: bool = False) -> str:
        """
        Сохраняет файл в кэш Google Colab и возвращает путь к файлу
        """
        try:
            cache_dir = _get_colab_cache_dir()
            file_path = os.path.join(cache_dir, filename)
            
            with open(file_path, "wb") as f:
                f.write(file_data)
            
            if show_progress:
                sys.stdout.write(f"Файл сохранен в кэш: {file_path}\n")
            
            return file_path
        except Exception as e:
            if show_progress:
                sys.stdout.write(f"Ошибка сохранения в кэш: {e}\n")
            return None

    def _create_colab_download_link(file_path: str, filename: str, show_progress: bool = False) -> str:
        """
        Создает HTML-страницу для скачивания файла из кэша Google Colab
        """
        try:
            # Создаем уникальное имя для HTML файла
            html_filename = f"download_{filename}_{int(time.time())}.html"
            html_path = os.path.join(_get_colab_cache_dir(), html_filename)
            
            # Создаем HTML-страницу с автоматическим скачиванием
            html_content = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>Скачивание файла: {filename}</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 40px; }}
        .download-btn {{ 
            background-color: #4CAF50; 
            color: white; 
            padding: 15px 32px; 
            text-align: center; 
            text-decoration: none; 
            display: inline-block; 
            font-size: 16px; 
            margin: 4px 2px; 
            cursor: pointer; 
            border-radius: 4px;
        }}
        .info {{ background-color: #f0f0f0; padding: 15px; border-radius: 4px; margin: 20px 0; }}
    </style>
    <script>
        // Автоматическое скачивание при загрузке страницы
        window.onload = function() {{
            var link = document.createElement('a');
            link.href = '/content/yadisk_cache/{filename}';
            link.download = '{filename}';
            document.body.appendChild(link);
            link.click();
            document.body.removeChild(link);
            
            // Показываем сообщение
            document.getElementById('message').innerHTML = 'Файл скачивается...';
        }};
    </script>
</head>
<body>
    <h2>📁 Скачивание файла: {filename}</h2>
    <div class="info">
        <p><strong>Статус:</strong> <span id="message">Подготовка к скачиванию...</span></p>
        <p><strong>Размер файла:</strong> {os.path.getsize(file_path) if os.path.exists(file_path) else 'Неизвестно'} байт</p>
        <p><strong>Время создания:</strong> {time.strftime('%Y-%m-%d %H:%M:%S')}</p>
    </div>
    
    <p>Если скачивание не началось автоматически:</p>
    <a href="/content/yadisk_cache/{filename}" download="{filename}" class="download-btn">
        📥 Скачать файл
    </a>
    
    <div class="info">
        <h3>Инструкции:</h3>
        <ol>
            <li>Файл должен скачаться автоматически</li>
            <li>Если не работает, нажмите кнопку "Скачать файл" выше</li>
            <li>Файл также доступен по пути: <code>/content/yadisk_cache/{filename}</code></li>
        </ol>
    </div>
</body>
</html>"""
            
            with open(html_path, "w", encoding="utf-8") as f:
                f.write(html_content)
            
            if show_progress:
                sys.stdout.write(f"Создана HTML-страница для скачивания: {html_path}\n")
            
            return html_path
        except Exception as e:
            if show_progress:
                sys.stdout.write(f"Ошибка создания HTML-страницы: {e}\n")
            return None

    def _download_file_from_url(url: str, show_progress: bool = False) -> Optional[bytes]:
        """
        Скачивает файл по URL в память
        """
        try:
            if show_progress:
                sys.stdout.write(f"Скачиваем файл по URL: {url[:100]}...\n")
            
            # Специальные заголовки для обхода блокировок
            headers = {
                'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Accept': '*/*',
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',
                'Connection': 'keep-alive',
                'Referer': 'https://disk.yandex.ru/',
                'Sec-Fetch-Dest': 'empty',
                'Sec-Fetch-Mode': 'cors',
                'Sec-Fetch-Site': 'cross-site'
            }
            
            file_response = requests.get(url, headers=headers, stream=True, timeout=300, allow_redirects=True)
            file_response.raise_for_status()
            
            # Читаем файл по частям
            file_data = b""
            total_size = int(file_response.headers.get('Content-Length', 0))
            downloaded = 0
            
            for chunk in file_response.iter_content(chunk_size=8192):
                if chunk:
                    file_data += chunk
                    downloaded += len(chunk)
                    if show_progress and total_size > 0:
                        percent = int(downloaded * 100 / total_size)
                        sys.stdout.write(f"\rСкачивание: {percent}% ({downloaded}/{total_size} байт)")
                        sys.stdout.flush()
                    elif show_progress:
                        sys.stdout.write(f"\rСкачано: {downloaded} байт")
                        sys.stdout.flush()
            
            if show_progress:
                sys.stdout.write(f"\nФайл скачан: {len(file_data)} байт\n")
            
            return file_data
            
        except Exception as e:
            if show_progress:
                sys.stdout.write(f"Ошибка скачивания по URL: {e}\n")
            return None

    def _download_file_directly(token: str, disk_path: str, show_progress: bool = False) -> Optional[bytes]:
        """
        Скачивает файл напрямую в память (для Google Colab)
        """
        try:
            headers = _auth_headers(token)
            
            # Получаем ссылку для скачивания
            download_resp = _make_request_with_retry("GET", f"{BASE}/resources/download", 
                                                   headers=headers, params={"path": disk_path}, timeout=30)
            if download_resp.status_code != 200:
                return None
            
            href = download_resp.json().get("href")
            if not href:
                return None
            
            # Скачиваем файл по полученной ссылке
            return _download_file_from_url(href, show_progress)
            
        except Exception as e:
            if show_progress:
                sys.stdout.write(f"Ошибка прямого скачивания: {e}\n")
            return None

    def _get_colab_compatible_url(token: str, disk_path: str, show_progress: bool = False) -> Optional[str]:
        """
        Получает URL, совместимый с Google Colab
        """
        try:
            headers = _auth_headers(token)
            
            # Пытаемся получить информацию о файле
            file_info = _make_request_with_retry("GET", f"{BASE}/resources", 
                                               headers=headers, params={"path": disk_path}, timeout=30)
            if file_info.status_code != 200:
                return None
            
            file_data = file_info.json()
            
            # Пытаемся получить прямую ссылку для скачивания
            download_resp = _make_request_with_retry("GET", f"{BASE}/resources/download", 
                                                   headers=headers, params={"path": disk_path}, timeout=30)
            if download_resp.status_code == 200:
                href = download_resp.json().get("href")
                if href:
                    # Для Google Colab добавляем специальные параметры
                    if "?" in href:
                        href += "&disposition=attachment"
                    else:
                        href += "?disposition=attachment"
                    return href
            
            return None
            
        except Exception as e:
            if show_progress:
                sys.stdout.write(f"Ошибка получения Colab-совместимого URL: {e}\n")
            return None

    def _get_public_url(token: str, disk_path: str, show_progress: bool = False) -> Optional[str]:
        """
        Получает реальную публичную ссылку на файл через API Яндекс.Диска
        """
        try:
            headers = _auth_headers(token)
            # Сначала пытаемся получить информацию о файле
            file_info = _make_request_with_retry("GET", f"{BASE}/resources", 
                                               headers=headers, params={"path": disk_path}, timeout=30)
            if file_info.status_code != 200:
                if show_progress:
                    sys.stdout.write(f"Не удалось получить информацию о файле: {_json_error(file_info)}\n")
                return None
            
            file_data = file_info.json()
            public_url = file_data.get("public_url")
            
            if public_url:
                if show_progress:
                    sys.stdout.write(f"Получена публичная ссылка: {public_url}\n")
                return public_url
            
            # Если нет публичной ссылки, пытаемся опубликовать файл
            if show_progress:
                sys.stdout.write("Файл не опубликован, пытаемся опубликовать...\n")
            
            publish_resp = _make_request_with_retry("PUT", f"{BASE}/resources/publish", 
                                                  headers=headers, params={"path": disk_path}, timeout=30)
            if publish_resp.status_code in (200, 201, 202):
                # Повторно получаем информацию о файле
                file_info = _make_request_with_retry("GET", f"{BASE}/resources", 
                                                   headers=headers, params={"path": disk_path}, timeout=30)
                if file_info.status_code == 200:
                    file_data = file_info.json()
                    public_url = file_data.get("public_url")
                    if public_url:
                        if show_progress:
                            sys.stdout.write(f"Файл опубликован, получена ссылка: {public_url}\n")
                        return public_url
            
            if show_progress:
                sys.stdout.write("Не удалось получить или создать публичную ссылку\n")
            return None
            
        except Exception as e:
            if show_progress:
                sys.stdout.write(f"Ошибка при получении публичной ссылки: {e}\n")
            return None

    def _print_progress(prefix: str, done_bytes: int, total_bytes: Optional[int], last_percent: list, show: bool):
        if not show:
            return
        if total_bytes and total_bytes > 0:
            percent = int(done_bytes * 100 / total_bytes)
            if percent == last_percent[0]:
                return
            last_percent[0] = percent
            bar_len = 30
            filled = int(percent * bar_len / 100)
            bar = "#" * filled + "-" * (bar_len - filled)
            sys.stdout.write(f"\r{prefix} [{bar}] {percent:3d}%")
            sys.stdout.flush()
            if percent >= 100:
                sys.stdout.write("\n")
        else:
            sys.stdout.write(f"\r{prefix} {done_bytes} bytes...")
            sys.stdout.flush()

    # --- Функция для извлечения прямой ссылки на файл из публичной ссылки Яндекс.Диска ---
    def _extract_direct_download_url(public_url: str, show_progress: bool = False) -> Optional[str]:
        """
        Пытается извлечь прямую ссылку на скачивание из публичной ссылки Яндекс.Диска.
        Это может быть необходимо, если пользователь предоставил ссылку типа https://disk.yandex.ru/...
        """
        if not public_url or not isinstance(public_url, str):
            if show_progress:
                sys.stdout.write("URL не является строкой или пустой\n")
            return None
            
        # Безопасная проверка URL
        if not public_url.startswith("https://disk.yandex.ru/"):
            if show_progress:
                sys.stdout.write(f"URL не является публичной ссылкой Яндекс.Диска: {public_url}\n")
            return None
            
        # Проверка на потенциально опасные символы
        dangerous_chars = ['<', '>', '"', "'", '&', '\x00']
        if any(char in public_url for char in dangerous_chars):
            if show_progress:
                sys.stdout.write("URL содержит потенциально опасные символы\n")
            return None

        # Попробуем получить HTML страницу
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'identity',
                'Connection': 'keep-alive',
                'Cache-Control': 'no-cache'
            }

            # Получаем HTML страницу
            if show_progress:
                sys.stdout.write(f"Получение HTML страницы: {public_url}\n")
            response = requests.get(public_url, headers=headers, timeout=30, allow_redirects=True)
            response.raise_for_status()

            # Ищем скрипт с данными, содержащими downloadUrl
            # Это может быть нестабильно, но часто работает
            html_content = response.text
            # Пример регулярного выражения для поиска downloadUrl (может потребоваться доработка)
            # Паттерн может меняться, поэтому проверяйте, соответствует ли он реальному содержимому страницы
            # Этот шаблон может быть не самым надежным, но работает для простых случаев
            match = re.search(r'"downloadUrl":"([^"]+)"', html_content)
            if match:
                download_url = match.group(1)
                # Убедимся, что URL корректен и начинается с https://downloader.disk.yandex.ru/
                if download_url.startswith("https://downloader.disk.yandex.ru/"):
                    if show_progress:
                        sys.stdout.write(f"Найдена прямая ссылка: {download_url}\n")
                    return download_url
                else:
                    if show_progress:
                        sys.stdout.write(f"Найдена ссылка, но не прямая: {download_url}\n")
            else:
                if show_progress:
                    sys.stdout.write("Не удалось найти downloadUrl в HTML.\n")

            # Попробуем найти другие возможные пути (например, ссылки в JS)
            # Ищем ссылки вида "https://downloader.disk.yandex.ru/..." в JS
            # Это более сложный подход, но может помочь
            js_match = re.search(r'https://downloader\.disk\.yandex\.ru/[^\s"\']+', html_content)
            if js_match:
                direct_url = js_match.group(0)
                if show_progress:
                    sys.stdout.write(f"Найдена прямая ссылка через JS: {direct_url}\n")
                return direct_url
            else:
                if show_progress:
                    sys.stdout.write("Не удалось найти прямую ссылку через JS.\n")

            # Если ничего не нашли, возможно, это ссылка на публичную папку или не тот формат
            # Попробуем найти данные о файле в JSON внутри HTML (часто встречается)
            # Поиск JSON-объекта с информацией о файле
            # Пример: <script id="react-data">{"someKey":"someValue"}</script>
            data_match = re.search(r'<script[^>]*id=["\']react-data["\'][^>]*>(.*?)</script>', html_content, re.DOTALL)
            if data_match:
                try:
                    json_data_str = data_match.group(1)
                    # Попробуем найти downloadUrl в этом JSON
                    # Простой парсинг, может не всегда работать
                    json_data = json.loads(json_data_str)

                    # Ищем в структуре, например, если есть ключи с downloadUrl
                    # Это зависит от внутренней структуры данных Яндекс.Диска
                    # Ниже пример для общего случая
                    def find_download_url(obj):
                        if isinstance(obj, dict):
                            if 'downloadUrl' in obj:
                                return obj['downloadUrl']
                            for value in obj.values():
                                result = find_download_url(value)
                                if result:
                                    return result
                        elif isinstance(obj, list):
                            for item in obj:
                                result = find_download_url(item)
                                if result:
                                    return result
                        return None

                    found_url = find_download_url(json_data)
                    if found_url and found_url.startswith("https://downloader.disk.yandex.ru/"):
                        if show_progress:
                            sys.stdout.write(f"Найдена прямая ссылка через JSON: {found_url}\n")
                        return found_url
                    else:
                        if show_progress:
                            sys.stdout.write(f"Найдена ссылка из JSON, но не прямая: {found_url}\n")
                except Exception as e:
                    if show_progress:
                        sys.stdout.write(f"Ошибка парсинга JSON: {e}\n")
            else:
                if show_progress:
                    sys.stdout.write("Не найден JSON с данными.\n")

            # Если ничего не нашли
            if show_progress:
                sys.stdout.write("Не удалось извлечь прямую ссылку из HTML.\n")
            return None

        except requests.exceptions.RequestException as e:
            if show_progress:
                sys.stdout.write(f"Ошибка при получении страницы: {e}\n")
            return None
        except Exception as e:
            if show_progress:
                sys.stdout.write(f"Ошибка при парсинге: {e}\n")
            return None

    # file-like с прогрессом для upload из URL (потоковая загрузка)
    class ProgressURLFile:
        def __init__(self, url, show):
            self._url = url
            self._show = show
            self._last_percent = None
            self._read = 0
            self._size = 0
            self._response = None
            self._closed = False
            self._error = None
            self._chunk_iterator = None

            # Инициализируем потоковое соединение
            try:
                if self._show:
                    sys.stdout.write("Инициализация потокового соединения...\n")
                    sys.stdout.flush()

                # Настройки для надежного скачивания
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                    'Accept': '*/*',
                    'Accept-Language': 'en-US,en;q=0.9',
                    'Accept-Encoding': 'identity',  # Отключаем сжатие для точности
                    'Connection': 'keep-alive',
                    'Cache-Control': 'no-cache'
                }

                # Используем stream=True для потокового скачивания
                self._response = requests.get(
                    url,
                    headers=headers,
                    timeout=600,
                    allow_redirects=True,
                    stream=True,  # Важно: потоковое скачивание
                    verify=True  # Проверяем SSL сертификаты
                )

                # Проверяем статус код после открытия соединения
                self._response.raise_for_status()

                # Проверка Content-Type
                content_type = self._response.headers.get('Content-Type', '')
                if 'text/html' in content_type.lower():
                    self._error = f"Получен HTML-ответ ({content_type}) вместо файла! Проверьте URL."
                    if self._show:
                        sys.stdout.write(f"Ошибка: {self._error}\n")
                    return

                # Получаем размер файла из заголовков
                content_length = self._response.headers.get('Content-Length')
                if content_length:
                    self._size = int(content_length)
                    if self._show:
                        sys.stdout.write(f"Размер файла: {self._size} байт\n")
                else:
                    if self._show:
                        sys.stdout.write("Размер файла неизвестен, будет показан прогресс в байтах\n")

                # Проверка Content-Disposition для определения имени файла
                content_disposition = self._response.headers.get('Content-Disposition', '')
                if 'filename=' in content_disposition:
                    filename = content_disposition.split('filename=')[1].strip('"\'')
                    if self._show:
                        sys.stdout.write(f"Имя файла по заголовку: {filename}\n")

                # Создаем итератор для потокового чтения
                self._chunk_iterator = self._response.iter_content(chunk_size=8192)

                if self._show:
                    sys.stdout.write(f"Content-Type: {content_type}\n")
                    sys.stdout.write("Начинаем потоковую загрузку на Яндекс.Диск...\n")
                    sys.stdout.flush()

            except requests.exceptions.RequestException as e:
                self._error = f"Ошибка сети при инициализации соединения: {e}"
                if self._show:
                    sys.stdout.write(f"\nОшибка: {self._error}\n")
            except Exception as e:
                self._error = f"Неожиданная ошибка при инициализации: {e}"
                if self._show:
                    sys.stdout.write(f"\nОшибка: {self._error}\n")

        def __len__(self):
            return self._size

        def read(self, amt=1024 * 1024):
            if self._closed or self._chunk_iterator is None:
                return b""

            try:
                # Читаем следующий чанк из потока
                chunk = next(self._chunk_iterator, b"")
                if chunk:
                    self._read += len(chunk)
                    
                    # Показываем прогресс загрузки
                    if self._show:
                        if self._size > 0:
                            percent = int(self._read * 100 / self._size)
                            if percent != self._last_percent:
                                self._last_percent = percent
                                bar_len = 30
                                filled = int(percent * bar_len / 100)
                                bar = "#" * filled + "-" * (bar_len - filled)
                                sys.stdout.write(f"\rЗагрузка на диск [{bar}] {percent:3d}%")
                                sys.stdout.flush()

                            if self._read >= self._size:
                                sys.stdout.write("\n")
                        else:
                            # Если размер неизвестен, показываем в байтах
                            sys.stdout.write(f"\rЗагружено: {self._read} байт")
                            sys.stdout.flush()

                return chunk

            except StopIteration:
                # Поток закончился
                if self._show and self._size == 0:
                    sys.stdout.write(f"\nЗагрузка завершена: {self._read} байт\n")
                return b""
            except Exception as e:
                self._error = f"Ошибка при чтении потока: {e}"
                if self._show:
                    sys.stdout.write(f"\nОшибка: {self._error}\n")
                return b""

        def close(self):
            self._closed = True
            # Закрываем потоковое соединение
            try:
                if self._response:
                    self._response.close()
                if self._show:
                    sys.stdout.write("Потоковое соединение закрыто\n")
                    sys.stdout.flush()
            except Exception:
                pass

        def has_error(self):
            """Возвращает True, если произошла ошибка при загрузке"""
            return self._error is not None

        def get_error(self):
            """Возвращает сообщение об ошибке"""
            return self._error

    # ---------- args ----------
    action = arguments.get("action")
    token = (arguments.get("oauth_token") or "").strip()
    disk_path = _norm_disk_path(arguments.get("disk_path", ""))
    new_name = arguments.get("new_name")
    local_path = arguments.get("local_path")  # локальный путь к файлу
    file_url = arguments.get("file_url")  # URL файла для загрузки
    overwrite = bool(arguments.get("overwrite", True))
    show_progress = bool(arguments.get("show_progress", True))
    chunk_override = arguments.get("chunk_size")
    chunk_override = int(chunk_override) if isinstance(chunk_override, int) and chunk_override > 0 else None
    public_key = arguments.get("public_key")
    public_path = arguments.get("public_path")
    limit = int(arguments.get("limit", 100))
    offset = int(arguments.get("offset", 0))
    direct_download = bool(arguments.get("direct_download", False))  # Прямое скачивание для Colab

    # Валидация входных параметров
    validation_error = _validate_inputs(arguments)
    if validation_error:
        return {"ok": False, "message": validation_error}
    
    # Автоматическая подмена limit <= 0 на минимальное значение (защита от ошибок моделей)
    # Минимальный лимит для Яндекс.Диска: 10 (0 означает "верни 0 элементов" и ломает логику)
    MIN_LIMIT = 10
    if limit <= 0:
        if show_progress:
            sys.stdout.write(f"⚠️  Предупреждение: limit={limit} заменен на {MIN_LIMIT} (минимальное значение для Яндекс.Диска)\n")
        limit = MIN_LIMIT
    elif limit < MIN_LIMIT:
        if show_progress:
            sys.stdout.write(f"⚠️  Предупреждение: limit={limit} заменен на {MIN_LIMIT} (минимальное значение для Яндекс.Диска)\n")
        limit = MIN_LIMIT
    
    # Определяем среду выполнения один раз для всех операций
    environment = _detect_environment()

    try:
        # -------- UPLOAD --------
        if action == "upload":
            if not token:
                return {"ok": False, "message": "Для upload требуется oauth_token"}
            if not disk_path:
                return {"ok": False, "message": "Для upload требуется disk_path"}

            # Проверяем, что указан file_url (локальная загрузка удалена)
            if not file_url:
                return {"ok": False, "message": "Для upload требуется file_url"}
            if local_path:
                return {"ok": False, "message": "Локальная загрузка не поддерживается. Используйте file_url."}

            # --- Попытка извлечь прямую ссылку ---
            # Проверим, является ли file_url прямой ссылкой
            direct_url = file_url
            if show_progress:
                sys.stdout.write(f"Проверка ссылки: {file_url}\n")
            if not file_url.startswith(("http://", "https://")):
                return {"ok": False, "message": "Указанная ссылка не является URL."}
            # Проверим, если это публичная ссылка (не прямая)
            # Для Яндекс.Диска: если это не downloader.disk.yandex.ru
            if "disk.yandex.ru" in file_url and not file_url.startswith("https://downloader.disk.yandex.ru/"):
                if show_progress:
                    sys.stdout.write("Публичная ссылка Яндекс.Диска, попытка извлечь прямую...\n")
                direct_url = _extract_direct_download_url(file_url, show_progress)
                if not direct_url:
                    return {"ok": False,
                            "message": "Не удалось извлечь прямую ссылку на файл. Убедитесь, что указана прямая ссылка на файл."}
            else:
                # Проверим, является ли это прямой ссылкой (на случай, если пользователь уже указал её)
                # Можно добавить проверку доступности файла
                try:
                    check_resp = requests.head(file_url, timeout=10)
                    if check_resp.status_code >= 400:
                        return {"ok": False, "message": f"Указанная ссылка недоступна (код {check_resp.status_code})."}
                except Exception as e:
                    return {"ok": False, "message": f"Ошибка проверки ссылки: {e}"}

            headers = _auth_headers(token)
            params = {"path": disk_path, "overwrite": "true" if overwrite else "false"}
            r = _make_request_with_retry("GET", f"{BASE}/resources/upload", headers=headers, params=params, timeout=30)
            if r.status_code not in (200, 201):
                return {"ok": False, "message": _json_error(r)}
            href = r.json().get("href")
            if not href:
                return {"ok": False, "message": "Не получена ссылка для загрузки"}

            # Используем ProgressURLFile для загрузки по URL
            pf = ProgressURLFile(direct_url, show_progress)  # Используем direct_url

            # Проверяем, произошла ли ошибка при загрузке файла
            if pf.has_error():
                error_msg = pf.get_error()
                if show_progress:
                    sys.stdout.write(f"Ошибка при загрузке файла: {error_msg}\n")
                return {"ok": False, "message": f"Не удалось загрузить файл с URL: {error_msg}"}

            # Получаем размер файла из заголовков (может быть 0 если неизвестен)
            file_size = len(pf)  # размер из заголовков HTTP

            try:
                # Используем data=pf, который реализует read()
                put = _make_request_with_retry("PUT", href, max_retries=2,  # Меньше попыток для больших файлов
                    data=pf,
                    headers={"Content-Type": "application/octet-stream"},
                                             timeout=600)
            finally:
                pf.close()

            if put.status_code not in (200, 201, 202):
                return {"ok": False, "message": _json_error(put)}

            # --- Получение ссылки на загруженный файл ---
            # После успешной загрузки получаем публичную ссылку
            file_url_on_disk = _get_public_url(token, disk_path, show_progress)
            if not file_url_on_disk:
                # Если не удалось получить публичную ссылку, возвращаем путь на диске
                file_url_on_disk = disk_path
                if show_progress:
                    sys.stdout.write(f"Публичная ссылка недоступна, используем путь: {disk_path}\n")

            return {"ok": True, "message": "Файл успешно загружен", "data": {
                "disk_path": disk_path,
                "file_size": file_size,
                "file_url": file_url_on_disk  # <-- Добавлена ссылка на загруженный файл
            }}

        # -------- DOWNLOAD --------
        elif action == "download":
            if show_progress:
                sys.stdout.write(f"Обнаружена среда выполнения: {environment}\n")
            
            # приватный файл (OAuth + disk_path)
            if token and disk_path:
                headers = _auth_headers(token)
                
                # Сначала получаем информацию о файле
                file_info = _make_request_with_retry("GET", f"{BASE}/resources", 
                                                   headers=headers, params={"path": disk_path}, timeout=30)
                if file_info.status_code != 200:
                    return {"ok": False, "message": f"Файл не найден: {_json_error(file_info)}"}
                
                file_data = file_info.json()
                file_size = file_data.get("size")
                
                href = None
                
                # Для Google Colab используем специальную обработку
                if environment == "colab":
                    if show_progress:
                        sys.stdout.write("Используем специальную обработку для Google Colab...\n")
                    
                    # Для Google Colab всегда используем кэш
                    if show_progress:
                        sys.stdout.write("Скачивание файла в кэш Google Colab...\n")
                    
                    file_data = _download_file_directly(token, disk_path, show_progress)
                    if file_data:
                        # Определяем имя файла
                        filename = os.path.basename(disk_path.replace("disk:/", ""))
                        if not filename:
                            filename = f"downloaded_file_{int(time.time())}"
                        
                        # Сохраняем файл в кэш
                        cached_file_path = _save_to_colab_cache(file_data, filename, show_progress)
                        if cached_file_path:
                            # Создаем HTML-страницу для скачивания
                            html_path = _create_colab_download_link(cached_file_path, filename, show_progress)
                            
                            if show_progress:
                                sys.stdout.write(f"Файл сохранен в кэш: {cached_file_path}\n")
                                if html_path:
                                    sys.stdout.write(f"HTML-страница создана: {html_path}\n")
                            
                            return {
                                "ok": True, 
                                "message": "Файл успешно скачан и сохранен в кэш", 
                                "data": {
                                    "filename": filename,
                                    "file_size": len(file_data),
                                    "environment": environment,
                                    "download_method": "colab_cache",
                                    "disk_path": disk_path,
                                    "cached_file_path": cached_file_path,
                                    "html_download_page": html_path,
                                    "direct_file_url": f"/content/yadisk_cache/{filename}",
                                    "instructions": [
                                        f"1. Файл сохранен в кэш: {cached_file_path}",
                                        f"2. Откройте HTML-страницу: {html_path}",
                                        f"3. Или используйте прямую ссылку: /content/yadisk_cache/{filename}",
                                        "4. Файл будет скачан автоматически при открытии HTML-страницы"
                                    ]
                                }
                            }
                        else:
                            if show_progress:
                                sys.stdout.write("Ошибка сохранения в кэш, пробуем получить ссылку...\n")
                    else:
                        if show_progress:
                            sys.stdout.write("Прямое скачивание не удалось, пробуем получить ссылку...\n")
                    
                    href = _get_colab_compatible_url(token, disk_path, show_progress)
                    if href:
                        if show_progress:
                            sys.stdout.write("Получена Colab-совместимая ссылка\n")
                
                # Если Colab-специфичная ссылка не получена, пробуем стандартный способ
                if not href:
                    if show_progress:
                        sys.stdout.write("Пробуем стандартный способ получения ссылки...\n")
                    
                    # Пытаемся получить прямую ссылку для скачивания
                    download_attempts = 0
                    max_attempts = 3
                    
                    while download_attempts < max_attempts and not href:
                        download_attempts += 1
                        if show_progress:
                            sys.stdout.write(f"Попытка {download_attempts}/{max_attempts} получить ссылку для скачивания...\n")
                        
                        r = _make_request_with_retry("GET", f"{BASE}/resources/download", 
                                                   headers=headers, params={"path": disk_path}, timeout=30)
                        
                        if r.status_code == 200:
                            href = r.json().get("href")
                            if href:
                                # Для Google Colab добавляем специальные параметры
                                if environment == "colab":
                                    if "?" in href:
                                        href += "&disposition=attachment"
                                    else:
                                        href += "?disposition=attachment"
                                
                                # Проверяем работоспособность ссылки (только если не Colab)
                                if environment != "colab":
                                    try:
                                        check_response = requests.head(href, timeout=10, allow_redirects=True)
                                        if check_response.status_code >= 400:
                                            if show_progress:
                                                sys.stdout.write(f"Ссылка недоступна (код {check_response.status_code}), пробуем снова...\n")
                                            href = None  # Сбрасываем, чтобы попробовать снова
                                        else:
                                            if show_progress:
                                                sys.stdout.write("Ссылка для скачивания получена и проверена\n")
                                    except Exception as e:
                                        if show_progress:
                                            sys.stdout.write(f"Ошибка проверки ссылки: {e}, пробуем снова...\n")
                                        href = None
                                else:
                                    if show_progress:
                                        sys.stdout.write("Ссылка для скачивания получена (Colab режим)\n")
                        else:
                            if show_progress:
                                sys.stdout.write(f"Ошибка получения ссылки: {_json_error(r)}\n")
                
                if not href:
                    # Если не удалось получить прямую ссылку, попробуем получить публичную ссылку
                    if show_progress:
                        sys.stdout.write("Прямая ссылка недоступна, пытаемся получить публичную ссылку...\n")
                    
                    public_url = _get_public_url(token, disk_path, show_progress)
                    if public_url:
                        href = public_url
                        if show_progress:
                            sys.stdout.write("Получена публичная ссылка как альтернатива\n")
                    else:
                        return {"ok": False, "message": f"Не удалось получить рабочую ссылку для скачивания. Попробуйте использовать публичную ссылку или проверьте права доступа к файлу."}
                
                # Для Google Colab, если не удалось скачать в кэш, создаем альтернативную ссылку
                if environment == "colab" and href:
                    if show_progress:
                        sys.stdout.write("Создаем альтернативную ссылку для скачивания...\n")
                    
                    # Создаем HTML-страницу для скачивания
                    filename = os.path.basename(disk_path.replace("disk:/", ""))
                    if not filename:
                        filename = "downloaded_file"
                    
                    # Создаем простую HTML-страницу с автоматическим скачиванием
                    html_content = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>Скачивание файла</title>
    <script>
        // Автоматическое скачивание при загрузке страницы
        window.onload = function() {{
            var link = document.createElement('a');
            link.href = '{href}';
            link.download = '{filename}';
            document.body.appendChild(link);
            link.click();
            document.body.removeChild(link);
            
            // Показываем сообщение
            document.getElementById('message').innerHTML = 'Файл скачивается...';
        }};
    </script>
</head>
<body>
    <h2>Скачивание файла: {filename}</h2>
    <p id="message">Подготовка к скачиванию...</p>
    <p>Если скачивание не началось автоматически, <a href="{href}" download="{filename}">нажмите здесь</a></p>
</body>
</html>"""
                    
                    # Сохраняем HTML-файл
                    html_filename = f"download_{filename}.html"
                    with open(html_filename, "w", encoding="utf-8") as f:
                        f.write(html_content)
                    
                    if show_progress:
                        sys.stdout.write(f"Создана HTML-страница для скачивания: {html_filename}\n")
                    
                    # Возвращаем информацию о созданной странице
                    return {
                        "ok": True, 
                        "message": "Создана страница для автоматического скачивания", 
                        "data": {
                            "download_url": href,  # Оригинальная ссылка
                            "html_file": html_filename,  # HTML-страница для скачивания
                            "filename": filename,
                            "file_size": file_size,
                            "environment": environment,
                            "download_method": "html_redirect",
                            "disk_path": disk_path,
                            "instructions": [
                                f"1. Откройте файл {html_filename} в браузере",
                                "2. Файл скачается автоматически",
                                f"3. Или используйте прямую ссылку: {href}"
                            ]
                        }
                    }
                
            # публичный файл (без OAuth)
            elif public_key:
                params = {"public_key": public_key}
                if public_path:
                    params["path"] = public_path
                r = _make_request_with_retry("GET", f"{BASE}/public/resources/download", 
                                           params=params, timeout=30)
                if r.status_code != 200:
                    return {"ok": False, "message": _json_error(r)}
                href = r.json().get("href")
                if not href:
                    return {"ok": False, "message": "Сервис не вернул href для скачивания"}
                file_size = None
            else:
                return {"ok": False, "message": "Для download укажи либо oauth_token+disk_path, либо public_key"}

            data = {
                "download_url": href, 
                "file_size": file_size,
                "expires_in": "Ссылка действительна ограниченное время (обычно несколько часов)",
                "note": "Если ссылка не работает, попробуйте получить новую через повторный вызов функции",
                "download_method": "direct" if token and disk_path else "public",
                "environment": environment
            }
            
            # Дополнительные инструкции для Google Colab
            if environment == "colab":
                data["colab_instructions"] = [
                    "1. Файл автоматически скачивается в кэш /content/yadisk_cache/",
                    "2. Откройте HTML-файл в браузере для автоматического скачивания",
                    "3. Или используйте прямую ссылку на файл в кэше",
                    "4. Альтернатива: используйте публичную ссылку через public_key"
                ]
                data["colab_example"] = {
                    "cache_download": {
                        "action": "download",
                        "oauth_token": "YOUR_TOKEN",
                        "disk_path": "disk:/file3.mp3",
                        "show_progress": True
                    },
                    "public_download": {
                        "action": "download",
                        "public_key": "https://disk.yandex.ru/d/NS-00uW07T-EsQ",
                        "public_path": "filename.mp3"
                    }
                }
            if token and disk_path:
                data.update({"disk_path": disk_path})
            else:
                data.update({"public_key": public_key, "public_path": public_path})
            return {"ok": True, "message": "Получена ссылка для скачивания", "data": data}

        # -------- RENAME --------
        elif action == "rename":
            if not token:
                return {"ok": False, "message": "Для rename требуется oauth_token"}
            if not disk_path or not new_name:
                return {"ok": False, "message": "Для rename нужны disk_path и new_name"}

            headers = _auth_headers(token)
            from_path = disk_path
            # Правильная обработка путей: если файл в корне диска, parent_dir будет пустым
            path_parts = from_path.split("/")
            if len(path_parts) > 1 and path_parts[-1]:  # есть имя файла
                parent_dir = "/".join(path_parts[:-1])
                to_path = f"{parent_dir}/{new_name}" if parent_dir else f"disk:/{new_name}"
            else:
                # Если путь заканчивается на "/" или это корень диска
                to_path = f"{from_path.rstrip('/')}/{new_name}" if from_path != "disk:/" else f"disk:/{new_name}"
            params = {"from": from_path, "path": to_path, "overwrite": "true"}
            r = requests.post(f"{BASE}/resources/move", headers=headers, params=params, timeout=30)
            if r.status_code not in (200, 201, 202):
                return {"ok": False, "message": _json_error(r)}

            # Получаем ссылку на переименованный файл
            file_link = _get_public_url(token, to_path, show_progress)
            if not file_link:
                # Если не удалось получить публичную ссылку, используем путь на диске
                file_link = to_path

            return {"ok": True, "message": f"Файл переименован в {new_name}",
                    "data": {"old_path": from_path, "new_path": to_path, "file_url": file_link}}

        # -------- DELETE --------
        elif action == "delete":
            if not token:
                return {"ok": False, "message": "Для delete требуется oauth_token"}
            if not disk_path:
                return {"ok": False, "message": "Для delete требуется disk_path"}
            headers = _auth_headers(token)
            r = requests.delete(f"{BASE}/resources", headers=headers, params={"path": disk_path, "permanently": "true"},
                                timeout=30)
            if r.status_code not in (202, 204):
                return {"ok": False, "message": _json_error(r)}
            return {"ok": True, "message": "Файл удален", "data": {"disk_path": disk_path}}

        # -------- LIST --------
        elif action == "list":
            if not token:
                return {"ok": False, "message": "Для list требуется oauth_token"}
            if not disk_path:
                return {"ok": False, "message": "Для list требуется disk_path папки"}
            headers = _auth_headers(token)
            params = {
                "path": disk_path,
                "limit": limit,
                "offset": offset,
                "fields": "_embedded.items.name,_embedded.items.type,_embedded.items.size,_embedded.items.mime_type,_embedded.items.path,_embedded.total"
            }
            r = requests.get(f"{BASE}/resources", headers=headers, params=params, timeout=30)
            if r.status_code != 200:
                return {"ok": False, "message": _json_error(r)}
            j = r.json()
            embedded = j.get("_embedded", {})
            items = embedded.get("items", [])
            total = embedded.get("total", len(items))
            simplified = []
            for it in items:
                item_data = {
                    "name": it.get("name"),
                    "type": it.get("type"),
                    "size": it.get("size"),
                    "mime_type": it.get("mime_type"),
                    "path": it.get("path")
                }
                # Добавляем ссылку на файл, если это файл
                if it.get("type") == "file":
                    item_path = it.get("path", "")
                    # Пытаемся получить публичную ссылку для каждого файла
                    public_url = _get_public_url(token, item_path, False)  # Без показа прогресса для каждого файла
                    item_data["file_url"] = public_url or item_path
                simplified.append(item_data)

            return {"ok": True, "message": f"Элементов: {len(simplified)} из {total}",
                    "data": {"disk_path": disk_path, "total": total, "limit": limit, "offset": offset,
                             "items": simplified}}

        else:
            return {"ok": False, "message": f"Неизвестное действие: {action}"}

    except requests.exceptions.Timeout as e:
        return {"ok": False, "message": f"Превышено время ожидания: {e}"}
    except requests.exceptions.ConnectionError as e:
        return {"ok": False, "message": f"Ошибка соединения: {e}"}
    except requests.exceptions.HTTPError as e:
        return {"ok": False, "message": f"HTTP ошибка: {e}"}
    except requests.exceptions.RequestException as e:
        return {"ok": False, "message": f"Сетевая ошибка: {e}"}
    except json.JSONDecodeError as e:
        return {"ok": False, "message": f"Ошибка парсинга JSON: {e}"}
    except ValueError as e:
        return {"ok": False, "message": f"Ошибка значения: {e}"}
    except Exception as e:
        return {"ok": False, "message": f"Неожиданная ошибка: {e}"}