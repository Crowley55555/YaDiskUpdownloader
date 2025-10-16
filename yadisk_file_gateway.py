def yadisk_file_gateway(arguments):
    """
    Яндекс.Диск helper (только ссылки):
      upload   — загрузка файла по URL на Яндекс.Диск (возвращает ссылку на загруженный файл)
      download — получение ссылки для скачивания (OAuth+disk_path ИЛИ public_key)
      rename   — переименование (через move)
      delete   — удаление
      list     — список элементов в папке
    """
    import os
    import json
    import pathlib
    import sys
    import hashlib
    import re  # Для парсинга HTML
    from typing import Optional, Dict, Any, Iterable
    import requests
    import urllib.request
    import urllib.parse

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
        if not public_url or not public_url.startswith("https://disk.yandex.ru/"):
            if show_progress:
                sys.stdout.write(f"URL не является публичной ссылкой Яндекс.Диска: {public_url}\n")
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

    # file-like с прогрессом для upload из URL (загрузка в буфер, затем выгрузка)
    class ProgressURLFile:
        def __init__(self, url, show):
            self._url = url
            self._show = show
            self._last_percent = None
            self._read = 0
            self._size = 0
            self._response = None
            self._closed = False
            self._data = None
            self._data_pos = 0
            self._error = None

            # Загружаем файл в буфер с надежными настройками
            try:
                if self._show:
                    sys.stdout.write("Загрузка файла с URL в буфер...\n")
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

                # Используем stream=True для более точного контроля
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
                    self._data = b""
                    self._size = 0
                    return

                # Проверка Content-Disposition для определения имени файла
                content_disposition = self._response.headers.get('Content-Disposition', '')
                if 'filename=' in content_disposition:
                    filename = content_disposition.split('filename=')[1].strip('"\'')
                    if self._show:
                        sys.stdout.write(f"Имя файла по заголовку: {filename}\n")

                # Читаем данные по частям и сохраняем в буфер
                self._data = b""
                total_read = 0
                chunk_size = 8192
                max_chunks = 100000  # Ограничение на количество чанков для безопасности

                for i, chunk in enumerate(self._response.iter_content(chunk_size=chunk_size)):
                    if chunk:  # Пропускаем пустые чанки
                        self._data += chunk
                        total_read += len(chunk)
                        if self._show and i % 10 == 0:  # Обновление прогресса каждые 10 чанков
                            sys.stdout.write(f"\rЗагружено: {total_read} байт")
                            sys.stdout.flush()

                    if i >= max_chunks:
                        # Ограничиваем размер файла для предотвращения переполнения памяти
                        if self._show:
                            sys.stdout.write(f"\nДостигнут лимит чанков ({max_chunks}), прекращаем загрузку.\n")
                        break

                self._size = len(self._data)

                # Проверяем, что файл имеет размер > 0
                if self._size == 0:
                    self._error = "Файл пустой (размер 0 байт)"
                    if self._show:
                        sys.stdout.write(f"\nОшибка: {self._error}\n")
                    return

                if self._show:
                    sys.stdout.write(f"\nФайл загружен в буфер, размер: {self._size} байт\n")
                    sys.stdout.write(f"Content-Type: {content_type}\n")
                    sys.stdout.write("Начинаем выгрузку на Яндекс.Диск...\n")
                    sys.stdout.flush()

            except requests.exceptions.RequestException as e:
                self._error = f"Ошибка сети при загрузке файла: {e}"
                if self._show:
                    sys.stdout.write(f"\nОшибка: {self._error}\n")
                self._data = b""
                self._size = 0
            except Exception as e:
                self._error = f"Неожиданная ошибка при загрузке файла: {e}"
                if self._show:
                    sys.stdout.write(f"\nОшибка: {self._error}\n")
                self._data = b""
                self._size = 0

        def __len__(self):
            return self._size

        def read(self, amt=1024 * 1024):
            if self._closed or self._data is None:
                return b""

            # Читаем из буфера
            if self._data_pos >= len(self._data):
                return b""

            end_pos = min(self._data_pos + amt, len(self._data))
            chunk = self._data[self._data_pos:end_pos]
            self._data_pos = end_pos
            self._read = self._data_pos

            # Показываем прогресс выгрузки на диск
            if self._show and self._size > 0:
                percent = int(self._read * 100 / self._size)
                if percent != self._last_percent:
                    self._last_percent = percent
                    bar_len = 30
                    filled = int(percent * bar_len / 100)
                    bar = "#" * filled + "-" * (bar_len - filled)
                    sys.stdout.write(f"\rВыгрузка на диск [{bar}] {percent:3d}%")
                    sys.stdout.flush()

                if self._read >= self._size:
                    sys.stdout.write("\n")

            return chunk

        def close(self):
            self._closed = True
            # Освобождаем буфер из памяти
            if self._data is not None:
                if self._show:
                    sys.stdout.write("Освобождение буфера из памяти...\n")
                    sys.stdout.flush()
                self._data = None

            try:
                if self._response:
                    self._response.close()
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

    if not action:
        return {"ok": False, "message": "Не указан action"}

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
            r = requests.get(f"{BASE}/resources/upload", headers=headers, params=params, timeout=30)
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

            file_size = len(pf)  # размер из буфера

            # Проверяем, удалось ли загрузить файл в буфер
            if file_size == 0:
                if show_progress:
                    sys.stdout.write("Файл не был загружен в буфер. Проверьте URL и подключение.\n")
                return {"ok": False, "message": "Не удалось загрузить файл с URL. Проверьте URL и подключение."}

            try:
                # Используем data=pf, который реализует read()
                put = requests.put(
                    href,
                    data=pf,
                    headers={"Content-Type": "application/octet-stream"},
                    timeout=600
                )
            finally:
                pf.close()

            if put.status_code not in (200, 201, 202):
                return {"ok": False, "message": _json_error(put)}

            # --- Получение ссылки на загруженный файл ---
            # После успешной загрузки получаем публичную ссылку
            try:
                file_info = requests.get(f"{BASE}/resources", headers=headers, params={"path": disk_path}, timeout=30)
                if file_info.status_code == 200:
                    file_data = file_info.json()
                    file_url_on_disk = file_data.get("public_url")
                    if file_url_on_disk:
                        if show_progress:
                            sys.stdout.write(f"Получена публичная ссылка на файл: {file_url_on_disk}\n")
                    else:
                        # Если нет public_url, формируем ссылку вручную
                        # Это не всегда корректно, но может быть полезно
                        file_url_on_disk = f"https://disk.yandex.ru/client/disk/{disk_path.replace('disk:/', '')}"
                        if show_progress:
                            sys.stdout.write(f"Сформирована ссылка на файл: {file_url_on_disk}\n")
                else:
                    file_url_on_disk = f"https://disk.yandex.ru/client/disk/{disk_path.replace('disk:/', '')}"
                    if show_progress:
                        sys.stdout.write(
                            f"Не удалось получить публичную ссылку, сформирована ссылка: {file_url_on_disk}\n")
            except Exception as e:
                file_url_on_disk = f"https://disk.yandex.ru/client/disk/{disk_path.replace('disk:/', '')}"
                if show_progress:
                    sys.stdout.write(f"Ошибка получения ссылки: {e}. Используется стандартная: {file_url_on_disk}\n")

            return {"ok": True, "message": "Файл успешно загружен", "data": {
                "disk_path": disk_path,
                "file_size": file_size,
                "file_url": file_url_on_disk  # <-- Добавлена ссылка на загруженный файл
            }}

        # -------- DOWNLOAD --------
        elif action == "download":
            # приватный файл (OAuth + disk_path)
            if token and disk_path:
                headers = _auth_headers(token)
                r = requests.get(f"{BASE}/resources/download", headers=headers, params={"path": disk_path}, timeout=30)
            # публичный файл (без OAuth)
            elif public_key:
                params = {"public_key": public_key}
                if public_path:
                    params["path"] = public_path
                r = requests.get(f"{BASE}/public/resources/download", params=params, timeout=30)
            else:
                return {"ok": False, "message": "Для download укажи либо oauth_token+disk_path, либо public_key"}

            if r.status_code != 200:
                return {"ok": False, "message": _json_error(r)}
            href = r.json().get("href")
            if not href:
                return {"ok": False, "message": "Сервис не вернул href для скачивания"}

            # Получаем информацию о файле для определения размера
            file_size = None
            if token and disk_path:
                file_info = requests.get(f"{BASE}/resources", headers=headers, params={"path": disk_path}, timeout=30)
                if file_info.status_code == 200:
                    file_data = file_info.json()
                    file_size = file_data.get("size")

            data = {"download_url": href, "file_size": file_size}
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
            parent_dir = "/".join(from_path.split("/")[:-1])
            to_path = f"{parent_dir}/{new_name}" if parent_dir else new_name
            params = {"from": from_path, "path": to_path, "overwrite": "true"}
            r = requests.post(f"{BASE}/resources/move", headers=headers, params=params, timeout=30)
            if r.status_code not in (200, 201, 202):
                return {"ok": False, "message": _json_error(r)}

            # Получаем ссылку на переименованный файл
            file_info = requests.get(f"{BASE}/resources", headers=headers, params={"path": to_path}, timeout=30)
            if file_info.status_code == 200:
                file_data = file_info.json()
                file_link = file_data.get(
                    "public_url") or f"https://disk.yandex.ru/client/disk/{to_path.replace('disk:/', '')}"
            else:
                file_link = f"https://disk.yandex.ru/client/disk/{to_path.replace('disk:/', '')}"

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
                    item_data["file_url"] = f"https://disk.yandex.ru/client/disk/{item_path.replace('disk:/', '')}"
                simplified.append(item_data)

            return {"ok": True, "message": f"Элементов: {len(simplified)} из {total}",
                    "data": {"disk_path": disk_path, "total": total, "limit": limit, "offset": offset,
                             "items": simplified}}

        else:
            return {"ok": False, "message": f"Неизвестное действие: {action}"}

    except requests.RequestException as e:
        return {"ok": False, "message": f"Сетевая ошибка: {e}"}
    except Exception as e:
        return {"ok": False, "message": f"Ошибка: {e}"}