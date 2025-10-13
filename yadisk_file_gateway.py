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

    # file-like с прогрессом для upload (надёжнее, чем генератор + ручной Content-Length)
    class ProgressFile:
        def __init__(self, path, show):
            self._f = open(path, "rb")
            self._size = os.path.getsize(path)
            self._read = 0
            self._show = show
            self._last_percent = None
        def __len__(self):
            return self._size
        def read(self, amt=1024 * 1024):
            chunk = self._f.read(amt)
            if chunk:
                self._read += len(chunk)
                if self._show and self._size > 0:
                    percent = int(self._read * 100 / self._size)
                    if percent != self._last_percent:
                        self._last_percent = percent
                        bar_len = 30
                        filled = int(percent * bar_len / 100)
                        bar = "#" * filled + "-" * (bar_len - filled)
                        sys.stdout.write(f"\rUploading [{bar}] {percent:3d}%")
                        sys.stdout.flush()
            else:
                if self._show and self._size > 0:
                    sys.stdout.write("\n")
            return chunk
        def close(self):
            try:
                self._f.close()
            except Exception:
                pass

    # file-like с прогрессом для upload из URL (адаптированная версия ProgressFile)
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
            
            # Сначала загружаем весь файл в память для надежности
            try:
                if self._show:
                    sys.stdout.write("Загрузка файла с URL...\n")
                    sys.stdout.flush()
                
                self._response = requests.get(url, timeout=600, allow_redirects=True)
                self._response.raise_for_status()
                self._data = self._response.content
                self._size = len(self._data)
                
                if self._show:
                    sys.stdout.write(f"Файл загружен, размер: {self._size} байт\n")
                    sys.stdout.flush()
                    
            except Exception as e:
                if self._show:
                    sys.stdout.write(f"\nОшибка при загрузке файла: {e}\n")
                self._data = b""
                self._size = 0
                
        def __len__(self):
            return self._size
            
        def read(self, amt=1024 * 1024):
            if self._closed or self._data is None:
                return b""
            
            # Читаем из загруженных данных
            if self._data_pos >= len(self._data):
                return b""
            
            end_pos = min(self._data_pos + amt, len(self._data))
            chunk = self._data[self._data_pos:end_pos]
            self._data_pos = end_pos
            self._read = self._data_pos
            
            # Показываем прогресс
            if self._show and self._size > 0:
                percent = int(self._read * 100 / self._size)
                if percent != self._last_percent:
                    self._last_percent = percent
                    bar_len = 30
                    filled = int(percent * bar_len / 100)
                    bar = "#" * filled + "-" * (bar_len - filled)
                    sys.stdout.write(f"\rUploading from URL [{bar}] {percent:3d}%")
                    sys.stdout.flush()
                    
                if self._read >= self._size:
                    sys.stdout.write("\n")
                    
            return chunk
                
        def close(self):
            self._closed = True
            try:
                if self._response:
                    self._response.close()
            except Exception:
                pass

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
            
            # Проверяем, что указан либо local_path, либо file_url
            if not local_path and not file_url:
                return {"ok": False, "message": "Для upload требуется либо local_path, либо file_url"}
            if local_path and file_url:
                return {"ok": False, "message": "Укажите либо local_path, либо file_url, но не оба"}
            
            # Проверяем локальный файл
            if local_path and not os.path.isfile(local_path):
                return {"ok": False, "message": f"Файл не найден: {local_path}"}

            headers = _auth_headers(token)
            params = {"path": disk_path, "overwrite": "true" if overwrite else "false"}
            r = requests.get(f"{BASE}/resources/upload", headers=headers, params=params, timeout=30)
            if r.status_code not in (200, 201):
                return {"ok": False, "message": _json_error(r)}
            href = r.json().get("href")
            if not href:
                return {"ok": False, "message": "Не получена ссылка для загрузки"}

            # Выбираем класс в зависимости от источника
            if local_path:
                pf = ProgressFile(local_path, show_progress)
                file_size = os.path.getsize(local_path)
            else:
                pf = ProgressURLFile(file_url, show_progress)
                file_size = len(pf)  # размер из HEAD запроса
                
            try:
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

            return {"ok": True, "message": "Файл успешно загружен", "data": {"disk_path": disk_path, "file_size": file_size}}

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
                file_link = file_data.get("public_url") or f"https://disk.yandex.ru/client/disk/{to_path.replace('disk:/', '')}"
            else:
                file_link = f"https://disk.yandex.ru/client/disk/{to_path.replace('disk:/', '')}"
            
            return {"ok": True, "message": f"Файл переименован в {new_name}", "data": {"old_path": from_path, "new_path": to_path, "file_url": file_link}}

        # -------- DELETE --------
        elif action == "delete":
            if not token:
                return {"ok": False, "message": "Для delete требуется oauth_token"}
            if not disk_path:
                return {"ok": False, "message": "Для delete требуется disk_path"}
            headers = _auth_headers(token)
            r = requests.delete(f"{BASE}/resources", headers=headers, params={"path": disk_path, "permanently": "true"}, timeout=30)
            if r.status_code not in (202, 204):
                return {"ok": False, "message": _json_error(r)}
            return {"ok": True, "message": "Файл удалён", "data": {"disk_path": disk_path}}

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
            
            return {"ok": True, "message": f"Элементов: {len(simplified)} из {total}", "data": {"disk_path": disk_path, "total": total, "limit": limit, "offset": offset, "items": simplified}}

        else:
            return {"ok": False, "message": f"Неизвестное действие: {action}"}

    except requests.RequestException as e:
        return {"ok": False, "message": f"Сетевая ошибка: {e}"}
    except Exception as e:
        return {"ok": False, "message": f"Ошибка: {e}"}
