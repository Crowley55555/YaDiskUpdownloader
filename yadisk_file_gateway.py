def yadisk_file_gateway(arguments):
    """
    Яндекс.Диск helper:
      upload   — загрузка (OAuth, прогресс-бар, умный чанк)
      download — скачивание (OAuth+disk_path ИЛИ public_key; прогресс-бар, умный чанк, resume)
      rename   — переименование (через move)
      delete   — удаление
      list     — список элементов в папке
    """
    import os
    import json
    import pathlib
    import sys
    from typing import Optional, Dict, Any, Iterable
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

    # ---------- args ----------
    action = arguments.get("action")
    token = (arguments.get("oauth_token") or "").strip()
    disk_path = _norm_disk_path(arguments.get("disk_path", ""))
    new_name = arguments.get("new_name")
    local_path = arguments.get("local_path")
    overwrite = bool(arguments.get("overwrite", True))
    show_progress = bool(arguments.get("show_progress", True))
    chunk_override = arguments.get("chunk_size")
    chunk_override = int(chunk_override) if isinstance(chunk_override, int) and chunk_override > 0 else None
    public_key = arguments.get("public_key")
    public_path = arguments.get("public_path")
    resume = bool(arguments.get("resume", False))
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
            if not local_path or not os.path.isfile(local_path):
                return {"ok": False, "message": f"Файл не найден: {local_path}"}

            headers = _auth_headers(token)
            params = {"path": disk_path, "overwrite": "true" if overwrite else "false"}
            r = requests.get(f"{BASE}/resources/upload", headers=headers, params=params, timeout=30)
            if r.status_code not in (200, 201):
                return {"ok": False, "message": _json_error(r)}
            href = r.json().get("href")
            if not href:
                return {"ok": False, "message": "Не получена ссылка для загрузки"}

            pf = ProgressFile(local_path, show_progress)
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

            return {"ok": True, "message": "Файл успешно загружен", "data": {"disk_path": disk_path, "file_size": os.path.getsize(local_path)}}

        # -------- DOWNLOAD --------
        elif action == "download":
            if not local_path:
                return {"ok": False, "message": "Не указан local_path"}

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

            # подготовка локального файла
            pathlib.Path(os.path.dirname(os.path.abspath(local_path)) or ".").mkdir(parents=True, exist_ok=True)
            headers_dl = {}
            existing_bytes = 0
            mode = "wb"
            if resume and os.path.exists(local_path):
                existing_bytes = os.path.getsize(local_path)
                if existing_bytes > 0:
                    headers_dl["Range"] = f"bytes={existing_bytes}-"
                    mode = "ab"

            dl = requests.get(href, headers=headers_dl, stream=True, timeout=600)
            if dl.status_code not in (200, 206):
                return {"ok": False, "message": _json_error(dl)}

            # общий размер, если его отдали
            total = None
            if "Content-Length" in dl.headers:
                try:
                    total = int(dl.headers["Content-Length"]) + existing_bytes
                except Exception:
                    total = None

            # читаем и пишем с прогрессом
            def _choose_chunk_by_total(t):
                return _choose_chunk_size(t, chunk_override)

            chunk_size = _choose_chunk_by_total(total)
            done, last_percent = existing_bytes, [None]

            with open(local_path, mode) as f:
                for chunk in dl.iter_content(chunk_size=chunk_size):
                    if chunk:
                        f.write(chunk)
                        done += len(chunk)
                        _print_progress("Downloading", done, total, last_percent, show_progress)
            _print_progress("Downloading", done, total if total else done, last_percent, show_progress)

            data = {"target_local_path": local_path, "bytes": done, "resumed_from": existing_bytes}
            if token and disk_path:
                data.update({"disk_path": disk_path})
            else:
                data.update({"public_key": public_key, "public_path": public_path})
            return {"ok": True, "message": "Файл успешно скачан", "data": data}

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
            return {"ok": True, "message": f"Файл переименован в {new_name}", "data": {"old_path": from_path, "new_path": to_path}}

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
            simplified = [
                {
                    "name": it.get("name"),
                    "type": it.get("type"),
                    "size": it.get("size"),
                    "mime_type": it.get("mime_type"),
                    "path": it.get("path")
                } for it in items
            ]
            return {"ok": True, "message": f"Элементов: {len(simplified)} из {total}", "data": {"disk_path": disk_path, "total": total, "limit": limit, "offset": offset, "items": simplified}}

        else:
            return {"ok": False, "message": f"Неизвестное действие: {action}"}

    except requests.RequestException as e:
        return {"ok": False, "message": f"Сетевая ошибка: {e}"}
    except Exception as e:
        return {"ok": False, "message": f"Ошибка: {e}"}
