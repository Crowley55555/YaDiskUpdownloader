# Яндекс.Диск REST API Gateway

Мини-утилита для работы с Яндекс.Диском через REST API. Подходит для скриптов, ноутбуков и Google Colab.

## Возможности

- **upload** — загрузка локального файла на Диск (OAuth)
- **download** — скачивание файлов (приватно по `oauth_token`+`disk_path` или публично по `public_key`) с поддержкой докачки
- **rename** — переименование файлов (через move)
- **delete** — удаление файлов
- **list** — просмотр содержимого папки

## Параметры (ключевые)

| Параметр | Обязательность | Описание |
|----------|----------------|----------|
| `action` | ✅ Обязателен | `upload` \| `download` \| `rename` \| `delete` \| `list` |
| `oauth_token` | Для приватных операций | OAuth-токен Яндекс.Диска |
| `disk_path` | Для приватных операций | Путь на Диске: `disk:/folder/file.txt` (или `disk:/folder` для list) |
| `local_path` | Для upload/download | Локальный путь в Colab/Drive |
| `public_key`/`public_path` | Для публичного download | Ключ публичного доступа |

### Дополнительные параметры

- `resume` (bool) - включить докачку для download
- `overwrite` (bool) - перезаписать существующий файл
- `show_progress` (bool) - показывать прогресс-бар
- `chunk_size` (int) - размер чанка для загрузки
- `limit`, `offset` - для пагинации в list

## Установка (Google Colab)

```bash
!pip install requests
```

# Важные замечания для Colab
Рабочая директория — /content (Linux-пути), не C:\

Файлы в /content исчезают после рестарта рантайма — для долговременного хранения используйте Google Drive

Для download указывайте полный путь с именем файла, а не только каталог

## Подготовка файлов в Colab
Загрузка файлов с ПК
```python
from google.colab import files
uploaded = files.upload()               # выбрать файл
local_src = list(uploaded.keys())[0]    # имя появится в /content/
