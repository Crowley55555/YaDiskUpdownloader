from yadisk_file_gateway import yadisk_file_gateway

# Загрузка файла на Яндекс.Диск
args = {
    "action": "upload",
    "oauth_token": "<YA_OAUTH_TOKEN>",
    "disk_path": "disk:/file2.mp3",
    "local_path": "D:/file1.mp3",
    "overwrite": True,          # перезаписать, если уже есть
    "show_progress": True       # показать прогресс-бар
}
print(yadisk_file_gateway(args))

# Скачивание приватного файла (по OAuth)
args = {
    "action": "download",
    "oauth_token": "<YA_OAUTH_TOKEN>",
    "disk_path": "disk:/file1.mp3",
    "local_path": "D:/file5.mp3",
    "resume": True,             # возобновить, если частично скачан
    "show_progress": True
}
print(yadisk_file_gateway(args))

# Скачивание по публичной ссылке (без OAuth)
args = {
    "action": "download",
    "public_key": "https://disk.yandex.ru/d/nkiBskZzyG5rNw",
    "public_path": "",   # опционально (если в опубликованной папке)
    "local_path": "D:/file3.mp3",
    "resume": True,                      # продолжить, если файл частично есть
    "show_progress": True
}
print(yadisk_file_gateway(args))

# Переименование файла
args = {
    "action": "rename",
    "oauth_token": "<YA_OAUTH_TOKEN>",
    "disk_path": "disk:/file1.mp3",
    "new_name": "file2525.mp3"
}
print(yadisk_file_gateway(args))

# Удаление файла
args = {
    "action": "delete",
    "oauth_token": "<YA_OAUTH_TOKEN>",
    "disk_path": "disk:/backups/db_2025.dump"
}
print(yadisk_file_gateway(args))

# Листинг содержимого папки
args = {
    "action": "list",
    "oauth_token": "<YA_OAUTH_TOKEN>",
    "disk_path": "disk:/",    # путь к папке
    "limit": 50,                     # сколько элементов вернуть
    "offset": 0                      # с какого индекса начать (для пагинации)
}
print(yadisk_file_gateway(args))

