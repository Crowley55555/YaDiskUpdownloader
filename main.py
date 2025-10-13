from yadisk_file_gateway import yadisk_file_gateway

# Загрузка файла по URL на Яндекс.Диск
args = {
    "action": "upload",
    "oauth_token": "y0__xDIuu2XBxiR7TogncG82BTbb9rgDpTKFA68a9jdILc5cHhkcg",
    "disk_path": "disk:/img1.png",
    "file_url": "https://drive.google.com/file/d/1VDw4A0vc0eovRMscfWmA_nP9n35Tn5BC/view?usp=sharing",  # ссылка на файл в интернете
    "overwrite": True,          # перезаписать, если уже есть
    "show_progress": True       # показать прогресс-бар
}
print(yadisk_file_gateway(args))

# # Получение ссылки для скачивания приватного файла (по OAuth)
# args = {
#     "action": "download",
#     "oauth_token": "<YA_OAUTH_TOKEN>",
#     "disk_path": "disk:/file1.mp3"
# }
# print(yadisk_file_gateway(args))
#
# # Получение ссылки для скачивания по публичной ссылке (без OAuth)
# args = {
#     "action": "download",
#     "public_key": "https://disk.yandex.ru/d/NS-00uW07T-EsQ",
#     "public_path": ""   # опционально (если в опубликованной папке)
# }
# print(yadisk_file_gateway(args))
#
# Переименование файла
# args = {
#     "action": "rename",
#     "oauth_token": "<YA_OAUTH_TOKEN>",
#     "disk_path": "disk:/file1.mp3",
#     "new_name": "file55555.mp3"
# }
# print(yadisk_file_gateway(args))
#
# Удаление файла
# args = {
#     "action": "delete",
#     "oauth_token": "<YA_OAUTH_TOKEN>",
#     "disk_path": "disk:/file2.mp3"
# }
# print(yadisk_file_gateway(args))
#
# # Листинг содержимого папки
# args = {
#     "action": "list",
#     "oauth_token": "<YA_OAUTH_TOKEN>",
#     "disk_path": "disk:/",    # путь к папке
#     "limit": 50,                     # сколько элементов вернуть
#     "offset": 0                      # с какого индекса начать (для пагинации)
# }
# print(yadisk_file_gateway(args))

