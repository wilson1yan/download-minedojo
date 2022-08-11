import json

info = json.load(open('subset_info.json', 'r'))
bad_ids = []
for id, (_, status) in info.items():
    if status != 'success':
        print(id, status)
