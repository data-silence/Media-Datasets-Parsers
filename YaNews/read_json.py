import json

with open('smi_db.json', 'r', encoding='utf-8') as f:
    # data = json.load(f)
    data = f.read()
    print(data)

