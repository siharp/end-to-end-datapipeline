import json
import os

folder_file = os.path.join(
    os.getcwd(), 'olist_warehouse', 'target', 'manifest.json')

with open(folder_file, 'r') as file:
    manifest = json.load(file)

model = 0
test = 0

for i in manifest.get('nodes'):
    if i.split('.')[0] == 'model':
        model += 1
    if i.split('.')[0] == 'model':
        test += 1
print(model, test)
