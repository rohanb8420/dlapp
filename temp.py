import json
from pathlib import Path

nb_path = Path('reader_app.ipynb')
nb = json.loads(nb_path.read_text())

for cell in nb['cells']:
    if cell.get('cell_type') != 'code':
        continue
    text = ''.join(cell.get('source', []))
    if text and not text.endswith('\n'):
        text += '\n'
    if '"\n".join' not in text and '"\n"' not in text and '"\n"' not in text:
        # still perform replacement to ensure actual newline sequences fixed
        pass
    text = text.replace('"\n"', '"\\n"')
    text = text.replace('"\\n"', '"\\n"')
    text = text.replace('"\n"', '"\\n"')
    text = text.replace('"\n"', '"\\n"')
    text = text.replace('"\n"', '"\\n"')
    # Above repetitious lines? need to carefully handle actual newline char sequences
