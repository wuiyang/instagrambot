
import json
from pathlib import Path

language_file = Path("./language.json")
file = open(language_file)
lng = json.load(file)

def get_text(text_key):
    if text_key in lng:
        return lng[text_key]
    return "Unable to intepret output message, please DM developer with code [{e}]".format(e = text_key)