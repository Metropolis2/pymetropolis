import json
import tempfile
from pathlib import Path

from pymetropolis.metro_pipeline.file import MetroJsonFile


class JsonFile(MetroJsonFile):
    path = "results/data.json"


def test_json_file_round_trip():
    value = {"b": [1, 2.5, None], "a": {"nested": True, "text": "x"}}
    with tempfile.TemporaryDirectory() as tmp_dir:
        file = JsonFile.from_dir(Path(tmp_dir))
        assert file.read_if_exists() is None
        file.write(value)
        # Same formatting as the `json.dumps` calls the class replaces.
        assert file.get_path().read_text() == json.dumps(value, indent=2, sort_keys=True)
        assert file.read() == value
