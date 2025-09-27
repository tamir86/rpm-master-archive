from pathlib import Path
import os, tempfile

def atomic_write_text(path: Path, text: str):
    path = Path(path)
    with tempfile.NamedTemporaryFile('w', delete=False, dir=str(path.parent), newline='') as tmp:
        tmp.write(text)
        tmp_name = tmp.name
    os.replace(tmp_name, path)  # atomic on NTFS

def atomic_write_bytes(path: Path, data: bytes):
    path = Path(path)
    with tempfile.NamedTemporaryFile('wb', delete=False, dir=str(path.parent)) as tmp:
        tmp.write(data)
        tmp_name = tmp.name
    os.replace(tmp_name, path)
