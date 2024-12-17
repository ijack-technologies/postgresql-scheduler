import sys
from pathlib import Path

# Insert pythonpath into the front of the PATH environment variable, before importing anything from project/
pythonpath = str(Path(__file__).parent.parent)
try:
    sys.path.index(pythonpath)
except ValueError:
    sys.path.insert(0, pythonpath)
