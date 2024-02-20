import sys
import unittest

# Insert pythonpath into the front of the PATH environment variable, before importing anything from canpy
pythonpath = "/workspace"
try:
    sys.path.index(pythonpath)
except ValueError:
    sys.path.insert(0, pythonpath)

# Run all tests in the /test folder, not just one test_*.py file
loader = unittest.TestLoader()
start_dir = "/workspace/test"
suite = loader.discover(start_dir)

runner = unittest.TextTestRunner()
runner.run(suite)
