
import unittest

# Run all tests in the /test folder, not just one test_*.py file
loader = unittest.TestLoader()
start_dir = '/workspace/test'
suite = loader.discover(start_dir)

runner = unittest.TextTestRunner()
runner.run(suite)
