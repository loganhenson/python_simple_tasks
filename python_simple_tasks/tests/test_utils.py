import unittest
from python_simple_tasks.utils import extract_lambda_args


class TestUtils(unittest.TestCase):
    def test_extract_lambda_args_no_args(self):
        """Test lambda with no arguments."""
        func = lambda: 42
        args = extract_lambda_args(func)
        self.assertEqual(args, {})

    def test_extract_lambda_args_with_args(self):
        """Test lambda with arguments."""
        func = lambda x=1, y=2: x + y
        args = extract_lambda_args(func)
        self.assertEqual(args, {"x": 1, "y": 2})

    def test_extract_lambda_args_invalid_function(self):
        """Test invalid input for lambda extraction."""
        with self.assertRaises(TypeError):
            extract_lambda_args("not a function")


if __name__ == "__main__":
    unittest.main()
