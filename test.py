import unittest
import program as program


class SampleTests(unittest.TestCase):

    @classmethod
    def test_main(cls):
        program.main(True)


if __name__ == "__main__":
    unittest.main()
