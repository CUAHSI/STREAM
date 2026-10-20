import importlib.util
import unittest
from importlib.metadata import distribution

import streamwq as stream


class InstallationTests(unittest.TestCase):
    def test_installed_distribution_exposes_the_renamed_package(self):
        package = distribution("streamwq")

        self.assertEqual(package.metadata["Name"], "streamwq")
        self.assertEqual(package.metadata["Requires-Python"], "<3.15,>=3.14")
        self.assertTrue(callable(stream.get_metadata))
        self.assertIsNone(importlib.util.find_spec("stream_utils"))

    def test_dataframe_support_is_optional_and_arrow_range_is_preserved(self):
        requirements = distribution("streamwq").requires

        self.assertIn("pyarrow<26,>=25.0.0", requirements)
        self.assertIn('pandas; extra == "pandas"', requirements)
        required = [item for item in requirements if "extra ==" not in item]
        for name in ["pandas", "geopandas", "xarray", "pyright", "build"]:
            self.assertFalse(any(item.startswith(name) for item in required), name)


if __name__ == "__main__":
    unittest.main()