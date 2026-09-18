import unittest

from utils.catalog import DATASETS, RESOURCE_ID, dataset_path


class DatasetPathTests(unittest.TestCase):
    def test_returns_path_for_known_dataset(self):
        self.assertEqual(
            dataset_path("streamflow"),
            f"tonycastronova/{RESOURCE_ID}/data/contents/streamflow",
        )

    def test_every_dataset_uses_current_resource(self):
        for name in DATASETS:
            with self.subTest(dataset=name):
                self.assertIn(RESOURCE_ID, dataset_path(name))

    def test_unknown_dataset_has_clear_error(self):
        with self.assertRaisesRegex(ValueError, "Unknown STREAM dataset"):
            dataset_path("not-a-dataset")


if __name__ == "__main__":
    unittest.main()
