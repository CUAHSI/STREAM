import unittest

from stream_utils.catalog import (
    ANTHROPOGENIC_VARIABLES,
    DATASETS,
    GRAB_SAMPLE_VARIABLES,
    LAND_COVER_VARIABLES,
    METEOROLOGY_VARIABLES,
    RESOURCE_ID,
    WATER_QUALITY_VARIABLES,
    TIME_COLUMNS,
    dataset_path,
)


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

    def test_streamflow_is_not_a_water_quality_variable(self):
        self.assertNotIn("Q_m3s", WATER_QUALITY_VARIABLES)

    def test_grab_sample_variables_are_cataloged(self):
        self.assertIn("DOC_mgL", GRAB_SAMPLE_VARIABLES)
        self.assertIn("Cl_mgL", GRAB_SAMPLE_VARIABLES)

    def test_meteorology_variables_are_cataloged(self):
        self.assertIn("APCP_surface", METEOROLOGY_VARIABLES)
        self.assertIn("TMP_2maboveground", METEOROLOGY_VARIABLES)
        self.assertEqual(TIME_COLUMNS["meteorology"], "DateTime")

    def test_land_cover_variables_and_time_column_are_cataloged(self):
        self.assertEqual(len(LAND_COVER_VARIABLES), 16)
        self.assertIn("Cultivated Crops", LAND_COVER_VARIABLES)
        self.assertEqual(TIME_COLUMNS["land_cover"], "DateTime")

    def test_anthropogenic_variables_and_time_column_are_cataloged(self):
        self.assertEqual(len(ANTHROPOGENIC_VARIABLES), 16)
        self.assertIn("salt_pound", ANTHROPOGENIC_VARIABLES)
        self.assertIn("irrig", ANTHROPOGENIC_VARIABLES)
        self.assertEqual(TIME_COLUMNS["anthropogenic"], "DateTime")


if __name__ == "__main__":
    unittest.main()
