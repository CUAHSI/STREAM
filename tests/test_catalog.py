import unittest

import streamwq as stream


class CatalogTests(unittest.TestCase):
    def test_discovers_supported_dataset_names_and_variables(self):
        self.assertEqual(
            stream.catalog.DATASETS,
            (
                "metadata", "static_catchment_characteristics", "watersheds",
                "water_quality", "meteorology", "land_cover", "anthropogenic",
                "streamflow", "grab_samples", "huc_lookup",
            ),
        )
        self.assertEqual(set(stream.catalog.VARIABLES), set(stream.catalog.DATASETS))
        self.assertIn("WTemp_C", stream.catalog.VARIABLES["water_quality"])
        self.assertNotIn("Q_m3s", stream.catalog.VARIABLES["water_quality"])
        self.assertEqual(stream.catalog.VARIABLES["streamflow"], ("Q_m3s",))
        self.assertEqual(stream.catalog.VARIABLES["grab_samples"][0], "SSC_mgL")
        self.assertEqual(stream.catalog.VARIABLES["meteorology"][0], "APCP_surface")
        self.assertIn("Open Water", stream.catalog.VARIABLES["land_cover"])
        self.assertIn("so4_tw", stream.catalog.VARIABLES["anthropogenic"])
        self.assertEqual(stream.catalog.VARIABLES["metadata"], ())

    def test_catalog_does_not_expose_storage_or_connection_details(self):
        for name in ["dataset_path", "RESOURCE_ID", "RESOURCE_OWNER", "S3HydroShare"]:
            self.assertFalse(hasattr(stream.catalog, name), name)
        with self.assertRaises(TypeError):
            stream.catalog.VARIABLES["water_quality"] = ()


if __name__ == "__main__":
    unittest.main()