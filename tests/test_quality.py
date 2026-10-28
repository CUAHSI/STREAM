import unittest

from streamwq.quality import normalize_quality


class NormalizeQualityTests(unittest.TestCase):
    def test_none_disables_quality_filtering(self):
        self.assertIsNone(normalize_quality(None))

    def test_accepts_flag_code(self):
        self.assertEqual(normalize_quality("A"), ["A"])

    def test_accepts_friendly_names(self):
        self.assertEqual(
            normalize_quality(["approved", "Provisional"]),
            ["A", "P"],
        )

    def test_removes_duplicate_flags(self):
        self.assertEqual(normalize_quality(["A", "approved"]), ["A"])

    def test_rejects_unknown_flag(self):
        with self.assertRaisesRegex(ValueError, "Unknown quality flag"):
            normalize_quality("reviewed")

    def test_rejects_empty_selection(self):
        with self.assertRaisesRegex(ValueError, "at least one flag"):
            normalize_quality([])


if __name__ == "__main__":
    unittest.main()
