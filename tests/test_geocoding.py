"""
Tests for geocoding components in beisser_sync.py.

Run with: python -m pytest tests/test_geocoding.py -v
  or:      python -m unittest tests.test_geocoding -v
"""

import json
import os
import sys
import tempfile
import types
import unittest
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

# Make the project root importable when running from the repo root
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Stub out DB drivers and dotenv that are only available on the Pi so tests can run anywhere.
for _mod in ("psycopg2", "psycopg2.sql", "pyodbc", "dotenv"):
    if _mod not in sys.modules:
        _stub = types.ModuleType(_mod)
        if _mod == "dotenv":
            _stub.load_dotenv = lambda *a, **kw: None  # type: ignore[attr-defined]
        sys.modules[_mod] = _stub
# psycopg2.sql needs the `sql` attribute on the psycopg2 stub
sys.modules["psycopg2"].sql = sys.modules["psycopg2.sql"]  # type: ignore[attr-defined]

from beisser_sync import (
    ShipToGeocoder,
    addresses_equal,
    build_address_key,
    normalize_text,
    normalize_zip,
    should_geocode_shipto,
    split_house_and_street,
    transform_shipto_row,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _default_geocode_settings(**overrides):
    base = {
        "enabled": True,
        "geojson_path": "",
        "fallback_nominatim": False,
        "batch_size": 100,
        "require_missing_only": True,
        "retry_failed": False,
        "nominatim_user_agent": "beisser-test/1.0",
        "nominatim_min_interval_seconds": 0.0,
    }
    base.update(overrides)
    return base


def _make_geojson(features: list) -> str:
    return json.dumps({"type": "FeatureCollection", "features": features})


def _make_feature(address_1, city, state, zip_, lat, lon):
    return {
        "type": "Feature",
        "geometry": {"type": "Point", "coordinates": [lon, lat]},
        "properties": {
            "address_1": address_1,
            "city": city,
            "state": state,
            "zip": zip_,
        },
    }


# ---------------------------------------------------------------------------
# normalize_text
# ---------------------------------------------------------------------------

class TestNormalizeText(unittest.TestCase):

    def test_basic(self):
        self.assertEqual(normalize_text("Hello World"), "hello world")

    def test_strips_special_chars(self):
        self.assertEqual(normalize_text("123 Main St."), "123 main st")

    def test_collapses_whitespace(self):
        self.assertEqual(normalize_text("  foo   bar  "), "foo bar")

    def test_none(self):
        self.assertEqual(normalize_text(None), "")

    def test_empty(self):
        self.assertEqual(normalize_text(""), "")

    def test_numbers_preserved(self):
        self.assertEqual(normalize_text("4B"), "4b")


# ---------------------------------------------------------------------------
# normalize_zip
# ---------------------------------------------------------------------------

class TestNormalizeZip(unittest.TestCase):

    def test_five_digit(self):
        self.assertEqual(normalize_zip("12345"), "12345")

    def test_zip_plus_four(self):
        self.assertEqual(normalize_zip("12345-6789"), "12345")

    def test_none(self):
        self.assertEqual(normalize_zip(None), "")

    def test_empty(self):
        self.assertEqual(normalize_zip(""), "")

    def test_strips_letters(self):
        self.assertEqual(normalize_zip("AB123"), "123")

    def test_truncates_long(self):
        self.assertEqual(normalize_zip("123456789"), "12345")


# ---------------------------------------------------------------------------
# split_house_and_street
# ---------------------------------------------------------------------------

class TestSplitHouseAndStreet(unittest.TestCase):

    def test_numbered_address(self):
        house, street = split_house_and_street("123 Main Street")
        self.assertEqual(house, "123")
        self.assertIn("main", street)
        # street suffix should be stripped
        self.assertNotIn("street", street)

    def test_no_number(self):
        house, street = split_house_and_street("Main Street")
        self.assertEqual(house, "")
        self.assertIn("main", street)

    def test_none(self):
        house, street = split_house_and_street(None)
        self.assertEqual(house, "")
        self.assertEqual(street, "")

    def test_empty(self):
        house, street = split_house_and_street("")
        self.assertEqual(house, "")
        self.assertEqual(street, "")

    def test_suffix_variants(self):
        for suffix in ["Ave", "Blvd", "Dr", "Rd", "Ln", "Ct", "Hwy"]:
            _, street = split_house_and_street(f"100 Oak {suffix}")
            self.assertNotIn(suffix.lower(), street, f"Suffix '{suffix}' should be stripped")


# ---------------------------------------------------------------------------
# build_address_key
# ---------------------------------------------------------------------------

class TestBuildAddressKey(unittest.TestCase):

    def test_basic(self):
        row = {"address_1": "123 Main St", "city": "Springfield", "state": "IL", "zip": "62701"}
        key = build_address_key(row)
        self.assertIn("123", key)
        self.assertIn("springfield", key)
        self.assertIn("62701", key)

    def test_consistent(self):
        row = {"address_1": "123 Main St", "city": "Springfield", "state": "IL", "zip": "62701"}
        self.assertEqual(build_address_key(row), build_address_key(row))

    def test_zip_normalised(self):
        row1 = {"address_1": "1 Oak Ave", "city": "Salem", "state": "OR", "zip": "97301-1234"}
        row2 = {"address_1": "1 Oak Ave", "city": "Salem", "state": "OR", "zip": "97301"}
        self.assertEqual(build_address_key(row1), build_address_key(row2))


# ---------------------------------------------------------------------------
# addresses_equal
# ---------------------------------------------------------------------------

class TestAddressesEqual(unittest.TestCase):

    def _row(self, **kwargs):
        defaults = {"address_1": "100 Main St", "address_2": None, "city": "Lima", "state": "OH", "zip": "45801"}
        defaults.update(kwargs)
        return defaults

    def test_equal(self):
        self.assertTrue(addresses_equal(self._row(), self._row()))

    def test_case_insensitive(self):
        self.assertTrue(addresses_equal(self._row(city="LIMA"), self._row(city="lima")))

    def test_different_city(self):
        self.assertFalse(addresses_equal(self._row(city="Lima"), self._row(city="Findlay")))

    def test_different_address(self):
        self.assertFalse(addresses_equal(self._row(address_1="100 Main St"), self._row(address_1="200 Main St")))

    def test_different_zip(self):
        self.assertFalse(addresses_equal(self._row(zip="45801"), self._row(zip="45802")))


# ---------------------------------------------------------------------------
# transform_shipto_row
# ---------------------------------------------------------------------------

class TestTransformShiptoRow(unittest.TestCase):

    def _source(self, **overrides):
        base = {
            "CUST_KEY": "C001",
            "SEQ_NUM": "1",
            "SHIPTO_NAME": "Acme Corp",
            "ADDRESS_1": "100 Industrial Way",
            "ADDRESS_2": None,
            "CITY": "Lima",
            "STATE": "OH",
            "ZIP": "45801",
            "ATTENTION": "John",
            "PHONE": "419-555-0100",
            "BRANCH_CODE": "LMA",
            "PROWID": 9001,
            "UPDATED_AT": None,
        }
        base.update(overrides)
        return base

    def test_basic_mapping(self):
        row = transform_shipto_row(self._source())
        self.assertEqual(row["cust_key"], "C001")
        self.assertEqual(row["seq_num"], "1")
        self.assertEqual(row["city"], "Lima")
        self.assertEqual(row["source_prowid"], 9001)

    def test_case_insensitive_keys(self):
        src = {k.lower(): v for k, v in self._source().items()}
        row = transform_shipto_row(src)
        self.assertEqual(row["cust_key"], "C001")

    def test_seq_num_as_string(self):
        row = transform_shipto_row(self._source(SEQ_NUM=3))
        self.assertIsInstance(row["seq_num"], str)
        self.assertEqual(row["seq_num"], "3")

    def test_alternate_keys(self):
        src = {
            "customer_key": "C002",
            "shipto_seq": "2",
            "address1": "50 Elm St",
            "postal_code": "44001",
        }
        row = transform_shipto_row(src)
        self.assertEqual(row["cust_key"], "C002")
        self.assertEqual(row["seq_num"], "2")
        self.assertEqual(row["address_1"], "50 Elm St")
        self.assertEqual(row["zip"], "44001")


# ---------------------------------------------------------------------------
# should_geocode_shipto
# ---------------------------------------------------------------------------

class TestShouldGeocodeShipto(unittest.TestCase):

    def _settings(self, **overrides):
        return _default_geocode_settings(**overrides)

    def _row(self):
        return {"address_1": "100 Main St", "address_2": None, "city": "Lima", "state": "OH", "zip": "45801"}

    def _existing(self, lat=41.0, lon=-83.0, source="local_geojson_exact"):
        # Include address fields matching _row() so addresses_equal returns True
        return {
            "address_1": "100 Main St", "address_2": None,
            "city": "Lima", "state": "OH", "zip": "45801",
            "lat": lat, "lon": lon, "geocode_source": source,
        }

    def test_geocode_when_no_existing(self):
        self.assertTrue(should_geocode_shipto(self._row(), None, self._settings()))

    def test_skip_when_coords_exist_and_require_missing_only(self):
        self.assertFalse(should_geocode_shipto(self._row(), self._existing(), self._settings(require_missing_only=True)))

    def test_geocode_when_require_missing_only_false_and_no_coords(self):
        existing = self._existing(lat=None, lon=None, source="")
        self.assertTrue(should_geocode_shipto(self._row(), existing, self._settings(require_missing_only=False)))

    def test_geocode_when_address_changed(self):
        existing = {**self._existing(), "address_1": "999 Different Rd", "address_2": None, "city": "Lima", "state": "OH", "zip": "45801"}
        self.assertTrue(should_geocode_shipto(self._row(), existing, self._settings()))

    def test_skip_failed_when_retry_false(self):
        existing = self._existing(source="failed")
        # address same, coords present — would normally skip, but let's confirm retry_failed=False skips failed
        existing_no_coords = {**existing, "lat": None, "lon": None}
        self.assertFalse(should_geocode_shipto(self._row(), existing_no_coords, self._settings(retry_failed=False)))

    def test_geocode_failed_when_retry_true(self):
        existing = {"lat": None, "lon": None, "geocode_source": "failed", "address_1": "100 Main St", "address_2": None, "city": "Lima", "state": "OH", "zip": "45801"}
        self.assertTrue(should_geocode_shipto(self._row(), existing, self._settings(retry_failed=True)))

    def test_disabled(self):
        self.assertFalse(should_geocode_shipto(self._row(), None, self._settings(enabled=False)))


# ---------------------------------------------------------------------------
# ShipToGeocoder
# ---------------------------------------------------------------------------

class TestShipToGeocoderDisabled(unittest.TestCase):

    def test_disabled_returns_failed(self):
        geocoder = ShipToGeocoder(_default_geocode_settings(enabled=False))
        lat, lon, source = geocoder.geocode({"address_1": "100 Main St", "city": "Lima", "state": "OH", "zip": "45801"})
        self.assertIsNone(lat)
        self.assertIsNone(lon)
        self.assertEqual(source, "failed")


class TestShipToGeocoderNoFile(unittest.TestCase):

    def test_empty_path_returns_failed(self):
        geocoder = ShipToGeocoder(_default_geocode_settings(enabled=True, geojson_path=""))
        lat, lon, source = geocoder.geocode({"address_1": "100 Main St", "city": "Lima", "state": "OH", "zip": "45801"})
        self.assertIsNone(lat)
        self.assertIsNone(lon)
        self.assertEqual(source, "failed")

    def test_missing_file_returns_failed(self):
        geocoder = ShipToGeocoder(_default_geocode_settings(enabled=True, geojson_path="/nonexistent/path.geojson"))
        lat, lon, source = geocoder.geocode({"address_1": "100 Main St", "city": "Lima", "state": "OH", "zip": "45801"})
        self.assertIsNone(lat)
        self.assertIsNone(lon)
        self.assertEqual(source, "failed")


class TestShipToGeocoderExactMatch(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".geojson", delete=False)
        geojson = _make_geojson([
            _make_feature("123 Main Street", "Lima", "OH", "45801", 40.742800, -84.105200),
            _make_feature("456 Elm Avenue", "Findlay", "OH", "45840", 41.044400, -83.649900),
        ])
        self.tmp.write(geojson)
        self.tmp.close()
        self.geocoder = ShipToGeocoder(_default_geocode_settings(geojson_path=self.tmp.name))

    def tearDown(self):
        os.unlink(self.tmp.name)

    def test_exact_match(self):
        row = {"address_1": "123 Main Street", "city": "Lima", "state": "OH", "zip": "45801"}
        lat, lon, source = self.geocoder.geocode(row)
        self.assertAlmostEqual(lat, 40.742800, places=4)
        self.assertAlmostEqual(lon, -84.105200, places=4)
        self.assertEqual(source, "local_geojson_exact")

    def test_second_record(self):
        row = {"address_1": "456 Elm Avenue", "city": "Findlay", "state": "OH", "zip": "45840"}
        lat, lon, source = self.geocoder.geocode(row)
        self.assertAlmostEqual(lat, 41.044400, places=4)
        self.assertEqual(source, "local_geojson_exact")

    def test_no_match_returns_failed(self):
        row = {"address_1": "999 Nowhere Blvd", "city": "Columbus", "state": "OH", "zip": "43001"}
        lat, lon, source = self.geocoder.geocode(row)
        self.assertIsNone(lat)
        self.assertEqual(source, "failed")


class TestShipToGeocoderFuzzyZip(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".geojson", delete=False)
        geojson = _make_geojson([
            _make_feature("100 Oak Street", "Lima", "OH", "45801", 40.7400, -84.1050),
        ])
        self.tmp.write(geojson)
        self.tmp.close()
        self.geocoder = ShipToGeocoder(_default_geocode_settings(geojson_path=self.tmp.name))

    def tearDown(self):
        os.unlink(self.tmp.name)

    def test_fuzzy_zip_match(self):
        # Same zip, slightly different house number but same street name — should fuzzy match on street
        row = {"address_1": "102 Oak Street", "city": "Lima", "state": "OH", "zip": "45801"}
        lat, lon, source = self.geocoder.geocode(row)
        # Should find via fuzzy zip since exact key won't match
        self.assertIsNotNone(lat)
        self.assertIn("fuzzy", source)


class TestShipToGeocoderFuzzyCity(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".geojson", delete=False)
        # Use a distinct zip so zip-based lookup won't trigger
        geojson = _make_geojson([
            _make_feature("200 Pine Road", "Defiance", "OH", "43512", 41.2850, -84.3600),
        ])
        self.tmp.write(geojson)
        self.tmp.close()
        self.geocoder = ShipToGeocoder(_default_geocode_settings(geojson_path=self.tmp.name))

    def tearDown(self):
        os.unlink(self.tmp.name)

    def test_fuzzy_city_match(self):
        # Different zip but same city/state and close street name
        row = {"address_1": "202 Pine Road", "city": "Defiance", "state": "OH", "zip": "43500"}
        lat, lon, source = self.geocoder.geocode(row)
        self.assertIsNotNone(lat)
        self.assertIn("fuzzy", source)


class TestShipToGeocoderNominatimDisabled(unittest.TestCase):

    def test_nominatim_disabled_returns_failed(self):
        geocoder = ShipToGeocoder(_default_geocode_settings(enabled=True, fallback_nominatim=False))
        lat, lon, source = geocoder.geocode({"address_1": "100 Main St", "city": "Lima", "state": "OH", "zip": "45801"})
        self.assertIsNone(lat)
        self.assertEqual(source, "failed")


class TestShipToGeocoderNominatimFallback(unittest.TestCase):

    def _geocoder(self):
        return ShipToGeocoder(_default_geocode_settings(
            enabled=True,
            fallback_nominatim=True,
            nominatim_min_interval_seconds=0.0,
        ))

    def test_nominatim_success(self):
        geocoder = self._geocoder()
        mock_response = MagicMock()
        mock_response.read.return_value = json.dumps([{"lat": "40.742800", "lon": "-84.105200"}]).encode()

        with patch("beisser_sync.urlopen", return_value=mock_response):
            lat, lon, source = geocoder.geocode(
                {"address_1": "123 Main St", "city": "Lima", "state": "OH", "zip": "45801"}
            )
        self.assertAlmostEqual(lat, 40.742800, places=4)
        self.assertAlmostEqual(lon, -84.105200, places=4)
        self.assertEqual(source, "nominatim")

    def test_nominatim_no_results(self):
        geocoder = self._geocoder()
        mock_response = MagicMock()
        mock_response.read.return_value = json.dumps([]).encode()

        with patch("beisser_sync.urlopen", return_value=mock_response):
            lat, lon, source = geocoder.geocode(
                {"address_1": "123 Main St", "city": "Lima", "state": "OH", "zip": "45801"}
            )
        self.assertIsNone(lat)
        self.assertEqual(source, "nominatim_no_result")

    def test_nominatim_network_error(self):
        geocoder = self._geocoder()
        with patch("beisser_sync.urlopen", side_effect=OSError("timeout")):
            lat, lon, source = geocoder.geocode(
                {"address_1": "123 Main St", "city": "Lima", "state": "OH", "zip": "45801"}
            )
        self.assertIsNone(lat)
        self.assertEqual(source, "failed")

    def test_empty_address_skips_nominatim(self):
        geocoder = self._geocoder()
        lat, lon, source = geocoder.geocode({})
        self.assertIsNone(lat)
        self.assertEqual(source, "failed")


class TestShipToGeocoderGzipGeojson(unittest.TestCase):

    def test_loads_gzip_geojson(self):
        import gzip as gz_mod
        with tempfile.NamedTemporaryFile(suffix=".geojson.gz", delete=False) as tmp:
            tmp_name = tmp.name

        try:
            geojson = _make_geojson([
                _make_feature("10 Gzip Lane", "Tiffin", "OH", "44883", 41.1140, -83.1780),
            ])
            with gz_mod.open(tmp_name, "wt", encoding="utf-8") as f:
                f.write(geojson)

            geocoder = ShipToGeocoder(_default_geocode_settings(geojson_path=tmp_name))
            row = {"address_1": "10 Gzip Lane", "city": "Tiffin", "state": "OH", "zip": "44883"}
            lat, lon, source = geocoder.geocode(row)
            self.assertIsNotNone(lat)
            self.assertEqual(source, "local_geojson_exact")
        finally:
            os.unlink(tmp_name)


class TestShipToGeocoderNDGeoJson(unittest.TestCase):
    """Newline-delimited GeoJSON (one feature per line)."""

    def test_ndjson_features(self):
        feature = _make_feature("77 River Rd", "Sandusky", "OH", "44870", 41.4500, -82.7070)
        ndjson = json.dumps(feature)

        with tempfile.NamedTemporaryFile(mode="w", suffix=".geojson", delete=False) as tmp:
            tmp.write(ndjson)
            tmp_name = tmp.name

        try:
            geocoder = ShipToGeocoder(_default_geocode_settings(geojson_path=tmp_name))
            row = {"address_1": "77 River Rd", "city": "Sandusky", "state": "OH", "zip": "44870"}
            lat, lon, source = geocoder.geocode(row)
            self.assertIsNotNone(lat)
            self.assertEqual(source, "local_geojson_exact")
        finally:
            os.unlink(tmp_name)

    def test_ndjson_with_embedded_newlines(self):
        """Features containing embedded newlines in property values should still parse."""
        feature1 = {
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [-84.1, 40.7]},
            "properties": {
                "address_1": "100 Main St",
                "city": "Lima",
                "state": "OH",
                "zip": "45801",
                "notes": "Line one\nLine two",
            },
        }
        feature2 = _make_feature("200 Elm Ave", "Findlay", "OH", "45840", 41.04, -83.65)

        with tempfile.NamedTemporaryFile(mode="w", suffix=".geojson", delete=False) as tmp:
            # Write with indent to force multi-line JSON (embedded newlines)
            tmp.write(json.dumps(feature1, indent=2) + "\n")
            tmp.write(json.dumps(feature2) + "\n")
            tmp_name = tmp.name

        try:
            geocoder = ShipToGeocoder(_default_geocode_settings(geojson_path=tmp_name))
            row1 = {"address_1": "100 Main St", "city": "Lima", "state": "OH", "zip": "45801"}
            lat, lon, source = geocoder.geocode(row1)
            self.assertIsNotNone(lat)
            self.assertAlmostEqual(lat, 40.7, places=1)

            row2 = {"address_1": "200 Elm Ave", "city": "Findlay", "state": "OH", "zip": "45840"}
            lat2, lon2, source2 = geocoder.geocode(row2)
            self.assertIsNotNone(lat2)
        finally:
            os.unlink(tmp_name)


if __name__ == "__main__":
    unittest.main(verbosity=2)
