"""Pure helper functions of the simulator and the data generator."""
from datetime import date

import generate_synthetic as gen
import producer


def test_incident_location_is_deterministic_and_in_nyc():
    lat1, lon1 = producer.incident_location("claim-123")
    lat2, lon2 = producer.incident_location("claim-123")
    assert (lat1, lon1) == (lat2, lon2)
    assert 40.4 < lat1 < 41.0 and -74.4 < lon1 < -73.6


def test_incident_location_differs_per_claim():
    assert producer.incident_location("a") != producer.incident_location("b")


def test_add_years_handles_leap_day():
    assert gen.add_years(date(2000, 2, 29), 18) == date(2018, 2, 28)
    assert gen.add_years(date(2001, 5, 17), 1) == date(2002, 5, 17)
