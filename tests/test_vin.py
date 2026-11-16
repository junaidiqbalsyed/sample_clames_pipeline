"""VIN helpers: generated VINs must carry a valid ISO 3779 check digit (position 9)."""
import random

import generate_synthetic as gen
import producer


def test_known_vin_check_digit():
    # Public ISO 3779 example VIN: 1M8GDM9AXKP042788 -> check digit X
    assert gen.vin_check_digit("1M8GDM9AXKP042788") == "X"


def test_simulator_vins_are_valid():
    rng = random.Random(7)
    for _ in range(200):
        vin = producer.make_vin(rng)
        assert len(vin) == 17
        assert gen.vin_check_digit(vin) == vin[8]


def test_synthetic_vins_are_valid_and_deterministic():
    vin_a = gen.make_vin(random.Random(1), "1HG", 2018, "ABC")
    vin_b = gen.make_vin(random.Random(1), "1HG", 2018, "ABC")
    assert vin_a == vin_b
    assert gen.vin_check_digit(vin_a) == vin_a[8]
