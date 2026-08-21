import math


def final_rotation(rotation: float, winner_index: int, count: int) -> float:
    """Mirror the browser calculation used by the wheel."""
    target = winner_index * (360 / count) + (360 / count) / 2
    current_angle = rotation % 360
    finish_delta = (270 - target - current_angle + 360) % 360
    return rotation + 6 * 360 + finish_delta


def test_selected_slice_ends_under_the_pointer_on_first_spin():
    for count in (2, 3, 7, 16, 42):
        for winner_index in range(count):
            final = final_rotation(0, winner_index, count)
            center = winner_index * (360 / count) + (360 / count) / 2 + final
            assert math.isclose(center % 360, 270, abs_tol=1e-9)


def test_selected_slice_ends_under_the_pointer_on_later_spins():
    rotation = 0
    for count, winner_index in ((5, 1), (5, 4), (9, 0), (9, 6)):
        rotation = final_rotation(rotation, winner_index, count)
        center = winner_index * (360 / count) + (360 / count) / 2 + rotation
        assert math.isclose(center % 360, 270, abs_tol=1e-9)
