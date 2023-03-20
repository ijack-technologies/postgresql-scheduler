import pickle
from pathlib import Path

fixture_folder = Path(__file__).parent


def save_fixture(fixture_obj, name_stem: str):
    """Save a fixture for testing as a pickle file"""

    with open(str(fixture_folder.joinpath(f"{name_stem}.pkl")), "wb") as file_:
        pickle.dump(fixture_obj, file_)

    return None