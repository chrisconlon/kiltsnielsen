"""Errata and open-issue corrections: key-based updates on a small synthetic panel layout."""

import warnings
from pathlib import Path

import pyarrow as pa
import pytest

from kiltsreader import PanelReader
from kiltsreader.module import _update_by_key


def write_tsv(path: Path, columns, rows, sep="\t"):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join([sep.join(columns)] + [sep.join(str(v) for v in r) for r in rows]) + "\n")


def test_update_by_key_matches_keys_not_row_position():
    table = pa.table({"k": pa.array([1, 2, 3], pa.uint32()), "v": pa.array([10, 20, 30], pa.int16()),
                      "s": pa.array(["a", "b", "c"]).dictionary_encode()})
    revision = pa.table({"k": [3, 9], "v": [33, None], "s": ["z", "y"], "extra": [0, 0]})
    out = _update_by_key(table, revision, ["k"], "test")
    assert out["k"].to_pylist() == [1, 2, 3]            # row order kept
    assert out["v"].to_pylist() == [10, 20, 33]         # only the matching key changes
    assert out["s"].to_pylist() == ["a", "b", "z"]
    assert out.schema == table.schema                   # types kept, no new columns
    null_rev = pa.table({"k": [2], "v": pa.array([None], pa.int64())})
    assert _update_by_key(table, null_rev, ["k"], "test")["v"].to_pylist() == [10, 20, 30]  # nulls do not overwrite


def test_update_by_key_rejects_duplicate_keys_and_out_of_range_values():
    table = pa.table({"k": [1, 2], "v": pa.array([1, 2], pa.uint8())})
    with pytest.raises(ValueError, match="more than one row"):
        _update_by_key(table, pa.table({"k": [1, 1], "v": [5, 6]}), ["k"], "test")
    with pytest.raises(pa.ArrowInvalid):
        _update_by_key(table, pa.table({"k": [1], "v": [300]}), ["k"], "test")


def make_panel(root: Path, flavor=True, births=None):
    hms = root / "HMS"
    write_tsv(hms / "Master_Files" / "Latest" / "retailers.tsv", ["retailer_code", "channel_type"], [[1, "Grocery"]])
    write_tsv(hms / "Master_Files" / "Latest" / "products.tsv",
              ["upc", "upc_ver_uc", "upc_descr", "product_module_code", "product_module_descr", "product_group_code",
               "product_group_descr", "department_code", "department_descr", "brand_code_uc", "brand_descr", "multi",
               "size1_code_uc", "size1_amount", "size1_units", "dataset_found_uc", "size1_change_flag_uc"],
              [[u, 1, "X", 1344, "CEREAL", 1005, "CEREAL", 1, "DG", 500000, "B", 1, 30001, 12.0, "OZ", "ALL", 0]
               for u in (1001, 1002, 1003)])
    annual = hms / "2010" / "Annual_Files"
    write_tsv(annual / "panelists_2010.tsv",
              ["Household_Cd", "Panel_Year", "Projection_Factor", "Household_Income", "DMA_Cd", "Male_Head_Birth", "Female_Head_Birth"],
              [[1, 2010, 100, 10, 602, 1950, 1952], [2, 2010, 100, 11, 602, "", 1960], [3, 2010, 100, 12, 602, 1970, ""]])
    write_tsv(annual / "trips_2010.tsv",
              ["trip_code_uc", "household_code", "purchase_date", "retailer_code", "store_code_uc", "panel_year", "store_zip3", "total_spent"],
              [[t, (t % 3) + 1, "2010-01-05", 1, 5, 2010, 606, 9.5] for t in range(1, 7)])
    write_tsv(annual / "purchases_2010.tsv",
              ["trip_code_uc", "upc", "upc_ver_uc", "quantity", "total_price_paid", "coupon_value", "deal_flag_uc"],
              [[t, 1001, 1, 1, 3.0, 0.0, 0] for t in range(1, 7)])
    write_tsv(annual / "products_extra_2010.tsv", ["upc", "upc_ver_uc", "panel_year", "flavor_code", "flavor_descr"],
              [[1001, 1, 2010, "", ""], [1002, 1, 2010, "", ""], [1002, 2, 2010, "", ""], [1003, 1, 2010, "", ""]])
    # errata: the first revised row is the table's second household
    write_tsv(root / "Revised_Panelist_Files" / "2010" / "Files" / "panelists_2010.tsv",
              ["Household_Cd", "Panel_Year", "Household_Income"], [[2, 2010, 99]])
    issues = root / "OpenIssues_SupplementFiles"
    if flavor:  # comma-separated, quoted; 1002 written as a float just below the integer; 1003 has two flavors
        write_tsv(issues / "ExtraAttributes_FlavorCode" / "Latest_Flavor_2010.csv",
                  ['"upc"', '"module"', '"flavor_code"', '"flavor_descr"', '"num_flavor"'],
                  [[1001, 1344, 7, '"PLAIN"', 1], ["1001.9999", 1344, 8, '"HONEY"', 1],
                   [1003, 1344, 9, '"FRUIT"', 2], [1003, 1344, 10, '"NUT"', 2]], sep=",")
    if births:
        write_tsv(issues / "Panelist_maleHeadBirth_femaleHeadBirth" / "panel_10_birth_dates_corrected.tsv",
                  ["household_id", "panel_year", "male_head_birth", "female_head_birth"], births)


def read_panel(root: Path) -> PanelReader:
    pr = PanelReader(root, verbose=False)
    pr.read_retailers()
    pr.read_products()
    pr.read_annual()
    pr.read_extra()
    return pr


def test_revised_panelists_update_the_right_household(tmp_path):
    make_panel(tmp_path)
    pr = read_panel(tmp_path)
    before = pr.df_panelists
    pr.read_revised_panelists()
    rows = dict(zip(pr.df_panelists["household_code"].to_pylist(), pr.df_panelists["Household_Income"].to_pylist()))
    assert rows == {1: 10, 2: 99, 3: 12}
    assert pr.df_panelists.schema == before.schema


def test_flavor_codes_filled_by_upc_with_rounding_and_ambiguous_left_missing(tmp_path):
    make_panel(tmp_path)
    pr = read_panel(tmp_path)
    with pytest.warns(UserWarning, match="1 UPCs have more than one flavor"):
        pr.process_open_issues()
    got = {(u, v): (c, d) for u, v, c, d in zip(*(pr.df_extra[k].to_pylist() for k in
                                                   ["upc", "upc_ver_uc", "flavor_code", "flavor_descr"]))}
    assert got == {(1001, 1): (7, "PLAIN"), (1002, 1): (8, "HONEY"), (1002, 2): (8, "HONEY"), (1003, 1): (None, "")}  # "" as read from the file


def test_birth_years_checked_not_changed(tmp_path):
    agree = [[1, 2010, "1950-03", "1952-11"], [2, 2010, "-", "1960-01"], [3, 2010, "1970-07", "-"]]
    make_panel(tmp_path, flavor=False, births=agree)
    pr = read_panel(tmp_path)
    before = pr.df_panelists
    with warnings.catch_warnings():
        warnings.simplefilter("error", UserWarning)
        pr.process_open_issues()
    assert pr.df_panelists.equals(before)

    disagree = tmp_path / "other"
    make_panel(disagree, flavor=False, births=[[1, 2010, "1951-03", "1952-11"]] + agree[1:])
    pr = read_panel(disagree)
    with pytest.warns(UserWarning, match="Male_Head_Birth year differs from the panelist file for 1 households"):
        pr.process_open_issues()
