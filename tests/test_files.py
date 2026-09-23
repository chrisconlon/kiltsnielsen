"""File discovery and reading on a small synthetic Kilts retail layout (no real data needed)."""

from pathlib import Path

import pytest

from kiltsreader import RetailReader

PRODUCT_COLS = ["upc", "upc_ver_uc", "upc_descr", "product_module_code", "product_module_descr", "product_group_code",
                "product_group_descr", "department_code", "department_descr", "brand_code_uc", "brand_descr", "multi",
                "size1_code_uc", "size1_amount", "size1_units", "dataset_found_uc", "size1_change_flag_uc"]
STORE_COLS = ["store_code_uc", "year", "parent_code", "retailer_code", "channel_code", "store_zip3", "fips_state_code",
              "fips_state_descr", "fips_county_code", "fips_county_descr", "dma_code", "dma_descr"]


def write_tsv(path: Path, columns, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(["\t".join(columns)] + ["\t".join(str(v) for v in r) for r in rows]) + "\n")


def make_retail(root: Path, year=2019, n_products=2):
    """A one-year, one-module retail distribution: 2 stores, n_products UPCs, 2 weeks of sales."""
    products = [[1000 + i, 1, f"CEREAL {i}", 1344, "CEREAL", 1005, "CEREAL", 1, "DRY GROCERY", 500000 + i, f"B{i}", 1,
                 30001, 12.0, "OZ", "ALL", 0] for i in range(n_products)]
    write_tsv(root / "Master_Files" / "Latest" / "products.tsv", PRODUCT_COLS, products)
    write_tsv(root / str(year) / "Annual_Files" / f"stores_{year}.tsv", STORE_COLS,
              [[s, year, 10, 20, "F", 606, 17, "IL", 31, "COOK", 602, "CHICAGO"] for s in (1, 2)])
    write_tsv(root / str(year) / "Annual_Files" / f"rms_versions_{year}.tsv", ["upc", "upc_ver_uc", "panel_year"],
              [[1000 + i, 1, year] for i in range(n_products)])
    write_tsv(root / str(year) / "Movement_Files" / f"1005_{year}" / f"1344_{year}.tsv",
              ["store_code_uc", "upc", "week_end", "units", "prmult", "price", "feature", "display"],
              [[s, 1000 + i, w, 3, 1, 2.5, 0, 0] for s in (1, 2) for i in range(n_products) for w in (20190105, 20190112)])


def read(root: Path, products=True) -> RetailReader:
    rr = RetailReader(root, verbose=False)
    rr.read_stores()
    rr.filter_stores(keep_channels=["F"])
    if products:
        rr.read_products(keep_modules=[1344])
    rr.read_sales()
    return rr


def test_deeper_duplicate_is_ignored_with_a_warning(tmp_path):
    make_retail(tmp_path, n_products=2)
    make_retail(tmp_path / "nielsen_extracts" / "RMS", n_products=3)  # an older, different copy one level down
    with pytest.warns(UserWarning, match="ignoring duplicate"):
        rr = read(tmp_path)
    assert rr.files_product == [tmp_path / "Master_Files" / "Latest" / "products.tsv"]
    assert rr.dict_sales[2019] == [tmp_path / "2019" / "Movement_Files" / "1005_2019" / "1344_2019.tsv"]
    assert rr.df_sales.num_rows == 2 * 2 * 2  # stores x products x weeks, not doubled
    assert rr.df_products.num_rows == 2


def test_equally_deep_duplicates_raise(tmp_path):
    make_retail(tmp_path / "copy_a")
    make_retail(tmp_path / "copy_b")
    with pytest.raises(ValueError, match="equally deep copies"):
        RetailReader(tmp_path, verbose=False)


def test_read_sales_without_read_products(tmp_path):
    make_retail(tmp_path)
    rr = read(tmp_path, products=False)
    assert rr.df_sales.num_rows == 8
