"""File discovery and reading on a small synthetic Kilts retail layout (no real data needed)."""

from pathlib import Path

import pyarrow as pa
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


def make_archive(tmp_path: Path) -> Path:
    """The synthetic distribution packed as a Kilts-style .tgz, alone in its folder."""
    import tarfile
    staging, folder = tmp_path / "staging", tmp_path / "archives"
    make_retail(staging / "nielsen_extracts" / "RMS")
    folder.mkdir()
    with tarfile.open(folder / "cereal_2019.tgz", "w:gz") as tar:
        tar.add(staging / "nielsen_extracts", arcname="nielsen_extracts")
    return folder


def test_archive_read_with_and_without_extraction_matches_the_folder(tmp_path):
    folder = make_archive(tmp_path)
    plain = read(tmp_path / "staging").df_sales
    assert read(folder).df_sales.equals(plain)  # streamed from the archive
    extracted = tmp_path / "extracted"
    rr = RetailReader(folder, verbose=False, extract_dir=extracted)
    rr.read_stores(); rr.filter_stores(keep_channels=["F"]); rr.read_products(keep_modules=[1344]); rr.read_sales()
    assert rr.df_sales.equals(plain)
    assert (extracted / "cereal_2019" / "nielsen_extracts" / "RMS" / "2019" / "Movement_Files" / "1005_2019" / "1344_2019.tsv").exists()


def test_extraction_is_reused_and_redone_when_the_archive_changes(tmp_path):
    import os
    folder, extracted = make_archive(tmp_path), tmp_path / "extracted"
    sales = extracted / "cereal_2019" / "nielsen_extracts" / "RMS" / "2019" / "Movement_Files" / "1005_2019" / "1344_2019.tsv"
    RetailReader(folder, verbose=False, extract_dir=extracted)
    os.utime(sales, ns=(1, 1))  # mark the extracted copy
    RetailReader(folder, verbose=False, extract_dir=extracted)
    assert sales.stat().st_mtime_ns == 1  # reused, not extracted again
    archive = folder / "cereal_2019.tgz"
    os.utime(archive, ns=(archive.stat().st_atime_ns, archive.stat().st_mtime_ns + 10**9))
    RetailReader(folder, verbose=False, extract_dir=extracted)
    assert sales.stat().st_mtime_ns != 1  # the archive changed: extracted again


def test_tables_not_read_are_empty_arrow_tables(tmp_path):
    make_retail(tmp_path)
    rr = RetailReader(tmp_path, verbose=False)
    for name in ["df_products", "df_sales", "df_stores", "df_rms", "df_extra"]:
        table = getattr(rr, name)
        assert isinstance(table, pa.Table) and table.num_rows == 0, name
