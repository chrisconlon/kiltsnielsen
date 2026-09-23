#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Sep 16 08:40:09 2021

@author: Chitra Marti
Based on code from https://github.com/chrisconlon/kiltsnielsen


Goal: Read in raw Nielsen Retail Scanner files, downloadable from the
Kilts File Selection System
https://kiltsfiles.chicagobooth.edu/Requests/Create-New-Request.aspx

and raw Nielsen Consumer Panel files.

See https://www.chicagobooth.edu/research/kilts/datasets/nielsenIQ-nielsen
for details on access 

NielsenReader.py is an auxiliary file that defines the following classes
(1) RetailReader
(2) PanelReader

along with several other functions that are used only within the file.

See Example.py for implementation of the RetailReader and PanelReader functions
"""


# %% Initial Methods and Packages
import time
import tarfile
import warnings
import numpy as np
import pyarrow as pa
import pyarrow.dataset as pads
import pyarrow.parquet as pq
import pyarrow.compute as pc
from pyarrow import csv

import pathlib as path

_start_time = time.time()
def tick():
    """
    Start Timer
    """
    global _start_time 
    _start_time = time.time()
def tock():
    """
    Stop Timer and Print Time Passed
    """
    t_sec = round(time.time() - _start_time)
    (t_min, t_sec) = divmod(t_sec,60)
    (t_hour,t_min) = divmod(t_min,60) 
    print('Time passed: {}hour:{}min:{}sec'.format(t_hour,t_min,t_sec))


# note: u is for "unsigned"
# so it technically has twice as much space!
dict_types = {'upc': pa.uint64(),
              'upc_ver_uc': pa.uint8(),
              'product_module_code': pa.uint16(),
              'brand_code_uc': pa.uint32(),
              'multi': pa.uint16(),
              'size1_code_uc': pa.uint16(),
              'year':pa.uint16(),
              'panel_year': pa.uint16(),
              'dma_code': pa.uint16(),
              'retailer_code': pa.uint16(),
              'parent_code': pa.uint16(),
              'store_zip3': pa.uint16(),
              'fips_county_code': pa.uint16(),
              'fips_state_code': pa.uint8(),
              'store_code_uc': pa.uint32(),
              'week_end':pa.uint32(),
              'units': pa.uint64(),
              'prmult': pa.uint8(),
              'feature': pa.int8(),
              'display': pa.int8(),
              'price':pa.float64(),
              'flavor_code': pa.uint64(),
              'flavor_descr': pa.string(),
              'form_code': pa.uint64(),
              'form_descr': pa.string(),
              'formula_code': pa.uint64(),
              'formula_descr': pa.string(),
              'container_code': pa.uint64(),
              'container_descr': pa.string(),
              'salt_content_code': pa.uint64(),
              'salt_content_descr': pa.string(),
              'style_code': pa.uint64(),
              'style_descr': pa.string(),
              'type_code': pa.uint64(),
              'type_descr': pa.string(),
              'product_code': pa.uint64(),
              'product_descr': pa.string(),
              'variety_code': pa.uint64(),
              'variety_descr': pa.string(),
              'organic_claim_code': pa.uint64(),
              'organic_claim_descr': pa.string(),
              'usda_organic_seal_code': pa.uint64(),
              'usda_organic_seal_descr': pa.string(),
              'common_consumer_name_code': pa.uint64(),
              'common_consumer_name_descr': pa.string(),
              'strength_code': pa.uint64(),
              'strength_descr': pa.string(),
              'scent_code': pa.uint64(),
              'scent_descr': pa.string(),
              'dosage_code': pa.uint64(),
              'dosage_descr': pa.string(),
              'gender_code': pa.uint64(),
              'gender_descr': pa.string(),
              'target_skin_condition_code': pa.uint64(),
              'target_skin_condition_descr': pa.string(),
              'use_code': pa.uint64(),
              'use_descr': pa.string(),
              'size2_code': pa.uint64(),
              'size2_amount': pa.float64(),
              'size2_units': pa.string(),
              'deal_flag_uc': pa.uint8(),
              'quantity':pa.uint16(),
              'household_code':pa.uint32(),
              'Household_Cd':pa.uint32(),
              # Panelist columns that match store and sales keys, typed alike so the tables join
              # without casts (Panel_Year becomes panel_year after reading). Ranges in the Kilts
              # files 2004-2020: Panel_Year 2004-2020, DMA_Cd 500-881, Fips_State_Cd <= 56,
              # Fips_County_Cd <= 840. The CSV reader raises on a value outside the type.
              'Panel_Year': pa.uint16(),
              'DMA_Cd': pa.uint16(),
              'Fips_State_Cd': pa.uint8(),
              'Fips_County_Cd': pa.uint16(),
              # Product hierarchy codes (nullable; otherwise inferred): group <= 9599, department <= 99
              'product_group_code': pa.uint16(),
              'department_code': pa.uint16(),
              # Trip ids (<= 1.1e9) in trips and purchases; int64 is what inference already gave
              'trip_code_uc': pa.int64(),
              'Fips_County_Desc':pa.string(),
              'Fips_State_Desc':pa.string(),
              'Scantrack_Market_Identifier_Desc':pa.string(),
              'DMA_Name':pa.string(),
              }


# Column renames applied to panelist data
# Extend this mapping if NielsenIQ changes column naming conventions
COLUMN_RENAME_MAP = {'Household_Cd': 'household_code',
                     'Panel_Year': 'panel_year'}
# Keep backward compat alias
dict_column_map = COLUMN_RENAME_MAP

# Expected column sets for format validation
# If NielsenIQ changes column names, update these sets and dict_types above
EXPECTED_PRODUCT_COLS = {
    'upc', 'upc_ver_uc', 'upc_descr', 'product_module_code',
    'product_module_descr', 'product_group_code', 'product_group_descr',
    'department_code', 'department_descr', 'brand_code_uc', 'brand_descr',
    'multi', 'size1_code_uc', 'size1_amount', 'size1_units',
    'dataset_found_uc', 'size1_change_flag_uc'
}

EXPECTED_SALES_COLS = {
    'store_code_uc', 'upc', 'week_end', 'units', 'prmult', 'price',
    'feature', 'display'
}

EXPECTED_STORE_COLS = {
    'store_code_uc', 'year', 'parent_code', 'retailer_code',
    'channel_code', 'store_zip3', 'fips_state_code', 'fips_state_descr',
    'fips_county_code', 'fips_county_descr', 'dma_code', 'dma_descr'
}

EXPECTED_RMS_COLS = {'upc', 'upc_ver_uc', 'panel_year'}

# Note: panelist columns use raw names BEFORE COLUMN_RENAME_MAP is applied
EXPECTED_PANELIST_COLS = {
    'Household_Cd', 'Panel_Year', 'Projection_Factor', 'Projection_Factor_Magnet',
    'Household_Income', 'Household_Size', 'Type_Of_Residence',
    'Household_Composition', 'Age_And_Presence_Of_Children',
    'Male_Head_Age', 'Female_Head_Age',
    'Male_Head_Employment', 'Female_Head_Employment',
    'Male_Head_Education', 'Female_Head_Education',
    'Male_Head_Occupation', 'Female_Head_Occupation',
    'Male_Head_Birth', 'Female_Head_Birth',
    'Marital_Status', 'Race', 'Hispanic_Origin',
    'Panelist_ZipCd', 'Fips_State_Cd', 'Fips_State_Desc',
    'Fips_County_Cd', 'Fips_County_Desc', 'Region_Cd',
    'Scantrack_Market_Identifier_Cd', 'Scantrack_Market_Identifier_Desc',
    'DMA_Cd', 'DMA_Name',
    'Kitchen_Appliances', 'TV_Items', 'Household_Internet_Connection',
    'Wic_Indicator_Current', 'Wic_Indicator_Ever_Not_Current',
    'Member_1_Birth', 'Member_1_Relationship_Sex', 'Member_1_Employment',
    'Member_2_Birth', 'Member_2_Relationship_Sex', 'Member_2_Employment',
    'Member_3_Birth', 'Member_3_Relationship_Sex', 'Member_3_Employment',
    'Member_4_Birth', 'Member_4_Relationship_Sex', 'Member_4_Employment',
    'Member_5_Birth', 'Member_5_Relationship_Sex', 'Member_5_Employment',
    'Member_6_Birth', 'Member_6_Relationship_Sex', 'Member_6_Employment',
    'Member_7_Birth', 'Member_7_Relationship_Sex', 'Member_7_Employment',
}

# Not in the trips files before 2013
OPTIONAL_TRIP_COLS = frozenset({'method_of_payment_cd'})

EXPECTED_TRIP_COLS = {
    'trip_code_uc', 'household_code', 'purchase_date',
    'retailer_code', 'store_code_uc', 'panel_year',
    'store_zip3', 'total_spent', 'method_of_payment_cd'
}

# Note: panel_year is appended by the code, not present in raw purchase files
EXPECTED_PURCHASE_COLS = {
    'trip_code_uc', 'upc', 'upc_ver_uc', 'quantity',
    'total_price_paid', 'coupon_value', 'deal_flag_uc'
}


def _validate_columns(actual_columns, expected_columns, file_description="file", optional=frozenset()):
    """Warn about missing or unexpected columns in a data file.
    Helps detect NielsenIQ format changes early. Columns in `optional` are expected but may be
    absent (e.g. ones NielsenIQ added in later years).
    """
    actual = set(actual_columns)
    expected = set(expected_columns)
    missing = expected - set(optional) - actual
    unexpected = actual - expected

    if missing:
        warnings.warn(
            f"Expected columns missing from {file_description}: {missing}. "
            "This may indicate a NielsenIQ format change.",
            UserWarning, stacklevel=3)
    if unexpected:
        warnings.warn(
            f"Unexpected columns in {file_description}: {unexpected}. "
            "This may indicate a NielsenIQ format change. "
            "These columns will still be read but type casting may not apply.",
            UserWarning, stacklevel=3)
    return missing, unexpected


def _safe_convert_options(actual_columns=None, **kwargs):
    """Create ConvertOptions with column_types restricted to columns that exist.
    Prevents errors when data files contain unexpected columns.
    """
    column_types = kwargs.pop('column_types', dict_types)
    if actual_columns is not None:
        column_types = {k: v for k, v in column_types.items()
                        if k in actual_columns}
    return csv.ConvertOptions(column_types=column_types, **kwargs)


class TgzFileManager:
    """Manages transparent reading of TSV/CSV files from .tgz archives.

    When Nielsen data is provided as .tgz files (as downloaded from Kilts),
    this class enumerates archive contents and provides file-like objects
    for reading without extracting to disk.

    With extract_dir, each archive is instead extracted once, in one sequential
    pass, to extract_dir/<archive name>/ (data files only), and files are read
    from there. A marker written after the extraction records the archive's size
    and modification time; a later reader reuses the folder when they match and
    extracts again otherwise (e.g. after an interrupted extraction). Reading from
    an archive without extracting decompresses it from the start for every file.
    """

    MARKER = '.kiltsreader_extracted'

    def __init__(self, dir_read, extract_dir=None):
        self.dir_read = dir_read
        self.extract_dir = None if extract_dir is None else path.Path(extract_dir)
        # Search for .tgz files in dir_read and one level deep
        self.tgz_files = sorted(set(
            list(dir_read.glob('*.tgz')) + list(dir_read.glob('*/*.tgz'))
        ))
        self._archive_map = {}  # maps virtual Path -> (tgz_path, member_name)

    @property
    def has_archives(self):
        return len(self.tgz_files) > 0

    def get_archive_files(self, data_type=None):
        """Enumerate TSV/CSV files inside all .tgz archives.
        Returns a list of virtual Path objects that can be used as keys.

        Args:
            data_type: 'RMS' to only scan scanner archives, 'HMS' to only scan
                       panel archives, or None to scan all.
        """
        virtual_files = []
        for tgz_path in self.tgz_files:
            # Skip archives that are clearly the wrong data type
            tgz_name = tgz_path.name.lower()
            if data_type == 'RMS' and ('panel' in tgz_name or 'consumer' in tgz_name):
                continue
            if data_type == 'HMS' and 'panel' not in tgz_name and 'consumer' not in tgz_name and 'master' not in tgz_name:
                continue
            # Skip reference/documentation archives
            if 'reference' in tgz_name or 'documentation' in tgz_name:
                continue
            if self.extract_dir is not None:
                names = self._extract(tgz_path)
            else:
                with tarfile.open(tgz_path, 'r:gz') as tar:
                    names = [m.name for m in tar.getmembers() if self._is_data_file(m)]
            for name in names:
                virtual_path = self.dir_read / name
                self._archive_map[virtual_path] = (tgz_path, name)
                virtual_files.append(virtual_path)
        return virtual_files

    @staticmethod
    def _is_data_file(member):
        """TSV/CSV files, skipping macOS resource forks."""
        name_lower = member.name.lower()
        base = path.Path(member.name).stem
        return (member.isfile() and (name_lower.endswith('.tsv') or name_lower.endswith('.csv'))
                and not base.startswith('._') and '/._' not in member.name)

    def _extract(self, tgz_path):
        """Extract the data files of one archive (once); return their names inside the archive."""
        target = self.extract_dir / tgz_path.name.removesuffix('.tgz')
        marker = target / self.MARKER
        stamp = f"{tgz_path.stat().st_size} {tgz_path.stat().st_mtime_ns}"
        if marker.exists():
            lines = marker.read_text().splitlines()
            if lines and lines[0] == stamp:
                return lines[1:]
            marker.unlink()
        target.mkdir(parents=True, exist_ok=True)
        names = []
        with tarfile.open(tgz_path, 'r|gz') as tar:  # one sequential pass
            for member in tar:
                if self._is_data_file(member):
                    tar.extract(member, path=target, filter='data')
                    names.append(member.name)
        marker.write_text("\n".join([stamp] + names) + "\n")
        return names

    def open_file(self, virtual_path):
        """Return a binary file-like object for a file inside an archive, or with
        extract_dir the path of its extracted copy. Returns None if the path is
        not an archive member.
        """
        if virtual_path not in self._archive_map:
            return None
        tgz_path, member_name = self._archive_map[virtual_path]
        if self.extract_dir is not None:
            return self.extract_dir / tgz_path.name.removesuffix('.tgz') / member_name
        tar = tarfile.open(tgz_path, 'r:gz')
        member = tar.getmember(member_name)
        extracted = tar.extractfile(member)
        # Keep a reference to the tar so it's not garbage collected
        extracted._tar_ref = tar
        return extracted


def _is_master_files(name):
    """Check if a directory name is a Master_Files variant (e.g. Master_Files, Master_Files_2006-2020)."""
    return name == 'Master_Files' or name.startswith('Master_Files_')


def _read_csv(self, filepath, **kwargs):
    """Read a CSV/TSV file, transparently handling .tgz archive members.
    Falls back to standard csv.read_csv for normal file paths.
    """
    if hasattr(self, '_tgz_manager') and self._tgz_manager is not None:
        file_obj = self._tgz_manager.open_file(filepath)
        if isinstance(file_obj, path.Path):  # extracted copy
            return csv.read_csv(file_obj, **kwargs)
        if file_obj is not None:
            try:
                return csv.read_csv(pa.PythonFile(file_obj), **kwargs)
            finally:
                file_obj.close()
    return csv.read_csv(filepath, **kwargs)


def _keep_shallowest(files, key, dir_read, description):
    """One file per key: when the same Kilts file appears more than once under dir_read (for
    example a second copy of the distribution in a subfolder), keep the copy with the shortest
    path below dir_read and warn about the others. Raises if two copies are equally shallow,
    because then there is no basis for choosing. Without this, which copy was read depended on
    the file system's listing order, and a duplicated sales file was read twice.
    """
    groups = {}
    for f in files:
        groups.setdefault(key(f), []).append(f)
    kept = []
    for k, copies in groups.items():
        copies = sorted(copies, key=lambda f: (len(f.relative_to(dir_read).parts), str(f)))
        if len(copies) > 1:
            if len(copies[0].relative_to(dir_read).parts) == len(copies[1].relative_to(dir_read).parts):
                raise ValueError(
                    f"{description}: {len(copies)} equally deep copies of {k} under {dir_read}: "
                    f"{[str(c) for c in copies]}. Point dir_read at one copy.")
            warnings.warn(
                f"{description}: using {copies[0]}; ignoring duplicate(s) {[str(c) for c in copies[1:]]}",
                UserWarning, stacklevel=3)
        kept.append(copies[0])
    return sorted(kept)


def _update_by_key(table, revision, keys, description):
    """Replace values in `table` with the non-null values of `revision` for the same keys.

    Key-based counterpart of pandas' DataFrame.update: rows are matched on `keys` (not on row
    position), only columns present in both tables change, rows of `revision` with no match in
    `table` are ignored, and the row order and column types of `table` are kept. Revision values
    are cast to the table's types with Arrow's checked cast, so an out-of-range value raises.
    """
    if revision.group_by(keys).aggregate([([], 'count_all')]).num_rows != revision.num_rows:
        raise ValueError(f"{description}: the revision has more than one row per {keys}")
    columns = [c for c in revision.column_names if c in table.column_names and c not in keys]
    rev = revision.select(keys + columns).rename_columns(keys + [f'__rev_{c}' for c in columns])
    rev = rev.cast(pa.schema([table.schema.field(k) for k in keys]
                             + [pa.field(f'__rev_{c}', _value_type(table.schema.field(c).type)) for c in columns]))
    left = table.select(keys).append_column('__row', pa.array(range(table.num_rows), pa.int64()))
    matched = left.join(rev, keys=keys, join_type='left outer', use_threads=False).sort_by('__row')
    for c in columns:
        original = table[c]
        value_type = _value_type(original.type)
        new = pc.if_else(pc.is_valid(matched[f'__rev_{c}']), matched[f'__rev_{c}'], original.cast(value_type))
        if pa.types.is_dictionary(original.type):
            new = new.dictionary_encode().cast(original.type)
        table = table.set_column(table.schema.get_field_index(c), c, new)
    return table


def _quarter_end(dates):
    """Last day of each date's quarter (pandas' QuarterEnd(0): a quarter-end date maps to itself)."""
    one_day = pa.scalar(86_400, pa.duration('s')).cast(pa.duration(dates.type.unit))
    start = pc.floor_temporal(dates, unit='quarter')
    return pc.subtract(pc.ceil_temporal(pc.add(start, one_day), unit='quarter'), one_day)


def _value_type(t):
    return t.value_type if pa.types.is_dictionary(t) else t


def _has_data_files(files):
    """Check if file list contains Nielsen data files (not just stray docs)."""
    data_dirs = {'Movement_Files', 'Annual_Files', 'Master_Files'}
    for f in files:
        parts = set(f.parts)
        if parts & data_dirs:
            return True
        # Also match Master_Files_YYYY-YYYY variants
        if any(_is_master_files(p) for p in f.parts):
            return True
    return False


def get_files(self):
    """Get all TSV/CSV files for a PanelReader or RetailReader object.
    Searches extracted directories first, then .tgz archives if none found.
    Falls back to .tgz if extracted files don't contain expected data directories.
    """
    files = [i for i in self.dir_read.glob('**/*.*sv') if '._' not in i.stem]

    # Determine data type for archive filtering
    data_type = 'RMS' if isinstance(self, RetailReader) else 'HMS'

    if len(files) == 0 or not _has_data_files(files):
        # Try .tgz archives
        self._tgz_manager = TgzFileManager(self.dir_read, getattr(self, 'extract_dir', None))
        if self._tgz_manager.has_archives:
            archive_files = self._tgz_manager.get_archive_files(data_type=data_type)
            if archive_files:
                files = files + archive_files
            else:
                self._tgz_manager = None
        else:
            self._tgz_manager = None
    else:
        self._tgz_manager = None

    if len(files) == 0:
        raise FileNotFoundError(
            f"Found no TSV/CSV files in {self.dir_read}. "
            "Check folder name and make sure folder is unzipped, "
            "or provide .tgz archive files.")
    return files



# given the Path of a sales file, find its year
# Differentiate Sales vs. Annual Files: different tree structure
# Panel files follow the Sales structure (bit confusing here)
def get_year(file, type = 'Sales'):
    """
    Arguments:
        file: filename
        type: 'Sales' or 'Ann' (see below)

    Get year a particular file corresponds to
    Will throw error if not an annual file
    if type = 'Sales': the last half of the filename is the year.
    Use type = 'Sales' if in Retail

    if type = 'Ann': the grandparent folder contains the year name
    Use type = 'Ann' if in Panel

    """
    if type == 'Ann': # these have one less root
        return int(file.parent.parent.name)
    return int(file.stem.split('_')[-1])


# Read in the Products File
# can limit to a subset of UPCs
# but unfortunately, we will always have to read all the products
## NOTE: INCORPORATE GROUP, MODULE PARTITIONING
def get_products(self, upc_list=None,
                 keep_groups = None, drop_groups = None,
                 keep_modules = None, drop_modules = None,
                 keep_departments = None, drop_departments = None):
    """
    Arguments:
        Required: RetailReader or PanelReader object
        Optional: keep_groups, drop_groups, keep_modules, drop_modules,
                  keep_departments, drop_departments, upc_list
        Each takes a list of group codes, module codes, department codes, or UPCs

    Select the Product file and read it in
    Common to both the Retail Reader and Panel Reader files
    """
    if self.files_product:
        self.file_products = self.files_product[0]
    else:
        raise FileNotFoundError(
            f"Could not find products.tsv under Master_Files/Latest in {self.dir_read}. "
            "Check folder name and make sure folder is unzipped.")

    read_opt = csv.ReadOptions(encoding='latin')
    parse_opt = csv.ParseOptions(delimiter = '\t')
    conv_opt = csv.ConvertOptions(column_types = dict_types)
    df_products = _read_csv(self, self.file_products,
                           read_options = read_opt,
                           parse_options = parse_opt,
                           convert_options = conv_opt)

    _validate_columns(df_products.column_names, EXPECTED_PRODUCT_COLS, "products.tsv")

    # Apply filters using Arrow compute
    my_filter = pc.greater(df_products['upc'], 0)  # base filter (always true)

    if keep_groups:
        my_filter = pc.and_(my_filter, pc.is_in(df_products['product_group_code'],
                            value_set=pa.array(keep_groups, pa.uint16())))
    if drop_groups:
        my_filter = pc.and_not(my_filter, pc.is_in(df_products['product_group_code'],
                               value_set=pa.array(drop_groups, pa.uint16())))
    if keep_modules:
        my_filter = pc.and_(my_filter, pc.is_in(df_products['product_module_code'],
                            value_set=pa.array(keep_modules, pa.uint16())))
    if drop_modules:
        my_filter = pc.and_not(my_filter, pc.is_in(df_products['product_module_code'],
                               value_set=pa.array(drop_modules, pa.uint16())))
    if keep_departments:
        my_filter = pc.and_(my_filter, pc.is_in(df_products['department_code'],
                            value_set=pa.array(keep_departments, pa.uint16())))
    if drop_departments:
        my_filter = pc.and_not(my_filter, pc.is_in(df_products['department_code'],
                               value_set=pa.array(drop_departments, pa.uint16())))
    if upc_list:
        my_filter = pc.and_(my_filter, pc.is_in(df_products['upc'],
                            value_set=pa.array(upc_list, pa.uint64())))

    df_products = df_products.filter(my_filter)

    # Sort by UPC
    df_products = df_products.sort_by('upc')
    self.df_products = df_products

    if self.verbose:
        print('Successfully Read in Products with', df_products.num_rows, 'rows')

    return



def get_extra(self, years = None, upc_list = None):

    """
    
    Function: populates self.df_extra
    
    Select the Extra [characteristics] file and read it in
    Common to both the Retail Reader and Panel Reader files
    Filter Options:
    upc_list: a list of integer UPCs to select, ignores versioning by Nielsen
    years (not recommended): selects extra characteristics that are associated with a year
    in the Nielsen data. Sometimes UPCs have repeat entries, but these tend
    to be due to missing data and reporting issues, not changes. Nielsen
    codes product changes as different product versions.
    
    Module and Group selections not possible for the extra files. 
    One option is to select modules and groups in the product data and then
    merge. 
    
    Columns: upc, upc_ver_uc, panel_year, flavor_code, flavor_descr, 
    form_code, form_descr, formula_code, formula_descr, container_code, 
    container_descr, salt_content_code, salt_content_descr, style_code, 
    style_descr, type_code, type_descr, product_code, product_descr, 
    variety_code, variety_descr, organic_claim_code, organic_claim_descr, 
    usda_organic_seal_code, usda_organic_seal_descr, 
    common_consumer_name_code, common_consumer_name_descr, 
    strength_code, strength_descr, scent_code, scent_descr, 
    dosage_code, dosage_descr, gender_code, gender_descr,
    target_skin_condition_code, target_skin_condition_descr, 
    use_code, use_descr, size2_code, size2_amount, size2_units
    
    
    See Nielsen documentation for a full description of these variables.
    """
    if years is None:
        years = self.all_years

    files_extra_in = [f for f in self.files_extra
                      if get_year(f) in years]

    def aux_read_extra_year(filename):
        conv_opt = csv.ConvertOptions(column_types = dict_types)
        parse_opt = csv.ParseOptions(delimiter='\t')
        return _read_csv(self, filename,
                         parse_options = parse_opt,
                         convert_options = conv_opt)

    df_extra = pa.concat_tables([aux_read_extra_year(f)
                                 for f in files_extra_in])

    if upc_list:
        df_extra = df_extra.filter(
            pc.is_in(df_extra['upc'], value_set=pa.array(upc_list, pa.uint64())))

    df_extra = df_extra.sort_by([('upc', 'ascending'), ('panel_year', 'ascending')])
    self.df_extra = df_extra

    if self.verbose:
        print('Successfully Read in Extra Files with', df_extra.num_rows, 'rows')

    return

def aux_write_direct(df, filename, compr = 'brotli'):
    """Write a non-empty Arrow table to parquet (tables not read are empty and skipped)."""
    if isinstance(df, pa.Table) and df.num_rows:
        pq.write_table(df, filename, compression = compr)
        print('Wrote as direct parquet to', filename)
    return

# %%

# Define class RetailReader
# will contain all methods we use to read in the Retail Scanner Data
class RetailReader(object):
    """
    Object class to read in Nielsen Retail Scanner Data
    Files created:
        df_extra: from annual product_extra files
        df_products: from Master product file
        df_rms: from Annual rms_versions files
        df_stores: from Annual stores files
        df_sales: from Movement files
    Can filter based on store locations (DMAs), product groups,
    product modules, and years
    """

    # initialize object
    # input: directory from which to read in the Scanner Data
    # if no input, assume current working directory
    def __init__(self, dir_read = path.Path.cwd(), verbose = True, extract_dir = None):
        """
        Function: initialize a RetailReader object
        identifies file names and locations for each dataset
        Will throw errors if any critical files are missing or incorrectly named
        """
        self.verbose = verbose

        self.dir_read = dir_read # save the folder to the class
        # .tgz archives are extracted once to this folder and read from there (optional)
        self.extract_dir = extract_dir

        # get all files in the relevant folder
        self.files = get_files(self)

        # then, get the product TSV file
        # we want the one under /RMS/Master_Files/Latest
        # we do NOT want the Revised Panelist Files (if in Panel)
        self.files_product = _keep_shallowest(
            [f for f in self.files if
             (f.name == 'products.tsv')&
             (f.parent.name == 'Latest')&
             _is_master_files(f.parent.parent.name)],
            lambda f: f.name, dir_read, "Master products")

        # Collect the Annual Files NOTE: currently unused
        self.files_annual = _keep_shallowest(
            [f for f in self.files if 'Annual_Files' in f.parts],
            lambda f: f.name, dir_read, "Annual files")


        # Collect the Movement Files, i.e. the store-weekly sales files
        # (one per group folder and module-year file name)
        self.files_sales = _keep_shallowest(
            [f for f in self.files if 'Movement_Files' in f.parts],
            lambda f: (f.parent.name, f.name), dir_read, "Movement files")
        if not self.files_sales:
            raise FileNotFoundError(
                f"Could not find Movement Files in {dir_read}. "
                "Check folder structure.")

        # Collect groups, modules, and years represented in the sales files
        # throws errors if the files were renamed from the original structure
        # NOTE these will be automatically sorted, it seems
        try:
            self.all_groups = set(self.get_group(f) for f in self.files_sales)
        except Exception as e:
            raise ValueError(
                f"Could not get Group Code from Movement Files in {dir_read}. "
                f"Use original Nielsen naming conventions. Error: {e}") from e

        try:
            self.all_modules = set(self.get_module(f) for f in self.files_sales)
        except Exception as e:
            raise ValueError(
                f"Could not get Module Code from Movement Files in {dir_read}. "
                f"Use original Nielsen naming conventions. Error: {e}") from e

        try:
            self.all_years = set(get_year(f) for f in self.files_sales)
        except Exception as e:
            raise ValueError(
                f"Could not get Year from Movement Files in {dir_read}. "
                f"Use original Nielsen naming conventions. Error: {e}") from e


        # Collect the Stores, RMS, and Extra Files (the Annual Files)
        # note that these do NOT vary by product group or module, only year
        self.files_stores = [ f for f in self.files_annual if 'stores' in f.name]
        self.files_rms = [ f for f in self.files_annual if 'rms_versions' in f.name]
        self.files_extra = [ f for f in self.files_annual if 'products_extra' in f.name]

        # Create Dictionaries mapping Years to their Files: Annuals
        # easy here because we only have one file for each year
        self.dict_stores = {get_year(f): f for f in self.files_stores}
        self.dict_rms = {get_year(f): f for f in self.files_rms}
        self.dict_extra = {get_year(f): f for f in self.files_extra}

        # Store the Sales Files in a similar Dictionary, by Year
        # there are (potentially) multiple sales files per year
        # because there may be many modules and groups
        self.dict_sales = {y: [f for f in self.files_sales
                               if get_year(f) == y]
                           for y in self.all_years}



        # Create empty DataFrames to store data as we process it

        self.df_products = pa.table({})
        self.df_sales = pa.table({})
        self.df_stores = pa.table({})
        self.df_rms = pa.table({})
        self.df_extra = pa.table({})

        return


    # given the Path of a sales file, find its module code
    def get_module(self, file_sales):
        """
        Given the Path of a sales file, find its module code
        """
        return int(file_sales.stem.split('_')[0])

    # given the Path of a sales file, find its group code
    def get_group(self, file_sales):
        """
        Given the Path of a sales file, find its group code
        """
        return int(file_sales.parent.stem.split('_')[0])


    # Begin a Proper Cleanup: filter years, groups, modules, etc.
    def filter_years(self, keep = None, drop = None):
        """
        Function: selects years of sales to include
        Arguments: keep, drop: both take lists of years
        Re-runs of this method are cumulative: cannot retrieve dropped years
        without re-initializing your Retail Reader object
        """
        # go through each of the four file types, and keep only
        # the keys that correspond to the years we want

        def aux_filter_years(orig_dict, keep = None, drop = None):
            new_dict = orig_dict
            if keep:
                new_dict = {y: f for y, f in new_dict.items() if y in keep}
            if drop:
                new_dict = {y: f for y, f in new_dict.items() if y not in drop}
            return new_dict

        self.dict_stores = aux_filter_years(self.dict_stores, keep = keep, drop = drop)
        self.dict_rms = aux_filter_years(self.dict_rms, keep = keep, drop = drop)
        self.dict_extra = aux_filter_years(self.dict_extra, keep = keep, drop = drop)
        self.dict_sales = aux_filter_years(self.dict_sales, keep = keep, drop = drop)


        new_years = self.all_years

        if keep:
            new_years = {y for y in new_years if y in keep}
        if drop:
            new_years = {y for y in new_years if y not in drop}

        self.all_years = new_years
        if self.verbose == True:
            print('Years Left: ', self.all_years)
        return

    # Filter Groups & Modules
    # structured Similarly Enough to Combine
    def filter_sales(self, keep_groups = None, drop_groups = None,
                     keep_modules = None, drop_modules = None):
        """
        Function: filters sales by group or module before reading in files
        to save space and memory
        Arguments: keep_groups, drop_groups: take lists of product group codes
        keep_modules_drop_modules: take lists of product module codes

        Filter sales: keep certain product groups and/or modules
        Re-runs of this method are cumulative: cannot retrieve dropped categories
        without re-initializing your Retail Reader object
        """

        def aux_filter_sales(orig_dict, func = self.get_group,
                             keep = None, drop = None):
            new_dict = orig_dict
            if keep:
                new_dict = {y: [ f  for f in new_dict[y]
                                if func(f) in keep]
                            for y in new_dict.keys()
                            }
            if drop:
                new_dict = {y: [ f  for f in new_dict[y]
                                if func(f) not in drop]
                            for y in new_dict.keys()
                            }
            return new_dict

        self.dict_sales = aux_filter_sales(self.dict_sales,
                                           self.get_group,
                                           keep = keep_groups,
                                           drop = drop_groups)

        self.dict_sales = aux_filter_sales(self.dict_sales,
                                           self.get_module,
                                           keep = keep_modules,
                                           drop = drop_modules)

        self.all_groups = {self.get_group(f)
                           for y in self.dict_sales.keys()
                           for f in self.dict_sales[y]
                           }
        self.all_modules = {self.get_module(f)
                           for y in self.dict_sales.keys()
                           for f in self.dict_sales[y]
                           }
        if self.verbose == True:
            print('Groups Left: ', self.all_groups)
            print('Modules Left: ', self.all_modules)

        return


    # Begin READING Process
    # this is quite memory intensive!
    # make sure you have filtered years and sales as much as possible
    def read_rms(self):
        """
        Function: populates self.df_rms

        Read in the RMS versions files
        Some UPCs are reuses of UPCs from previous years
        Nielsen notes the products may have changed sufficiently
        And codes these as a new product "version" in the later years
        Columns: upc, upc_ver_uc, panel_year
        See Nielsen documentation for a full description of these variables.
        """
        parse_opt = csv.ParseOptions(delimiter = '\t')
        # convert the types as needed
        conv_opt = csv.ConvertOptions(column_types = dict_types)

        self.df_rms = pa.concat_tables(
            [_read_csv(self, self.dict_rms[y], parse_options = parse_opt, convert_options = conv_opt)
             for y in self.dict_rms.keys()]
            )

        _validate_columns(self.df_rms.column_names, EXPECTED_RMS_COLS, "rms_versions")

        if self.verbose:
            print('Successfully Read in the RMS Files')
        return

    def read_products(self, upc_list=None,
                     keep_groups = None, drop_groups = None,
                     keep_modules = None, drop_modules = None,
                     keep_departments=None, drop_departments=None):
        """
        Function: populates self.df_products

        Arguments: 
            Required: RetailReader or PanelReader object
            Optional: keep_groups, drop_groups, keep_modules, drop_modules,
            upc_list
            Each takes a list of group codes, module codes, or upcs

        Select the Product file and read it in
        Common to both the Retail Reader and Panel Reader files
        
        Options:
        upc_list: a list of integer UPCs to select, ignores versioning by Nielsen
        keep_groups, drop_groups: selects or drops product group codes
        keep_modules, drop_modules: selects or drops product module codes
        
        Columns: upc, upc_ver_uc, upc_descr, product_module_code, product_module_descr,
        product_group_code, product_group_descr, department_code,
        department_descr, brand_code_uc, brand_descr, multi,
        size1_code_uc, size1_amount, size1_units, dataset_found_uc, 
        size1_change_flag_uc
        See Nielsen documentation for a full description of these variables.
        """
        get_products(self, upc_list=upc_list,
                     keep_groups=keep_groups, drop_groups=drop_groups,
                     keep_modules=keep_modules, drop_modules=drop_modules,
                     keep_departments=keep_departments, drop_departments=drop_departments)
        return
    def read_extra(self, years = None, upc_list = None):
        """

        Function: populates self.df_extra

        Select the Extra [characteristics] file and read it in
        Common to both the Retail Reader and Panel Reader files
        Filter Options:
        Sometimes UPCs have repeat entries, but these tend
        to be due to missing data and reporting issues, not changes. Nielsen
        codes product changes as different product versions.
        
        Module and Group selections not possible for the extra files. 
        One option is to select modules and groups in the product data and then
        merge. 

        Columns: upc, upc_ver_uc, panel_year, flavor_code, flavor_descr, 
        form_code, form_descr, formula_code, formula_descr, container_code, 
        container_descr, salt_content_code, salt_content_descr, style_code, 
        style_descr, type_code, type_descr, product_code, product_descr, 
        variety_code, variety_descr, organic_claim_code, organic_claim_descr, 
        usda_organic_seal_code, usda_organic_seal_descr, 
        common_consumer_name_code, common_consumer_name_descr, 
        strength_code, strength_descr, scent_code, scent_descr, 
        dosage_code, dosage_descr, gender_code, gender_descr,
        target_skin_condition_code, target_skin_condition_descr, 
        use_code, use_descr, size2_code, size2_amount, size2_units


        See Nielsen documentation for a full description of these variables.
        """
        get_extra(self, years = years, upc_list = upc_list)
        return


    # Read in the Stores File
    # again, common to all groups and modules, so if you are filtering products
    # keep in mind the stores files will be common

    def read_stores(self):
        """
        Function: populates self.df_stores
        Output: self.df_stores will be populated
        Read in stores files, which are common to all groups and modules
        If you are filtering products, note that stores will be common

        Columns: store_code_uc, year, parent_code, retailer_code,
        channel_code, store_zip3, fips_state_code, fips_state_descr,
        fips_county_code, fips_county_descr

        See Nielsen documentation for a full description of these variables.
        """
        parse_opt = csv.ParseOptions(delimiter = '\t')
        conv_opt = csv.ConvertOptions(column_types = dict_types)
        # renaming the year column for easier merging later on
        tab_stores = pa.concat_tables([_read_csv(self, f,
                                                   parse_options = parse_opt,
                                                   convert_options = conv_opt
                                                   )
                                      for f in self.dict_stores.values()]
                                     )

        _validate_columns(tab_stores.column_names, EXPECTED_STORE_COLS, "stores")

        # harmonize the column name for years
        my_dict = {'year':'panel_year'}
        col_names = [x if x not in my_dict else my_dict[x] for x in tab_stores.column_names]
        self.df_stores = tab_stores.rename_columns(col_names)

        # fill blanks with zeroes
        # df_stores = df_stores.fillna(0)

        if self.verbose == True:
            print('Successfully Read in Stores Files')

        return

    # Filter Stores by DMA, States, and Channel
    def filter_stores(self, keep_dmas = None, drop_dmas = None,
                      keep_states = None, drop_states = None,
                      keep_channels = None, drop_channels = None):
        """
        Function: filters self.df_stores based on DMA, state, or channel
        Must have read in df_stores first (cannot be empty)
        Filters stores based on DMA, State, and Channel
        
        See Nielsen documentation for a full description of these variables.

        """


        # make sure you have read in the stores files first
        if len(self.df_stores) == 0:
            self.read_stores()

        if self.verbose == True:
            print('Initial Store Count: ', len(self.df_stores))

        df_stores = self.df_stores
        my_filter = pc.greater(df_stores['store_code_uc'],0)

        if keep_dmas:
            my_filter = pc.and_(my_filter, pc.is_in(self.df_stores['dma_code'], value_set=pa.array(keep_dmas, pa.uint16())))

        if drop_dmas:
            my_filter = pc.and_not(my_filter, pc.is_in(self.df_stores['dma_code'], value_set=pa.array(drop_dmas, pa.uint16())))

        if keep_channels:
            my_filter = pc.and_(my_filter, pc.is_in(self.df_stores['channel_code'], value_set=pa.array(keep_channels, pa.string())))

        if drop_channels:
            my_filter = pc.and_not(my_filter, pc.is_in(self.df_stores['channel_code'], value_set=pa.array(drop_channels, pa.string())))

        if keep_states:
            my_filter = pc.and_(my_filter, pc.is_in(self.df_stores['fips_state_descr'], value_set=pa.array(keep_states, pa.string())))

        if drop_states:
            my_filter = pc.and_not(my_filter, pc.is_in(self.df_stores['fips_state_descr'], value_set=pa.array(drop_states, pa.string())))

        self.df_stores = self.df_stores.filter(my_filter)

        if self.verbose == True:
            print('Final Store Count: ', len(self.df_stores))
        return

    # Now, turn our attention to the Movement Files, i.e. the Sales
    # you should have already filtered the years that you want
    # NOTE: read only those sales corresponding to the filtered stores
    # ask: do you want to include the promotional columns?

    def read_sales(self, incl_promo = True, add_dates=False, agg_function=None, **kwargs):
        """
        Function: populates self.df_sales
        Note the method takes very long!

        Reads in the sales data, post filter if you have applied any
        Uses pyarrow methods to filter and read the data without
        taking up huge amounts of memory. But it still requires a large amount
        of memory and CPU 
        depending on the selected stores, years, groups, and modules
        Columns: store_code_uc, upc, week_end, units, prmult, price, feature,
        display

        See Nielsen documentation for a full description of these variables.        
        """

        # Get the relevant stores
        if len(self.df_stores) == 0:
            self.read_stores()

        if len(self.df_rms) ==0:
            self.read_rms()

        # select columns
        my_cols = ['store_code_uc', 'upc', 'week_end', 'units', 'prmult', 'price']

        if incl_promo == True:
            my_cols = my_cols + ['feature', 'display']

        # for each module-year, clean up the data frame
        # optional: add_dates: calculate the month and quarter        
        def aux_clean(df_tab, add_dates=False):
            # original format is 20050731
            # NOTE different from the more formal year function (CC: not as far as I can tell)
            def replace(tab, name, values):
                return tab.set_column(tab.schema.get_field_index(name), name, values)

            df_tab = replace(df_tab, 'week_end', pc.strptime(pc.cast(df_tab['week_end'], pa.string()),
                                                             format='%Y%m%d', unit='ns'))

            if 'feature' in df_tab.column_names:
                fill_value = pa.scalar(-1, type=pa.int8())
                df_tab = replace(df_tab, 'feature', pc.fill_null(df_tab['feature'], fill_value))
                df_tab = replace(df_tab, 'display', pc.fill_null(df_tab['display'], fill_value))

            # Compute unit price and year and add upc_ver_uc
            df_tab = df_tab.append_column('unit_price', pc.divide(df_tab['price'],df_tab['prmult']))
            df_tab = df_tab.append_column('panel_year', pc.cast(pc.year(df_tab['week_end']),pa.uint16()))
            df_tab = df_tab.append_column('revenue', pa.compute.multiply(df_tab['units'], df_tab['unit_price']))

            df_tab = df_tab.join(self.df_rms, keys=["upc","panel_year"],join_type='left outer')
            df_tab = df_tab.join(self.df_stores.select(['store_code_uc','panel_year','dma_code','retailer_code','parent_code']),
                keys=["store_code_uc","panel_year"],join_type='left outer')
            
            if add_dates:
                df_tab = df_tab.append_column('quarter', _quarter_end(df_tab['week_end']))
                df_tab = df_tab.append_column('month', pc.floor_temporal(df_tab['week_end'], unit='month'))

            return df_tab

        # have to read one module-year at a time
        # as a pandas table, which we will later concatenate
        def aux_read_mod_year(filename, list_stores = None,  add_dates=False, agg_function=None, **kwargs):

            parse_opt = csv.ParseOptions(delimiter = '\t')
            conv_opt = csv.ConvertOptions(column_types = dict_types,
                                          include_columns = my_cols)
            # is a dataset object that can be turned into a table
            # but we can also filter immediately if we like
            pa_my = pads.dataset(_read_csv(self, filename,
                                              parse_options = parse_opt,
                                              convert_options=conv_opt))

            if list_stores is None:
                pa_tab = aux_clean(pa_my.to_table(), add_dates)
            else:
                pa_tab = aux_clean(pa_my.to_table(filter=pads.field('store_code_uc').isin(list_stores)), add_dates)

            # The joins in aux_clean are multithreaded and return rows in a different order on
            # every run. Sort on all the raw columns (store-upc-week is not unique in the
            # Movement files) so that output and any float sums are reproducible.
            pa_tab = pa_tab.sort_by([(c, 'ascending') for c in my_cols])

            if agg_function:
                return agg_function(pa_tab, **kwargs)
            else:
                return pa_tab

        # read all the modules (and groups) for one year
        def aux_read_year(year, add_dates, agg_function=None, **kwargs):
            # get the list of stores that were present in the year of choice
            # CC: can we keep this as pa.Array()?
            list_stores = self.df_stores['store_code_uc'].filter(pc.equal(self.df_stores['panel_year'],year)).to_pylist()

            pa_y = pa.concat_tables([aux_read_mod_year(f, list_stores, add_dates, agg_function, **kwargs)
                                     for f in sorted(self.dict_sales[year])
                                     ])

            # still a table object, not a pandas dataframe
            # since we will be concatenating years together, presumably?
            return pa_y

        if self.verbose == True:
            print('Reading Sales')
            tick()
        
        
        # This does the work -- keep as PyArrow table
        self.df_sales = pa.concat_tables([aux_read_year(y, add_dates, agg_function, **kwargs) for y in sorted(self.dict_sales.keys())])
        
        # Merge the RMS (upc_ver_uc) and store (dma, retailer_code)

        if self.verbose == True:
            print('Finished Sales')
            tock()

        # NOTE: ORIGINAL CODE MERGES THIS WITH df_stores
        # # finally, drop the stores that have no sales
        if 'store_code_uc' in self.df_sales.column_names:
            self.df_stores = self.df_stores.filter(
                pc.is_in(self.df_stores['store_code_uc'],
                pc.unique(self.df_sales['store_code_uc'])))

        # Filter products for only those in sales data
        if 'upc' in self.df_sales.column_names:
            sales_upcs = pc.unique(self.df_sales['upc'])
            if self.df_products.num_rows:  # skipped when read_products() was not called
                self.df_products = self.df_products.filter(
                    pc.is_in(self.df_products['upc'], value_set=sales_upcs))

        return

    def write_data(self, dir_write = path.Path.cwd(), stub = 'out',
                   compr = 'brotli', as_table = False,
                   separator = 'panel_year'):

        """
        Function: writes data to parquet files
        Arguments:
            dir_write: Path to output directory (default: cwd)
            stub: prefix for output filenames (default: 'out')
            compr: compression type (default: 'brotli')
            as_table: if True, writes sales as a partitioned parquet dataset
            separator: column on which to partition when as_table=True

        Note: will save all non-empty datasets
        i.e. any datasets for which the read* method has been applied
        """

        # most important: define a writing directory
        # if none specified, use the current working directory
        self.dir_write = dir_write

        if self.verbose == True:
            print('Writing to', dir_write)

        f_stores = self.dir_write / '{stub}_stores.parquet'.format(stub=stub)
        f_sales = self.dir_write /'{stub}_sales.parquet'.format(stub=stub)
        f_products = self.dir_write / '{stub}_products.parquet'.format(stub=stub)
        f_extra = self.dir_write /'{stub}_extra.parquet'.format(stub=stub)

        aux_write_direct(self.df_stores, f_stores, compr=compr)
        aux_write_direct(self.df_products, f_products, compr=compr)
        aux_write_direct(self.df_extra, f_extra, compr=compr)

        if as_table == False:
            aux_write_direct(self.df_sales, f_sales, compr=compr)
        else:
            dir_sales = self.dir_write / '{stub}_sales'.format(stub=stub)

            pq.write_to_dataset(self.df_sales,
                root_path=dir_sales,
                partition_cols=[separator],
                compression=compr)

            if self.verbose == True:
                print('Wrote Dataset to {d} and partition {sep}'.format(d=dir_sales, sep=separator))
        return

# %% Defining the PanelReader class
class PanelReader(object):
    """
    Object class to read in Nielsen Household Panel Data
    Files created:
        df_extra: from Annual product_extra files
        df_products: from Master product file
        df_panelists: from Annual panelists files
        df_purchases: from Annual purchases files
        df_retailers: from Master retailers files
        df_trips: from Annual trips files
        df_variations: from Master brand_variations files
    Many filtering options available

    """
    def __init__(self, dir_read = path.Path.cwd(), verbose = True, extract_dir = None):
        """
        Function: initialize a PanelReader object
        identifies file names and locations for each dataset
        Will throw errors if any critical files are missing or incorrectly named
        """
        self.verbose = verbose

        self.dir_read = dir_read
        # .tgz archives are extracted once to this folder and read from there (optional)
        self.extract_dir = extract_dir
        self.files = get_files(self)

        # locate the common master files
        self.files_master = [f for f in self.files
                             if _is_master_files(f.parts[-3])]

        self.files_annual = _keep_shallowest(
            [f for f in self.files if f.parts[-2] == 'Annual_Files'],
            lambda f: f.name, dir_read, "Annual files")

        def master(name):
            return _keep_shallowest(
                [f for f in self.files if
                 (f.name == name)&
                 (f.parent.name == 'Latest')&
                 _is_master_files(f.parent.parent.name)&
                 ('Revised_Panelist_Files' not in f.parts)],
                lambda f: f.name, dir_read, "Master files")

        self.files_product = master('products.tsv')
        self.files_variations = master('brand_variations.tsv')
        self.files_retailers = master('retailers.tsv')



        if not self.files_annual:
            raise FileNotFoundError(
                f"Could not find Annual Files (panelists, purchases, trips) in {dir_read}.")

        try:
            self.all_years = set([get_year(x) for x in self.files_annual])
        except Exception as e:
            raise ValueError(
                f"Could not get year from Annual files in {dir_read}. "
                f"Keep original Nielsen file structure. Error: {e}") from e

        # then, partition into panelists, extra, purchases, and trips
        # note that extra is EXACTLY the same as in

        self.files_trips = [f for f in self.files_annual if 'trips' in f.name]
        self.files_panelists = [f for f in self.files_annual if 'panelists' in f.name]
        self.files_purchases = [f for f in self.files_annual if 'purchases' in f.name]
        self.files_extra = [f for f in self.files_annual if 'products_extra' in f.name]

        self.dict_trips = {y: [f for f in self.files_trips
                               if get_year(f) == y]
                               for y in self.all_years}
        self.dict_panelists = {y: [f for f in self.files_panelists
                               if get_year(f) == y]
                               for y in self.all_years}
        self.dict_purchases = {y: [f for f in self.files_purchases
                               if get_year(f) == y]
                               for y in self.all_years}
        self.dict_extra = {y: [f for f in self.files_extra
                               if get_year(f) == y]
                               for y in self.all_years}


        self.df_products = pa.table({})
        self.df_variations = pa.table({})
        self.df_retailers = pa.table({})

        self.df_panelists = []
        self.df_trips = []
        self.df_purchases = []

        self.df_extra = pa.table({})

        # NOTE some of these are repeats from RR
        # we will therefore append _panel to file names

        # NOTE skipping the cols_hh, cols_prod thing

        return
    
    # Begin a Proper Cleanup: filter years, groups, modules, etc.
    def filter_years(self, keep = None, drop = None):
        """
        Function: selects years of trips to include
        Arguments: 
            optional: keep, drop: take lists of years
        """
        # go through each of the four file types, and keep only
        # the keys that correspond to the years we want
    
        def aux_filter_years(orig_dict, keep = None, drop = None):
            new_dict = orig_dict
            if keep:
                new_dict = {y: f for y, f in new_dict.items() if y in keep}
            if drop:
                new_dict = {y: f for y, f in new_dict.items() if y not in drop}
            return new_dict
    
        self.dict_trips = aux_filter_years(self.dict_trips, keep = keep, drop = drop)
        self.dict_panelists = aux_filter_years(self.dict_panelists, keep = keep, drop = drop)
        self.dict_purchases = aux_filter_years(self.dict_purchases, keep = keep, drop = drop)
        self.dict_extra = aux_filter_years(self.dict_extra, keep = keep, drop = drop)


        new_years = self.all_years

        if keep:
            new_years = {y for y in new_years if y in keep}
        if drop:
            new_years = {y for y in new_years if y not in drop}
    
        self.all_years = new_years
        if self.verbose == True:
            print('Years Left: ', self.all_years)
        return


    # Read in the Products File
    # can limit to a subset of UPCs
    # NOTE: REMOVED the ability to take out groups and modules here
    # since we don't have that in RetailReader either
    # this is an EXACT copy of the RetailReader function


    def read_retailers(self):
        """
        Function: populates self.df_retailers
        Arguments: none (no filtering here)
        Columns: retailer_code, channel_type
        
        See Nielsen documentation for a full description of these variables.        

        """
        read_opt = csv.ReadOptions(encoding='latin')
        parse_opt = csv.ParseOptions(delimiter = '\t')
        conv_opt = csv.ConvertOptions(column_types = dict_types)

        if self.files_retailers:
            self.file_retailers = self.files_retailers[0]
        else:
            raise FileNotFoundError(
                f"Could not find retailers.tsv under Master_Files/Latest in {self.dir_read}. "
                "Check folder name and make sure folder is unzipped.")
        
        

        self.df_retailers = _read_csv(self, self.file_retailers,
                               read_options = read_opt,
                               parse_options = parse_opt,
                               convert_options = conv_opt)

        if self.verbose:
            print('Successfully Read in Retailers with', self.df_retailers.num_rows, 'rows')

        return

    def read_products(self, upc_list=None,
                     keep_groups = None, drop_groups = None,
                     keep_modules = None, drop_modules = None,
                     keep_departments=None, drop_departments=None):
        """
        Function: populates self.df_products
        
        Arguments: 
            Required: RetailReader or PanelReader object
            Optional: keep_groups, drop_groups, keep_modules, drop_modules,
            upc_list
            Each takes a list of group codes, module codes, or upcs
        
        Select the Product file and read it in
        Common to both the Retail Reader and Panel Reader files
        
        Options:
        upc_list: a list of integer UPCs to select, ignores versioning by Nielsen
        keep_groups, drop_groups: selects or drops product group codes
        keep_modules, drop_modules: selects or drops product module codes
        
        Columns: upc, upc_ver_uc, upc_descr, product_module_code, product_module_descr,
        product_group_code, product_group_descr, department_code,
        department_descr, brand_code_uc, brand_descr, multi,
        size1_code_uc, size1_amount, size1_units, dataset_found_uc, 
        size1_change_flag_uc
        See Nielsen documentation for a full description of these variables.
        """

        get_products(self, upc_list=upc_list,
                     keep_groups=keep_groups, drop_groups=drop_groups,
                     keep_modules=keep_modules, drop_modules=drop_modules,
                     keep_departments=keep_departments, drop_departments=drop_departments)

        return


    def read_extra(self, years = None, upc_list = None):
        """
        
        Function: populates self.df_extra
        
        Select the Extra [characteristics] file and read it in
        Common to both the Retail Reader and Panel Reader files
        Filter Options:
        upc_list: a list of integer UPCs to select, ignores versioning by Nielsen
        years (not recommended): selects extra characteristics that are associated with a year
        in the Nielsen data. Sometimes UPCs have repeat entries, but these tend
        to be due to missing data and reporting issues, not changes. Nielsen
        codes product changes as different product versions.
        
        Module and Group selections not possible for the extra files. 
        One option is to select modules and groups in the product data and then
        merge. 
        
        Columns: upc, upc_ver_uc, panel_year, flavor_code, flavor_descr, 
        form_code, form_descr, formula_code, formula_descr, container_code, 
        container_descr, salt_content_code, salt_content_descr, style_code, 
        style_descr, type_code, type_descr, product_code, product_descr, 
        variety_code, variety_descr, organic_claim_code, organic_claim_descr, 
        usda_organic_seal_code, usda_organic_seal_descr, 
        common_consumer_name_code, common_consumer_name_descr, 
        strength_code, strength_descr, scent_code, scent_descr, 
        dosage_code, dosage_descr, gender_code, gender_descr,
        target_skin_condition_code, target_skin_condition_descr, 
        use_code, use_descr, size2_code, size2_amount, size2_units
        
        
        See Nielsen documentation for a full description of these variables.
        """
        get_extra(self, years = years, upc_list = upc_list)
        return


    def read_variations(self):
        """
        Function: populates self.df_variations with data from brand_variations
        Arguments: none

        Columns: brand_code_uc, brand_descr, brand_descr_alternative, 
        start_date, end_date, datasets_found_uc

        See Nielsen documentation for a full description of these variables.        


        """
        read_opt = csv.ReadOptions(encoding='latin')
        parse_opt = csv.ParseOptions(delimiter = '\t')
        conv_opt = csv.ConvertOptions(column_types = dict_types)
        
        if self.files_variations:
            self.file_variations = self.files_variations[0]
        else:
            raise FileNotFoundError(
                f"Could not find brand_variations.tsv under Master_Files/Latest in {self.dir_read}. "
                "Check folder name and make sure folder is unzipped.")
        
        
        
        self.df_variations = _read_csv(self, self.file_variations,
                               read_options = read_opt,
                               parse_options = parse_opt,
                               convert_options = conv_opt)

        if self.verbose:
            print('Successfully Read in Brand Variations with', self.df_variations.num_rows, 'rows')

        return


    def read_year(self, year, keep_dmas = None, drop_dmas = None,
        keep_states = None, drop_states = None, keep_stores=None, add_household=False):
        """
        Function: reads a single year of panel data (an auxiliary method)
        Arguments: required: year
        optional: keep_states, drop_states: list of states in two-letter format
        keep_dmas, drop_dmas: list of DMA codes

        See Nielsen documentation for a full description of these variables.        

        """
        try:
            f_trips = self.dict_trips[year][0]
        except (KeyError, IndexError) as e:
            raise FileNotFoundError(f"Could not find trip file for year {year}") from e

        try:
            f_purchases = self.dict_purchases[year][0]
        except (KeyError, IndexError) as e:
            raise FileNotFoundError(f"Could not find purchases file for year {year}") from e

        try:
            f_panelists = self.dict_panelists[year][0]
        except (KeyError, IndexError) as e:
            raise FileNotFoundError(f"Could not find panelists file for year {year}") from e

        parse_opt = csv.ParseOptions(delimiter = '\t')
        conv_opt = csv.ConvertOptions(column_types = dict_types,
                                      auto_dict_encode = True,
                                      auto_dict_max_cardinality = 1024)
        ds_panelists = pads.dataset(_read_csv(self, f_panelists,
                                                 parse_options = parse_opt,
                                                 convert_options = conv_opt))

        panelist_filter = pads.field('Projection_Factor') > 0

        if keep_states:
            panelist_filter = panelist_filter & (pads.field('Fips_State_Desc'
                                                            ).isin(keep_states))
        if drop_states:
            panelist_filter = panelist_filter & (~pads.field('Fips_State_Desc'
                                                             ).isin(drop_states))
        if keep_dmas:
            panelist_filter = panelist_filter & (pads.field('DMA_Cd').isin(keep_dmas))
        
        if drop_dmas:
            panelist_filter = panelist_filter & (~pads.field('DMA_Cd').isin(drop_dmas))

        # Get the Panelist Table Filtered
        df_panelists = ds_panelists.to_table(filter = panelist_filter)
        _validate_columns(df_panelists.column_names, EXPECTED_PANELIST_COLS,
                          f"panelists ({year})")
        col_names = [x if x not in dict_column_map else dict_column_map[x] for x in df_panelists.column_names]
        df_panelists = df_panelists.rename_columns(col_names)

        # Get a list of Unique HH
        trip_filter = pads.field('household_code').isin(pc.unique(df_panelists['household_code']).to_pylist())

        if keep_stores:
            trip_filter = trip_filter & pads.field('store_code_uc').isin(keep_stores)

        df_trips = pads.dataset(_read_csv(self, f_trips,
                    parse_options = parse_opt,
                    convert_options = conv_opt)
                    ).to_table(filter = trip_filter)
        _validate_columns(df_trips.column_names, EXPECTED_TRIP_COLS,
                          f"trips ({year})", optional=OPTIONAL_TRIP_COLS)

        # Get unique UPCs from products to filter purchases (if products were read)
        has_products = self.df_products.num_rows > 0

        trip_filter_purchases = pads.field('trip_code_uc').isin(df_trips['trip_code_uc'].to_numpy())

        if has_products:
            unique_upcs = pc.unique(self.df_products['upc']).to_pylist()
            purchase_filter = trip_filter_purchases & pads.field('upc').isin(unique_upcs)
        else:
            purchase_filter = trip_filter_purchases

        ds_purchases = pads.dataset(_read_csv(self, f_purchases,
                    parse_options = parse_opt,
                    convert_options = conv_opt))\
                    .to_table(filter = purchase_filter)
        _validate_columns(ds_purchases.column_names, EXPECTED_PURCHASE_COLS,
                          f"purchases ({year})")

        df_purchases = ds_purchases.append_column('panel_year', pa.array([year]*ds_purchases.num_rows,pa.uint16()))

        # Going through numpy and pandas map cannot be fastest solution here
        if add_household:
            df_purchases=df_purchases.join(df_trips.select(['trip_code_uc','household_code']), keys=['trip_code_uc'])
            # the join returns rows in a run-dependent order; restore a fixed one
            df_purchases = df_purchases.sort_by([(c, 'ascending') for c in df_purchases.column_names])

        # add to the list
        self.df_trips.append(df_trips)
        self.df_purchases.append(df_purchases)
        self.df_panelists.append(df_panelists)


        return
        # need to have already read in products?
        # but our version of products has no differences
        #unique_upc = self.df_products['upc'].unique()


    def read_annual(self, keep_states = None, drop_states = None,
                    keep_dmas = None, drop_dmas = None, keep_stores=None, add_household=False):
        """
        Function: populates all annual datasets, except df_extra:
            df_panelists
            df_purchases
            df_trips

        Arguments: optional: keep_states, drop_states, keep_dmas, drop_dmas:
            keeps households in the selected states and DMAs
            states taken in two-letter codes; DMAs follow Nielsen codes

        See Nielsen documentation for a full description of these variables.        

        """

        # read in all the years

        for year in sorted(self.all_years):
            print('Processing Year', year)
            tick()
            self.read_year(year, keep_states = keep_states,
                           drop_states = drop_states,
                           keep_dmas = keep_dmas,
                           drop_dmas = drop_dmas,
                           keep_stores = keep_stores,
                           add_household= add_household)
            tock()

        # Filter products for only those in sales data
        #self.df_products = self.df_products[self.df_products.upc.isin(pa.concat_tables(self.df_purchases).select(['upc'])['upc'].to_numpy())]

        print('Concatenating Tables...')
        self.df_trips = pa.concat_tables(self.df_trips, promote_options='default')#.to_pandas(self_destruct=True, split_blocks=True)
        self.df_purchases = pa.concat_tables(self.df_purchases, promote_options='default')#.to_pandas(self_destruct=True, split_blocks=True)
        self.df_panelists = pa.concat_tables(self.df_panelists, promote_options='default')#.to_pandas(self_destruct=True, split_blocks=True)

        return


    def write_data(self, dir_write = path.Path.cwd(), stub = 'out',
                   compr = 'brotli', as_table = False,
                   separator = 'panel_year'):
        """
        Function: writes the tables that have been read to parquet files
        Arguments
        Requires a directory to write to; otherwise will use current 
        working directory
        Can also include a stub, to save files as [stub]_[data].parquet
        Note: will save all non-empty datasets
        i.e. any datasets for which the read* method has been applied
        
        Can be saved intermediately as a pyarrow Table, with separators,
        Not yet: option for different separators for different files
        
        If you are looking to save just a single file with a specific separator
        RR.read_XXX()
        RR.write_data(dir_write, separator = XXX)
        since the separator for now must be common to all files
        
        Always saves as parquets with compression of your choice
        (default: brotli)
        """

        # most important: define a writing directory
        # if none specified, use the current working directory
        self.dir_write = dir_write
    
        if self.verbose == True:
            print('Writing to', dir_write)
    
        f_products = self.dir_write / '{stub}_products.parquet'.format(stub=stub)
        f_variations = self.dir_write /'{stub}_variations.parquet'.format(stub=stub)
        f_retailers = self.dir_write / '{stub}_retailers.parquet'.format(stub=stub)
        f_trips = self.dir_write / '{stub}_trips.parquet'.format(stub=stub)
        f_panelists = self.dir_write / '{stub}_panelists.parquet'.format(stub=stub)
        f_purchases = self.dir_write / '{stub}_purchases.parquet'.format(stub=stub)
        f_extra = self.dir_write /'{stub}_extra.parquet'.format(stub=stub)

        print(dir_write)

        if as_table == False:
            aux_write_direct(self.df_products, f_products)
            aux_write_direct(self.df_variations, f_variations)
            aux_write_direct(self.df_retailers, f_retailers)
            aux_write_direct(self.df_extra, f_extra)

            aux_write_direct(self.df_trips, f_trips)
            aux_write_direct(self.df_panelists, f_panelists)
            aux_write_direct(self.df_purchases, f_purchases)
    
            return # end the job right here
    
    
        def aux_write_separated(df, filename, separator = 'panel_year',
                            compr = 'brotli'):
            if isinstance(df, pa.Table):
                if df.num_rows == 0:
                    return
                col_names = df.column_names
            else:
                return

            if separator not in col_names:
                if self.verbose:
                    print('Separator not found in table. Writing directly')
                aux_write_direct(df, filename, compr)
                return

            # Write row groups separated by the separator column
            seps = pc.unique(df[separator]).to_pylist()
            table0 = df.filter(pc.equal(df[separator], seps[0]))
            writer = pq.ParquetWriter(filename, table0.schema,
                                      compression = compr)
            writer.write_table(table0)
            for sep_val in seps[1:]:
                table_part = df.filter(pc.equal(df[separator], sep_val))
                writer.write_table(table_part)
            writer.close()

            if self.verbose:
                print('Wrote Data to {f} with row groups by {sep}'.format(
                    f=filename, sep=separator))

            return
    
        # can separate out the files and write them as pyarrow tables
        # create separate dataframes and avoid overwhelming your system, i guess
        aux_write_separated(self.df_products, f_products)
        aux_write_separated(self.df_variations, f_variations)
        aux_write_separated(self.df_retailers, f_retailers)
        aux_write_separated(self.df_trips, f_trips)
        aux_write_separated(self.df_panelists, f_panelists)
        aux_write_separated(self.df_purchases, f_purchases)
        aux_write_separated(self.df_extra, f_extra)

    # Revised Panelist Files
    # Updates the usual Panel files with the revisions
    def read_revised_panelists(self):
        """
        Function: corrects the panelist files using errata from the Panel files
        Every year, Nielsen has some panelists whose data they revise

        Revisions replace values for the same keys: panelists by (household_code, panel_year),
        products by (upc, upc_ver_uc), brand variations by (brand_code_uc,
        brand_descr_alternative), retailers by retailer_code. A table that has not been read
        (e.g. read_retailers() not called) or has no revision file is left as it is. Years
        without a revised panelist file are skipped.

        Must have already run read_annual() (and read_products(), read_variations(),
        read_retailers() for the tables to be revised).
        """

        self.files_revised = [f for f in self.files if
                              f.parent.parent.parent.name == 'Revised_Panelist_Files'
                              ]

        self.files_panelist_revised = [f for f in self.files_revised if
                                       'panelists' in f.name and
                                       int(f.parent.parent.name)
                                       in self.all_years]

        tsv = csv.ParseOptions(delimiter='\t')
        for f in sorted(self.files_panelist_revised):
            revision = _read_csv(self, f, parse_options=tsv,
                                 convert_options=csv.ConvertOptions(column_types=dict_types))
            revision = revision.rename_columns([COLUMN_RENAME_MAP.get(c, c) for c in revision.column_names])
            self.df_panelists = _update_by_key(self.df_panelists, revision,
                                               ['household_code', 'panel_year'], f"Revised panelists {f.name}")

        # the master files (unquoted, UTF-8)
        unquoted = csv.ParseOptions(delimiter='\t', quote_char=False)
        for attr, name, keys in [('df_products', 'products', ['upc', 'upc_ver_uc']),
                                 ('df_variations', 'brand_variations', ['brand_code_uc', 'brand_descr_alternative']),
                                 ('df_retailers', 'retailers', ['retailer_code'])]:
            files = [f for f in self.files_revised if f.stem == name]
            table = getattr(self, attr)
            if not files or not isinstance(table, pa.Table) or table.num_rows == 0:
                continue
            revision = _read_csv(self, files[0], read_options=csv.ReadOptions(encoding='utf8'),
                                 parse_options=unquoted,
                                 convert_options=csv.ConvertOptions(column_types=dict_types))
            setattr(self, attr, _update_by_key(table, revision, keys, f"Revised {name}"))

        return

    # Look through open issues
    # Currently somehwat ad-hoc: can fix the Flavor Code + Male Birth Month
    # Will have to udpate as issues close and open

    def process_open_issues(self):
        """
        Function: addresses the open issues in the Nielsen Panel data for which
        Kilts distributes supplement files (OpenIssues_SupplementFiles)

        Issue 1: flavor code and description are missing from the 2010 extra
        attributes. Filled from Latest_Flavor_2010.csv by (upc, panel_year = 2010).
        That file writes some UPCs as floats just below the integer
        (85315210097.9999 for 85315210098), so UPCs are rounded. UPCs listed with
        more than one flavor are left missing (there is no basis for choosing).

        Issue 2: household heads' birth months were wrong in 2004-2007 and 2010.
        The panelist files now carry the birth year only (YYYY), and the
        corrections change months only, so there is nothing to apply. The years
        in the supplement are compared with df_panelists and any disagreement is
        reported; the data are not changed.

        Affected files: df_extra (issue 1); df_panelists is only checked (issue 2)
        """

        self.files_issues = [f for f in self.files if
                            'OpenIssues_SupplementFiles' in f.parts]

        self.open_issues = set([f.parent.name for f in self.files_issues])

        print('Current Open Issues:', self.open_issues)

        # Issue 1: 2010 flavor codes
        flavor_files = [f for f in self.files_issues if f.name == 'Latest_Flavor_2010.csv']
        if flavor_files and isinstance(self.df_extra, pa.Table) and self.df_extra.num_rows:
            flavor = _read_csv(self, flavor_files[0])
            upc = pc.cast(pc.round(pc.cast(flavor['upc'], pa.float64())), pa.uint64())
            flavor = pa.table({'upc': upc, 'panel_year': pa.array([2010] * flavor.num_rows, pa.uint16()),
                               'flavor_code': flavor['flavor_code'], 'flavor_descr': flavor['flavor_descr']})
            counts = flavor.group_by('upc').aggregate([([], 'count_all')])
            single = counts.filter(pc.equal(counts['count_all'], 1))['upc']
            ambiguous = counts.num_rows - len(single)
            if ambiguous:
                warnings.warn(f"Latest_Flavor_2010.csv: {ambiguous} UPCs have more than one flavor and are left missing",
                              UserWarning, stacklevel=2)
            flavor = flavor.filter(pc.is_in(flavor['upc'], value_set=single))
            self.df_extra = _update_by_key(self.df_extra, flavor, ['upc', 'panel_year'], "2010 flavor codes")

        # Issue 2: birth years (check only)
        birth_files = [f for f in self.files_issues if f.parent.name == 'Panelist_maleHeadBirth_femaleHeadBirth'
                       and f.suffix == '.tsv']
        if birth_files and isinstance(self.df_panelists, pa.Table) and self.df_panelists.num_rows:
            for f in sorted(birth_files):
                year = 2000 + int(f.name.split('_')[1])  # panel_10_birth_dates_corrected.tsv
                if year not in self.all_years:
                    continue
                births = _read_csv(self, f, parse_options=csv.ParseOptions(delimiter='\t'),
                                   convert_options=csv.ConvertOptions(column_types={
                                       'household_id': pa.uint32(), 'male_head_birth': pa.string(),
                                       'female_head_birth': pa.string()}))
                panel = self.df_panelists.filter(pc.equal(self.df_panelists['panel_year'], year))
                joined = births.join(panel.select(['household_code', 'Male_Head_Birth', 'Female_Head_Birth']),
                                     keys='household_id', right_keys='household_code', join_type='inner',
                                     use_threads=False)
                for supplement, column in [('male_head_birth', 'Male_Head_Birth'),
                                           ('female_head_birth', 'Female_Head_Birth')]:
                    corrected = pc.cast(pc.utf8_slice_codeunits(pc.if_else(pc.equal(joined[supplement], '-'), None,
                                                                           joined[supplement]), 0, 4), pa.string())
                    current = pc.cast(joined[column], pa.string())
                    current = pc.if_else(pc.equal(current, ''), None, current)
                    same = pc.or_kleene(pc.equal(corrected, current), pc.and_(pc.is_null(corrected), pc.is_null(current)))
                    differ = joined.num_rows - pc.sum(pc.fill_null(same, False).cast(pa.int64())).as_py()
                    if differ:
                        warnings.warn(f"{f.name}: {column} year differs from the panelist file for {differ} households",
                                      UserWarning, stacklevel=2)

        return

