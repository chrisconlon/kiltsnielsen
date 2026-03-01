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
import pandas as pd
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


def _validate_columns(actual_columns, expected_columns, file_description="file"):
    """Warn about missing or unexpected columns in a data file.
    Helps detect NielsenIQ format changes early.
    """
    actual = set(actual_columns)
    expected = set(expected_columns)
    missing = expected - actual
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
    """

    def __init__(self, dir_read):
        self.dir_read = dir_read
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
            with tarfile.open(tgz_path, 'r:gz') as tar:
                for member in tar.getmembers():
                    if not member.isfile():
                        continue
                    name_lower = member.name.lower()
                    if not (name_lower.endswith('.tsv') or name_lower.endswith('.csv')):
                        continue
                    # Skip macOS resource fork files
                    base = path.Path(member.name).stem
                    if base.startswith('._') or '/._' in member.name:
                        continue
                    virtual_path = self.dir_read / member.name
                    self._archive_map[virtual_path] = (tgz_path, member.name)
                    virtual_files.append(virtual_path)
        return virtual_files

    def open_file(self, virtual_path):
        """Return a binary file-like object for a file inside an archive.
        Returns None if the path is not an archive member.
        """
        if virtual_path not in self._archive_map:
            return None
        tgz_path, member_name = self._archive_map[virtual_path]
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
        if file_obj is not None:
            try:
                return csv.read_csv(pa.PythonFile(file_obj), **kwargs)
            finally:
                file_obj.close()
    return csv.read_csv(filepath, **kwargs)


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
        self._tgz_manager = TgzFileManager(self.dir_read)
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
    if isinstance(df, pa.Table):
        if df.num_rows == 0:
            return
        pq.write_table(df, filename, compression = compr)
        print('Wrote as direct parquet to', filename)
    elif isinstance(df, pd.DataFrame):
        if df.empty:
            return
        pq.write_table(pa.Table.from_pandas(df, preserve_index=False),
                        filename, compression = compr)
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
    def __init__(self, dir_read = path.Path.cwd(), verbose = True):
        """
        Function: initialize a RetailReader object
        identifies file names and locations for each dataset
        Will throw errors if any critical files are missing or incorrectly named
        """
        self.verbose = verbose

        self.dir_read = dir_read # save the folder to the class

        # get all files in the relevant folder
        self.files = get_files(self)

        # then, get the product TSV file
        # we want the one under /RMS/Master_Files/Latest
        # we do NOT want the Revised Panelist Files (if in Panel)
        self.files_product = [f for f in self.files if
                              (f.name == 'products.tsv')&
                              (f.parent.name == 'Latest')&
                              _is_master_files(f.parent.parent.name)]

        # Collect the Annual Files NOTE: currently unused
        self.files_annual = [f for f in self.files
                             if 'Annual_Files' in f.parts]


        # Collect the Movement Files, i.e. the store-weekly sales files
        self.files_sales = [f for f in self.files
                            if 'Movement_Files' in f.parts]
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

        self.df_products = pd.DataFrame()
        self.df_sales = pd.DataFrame()
        self.df_stores = pd.DataFrame()
        self.df_rms = {}
        self.df_extra = pd.DataFrame()

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
            df_tab = df_tab.set_column(2,'week_end', 
                pa.array(pd.to_datetime( df_tab['week_end'].to_numpy(), format = '%Y%m%d'),
                pa.timestamp('ns')))

            if 'feature' in df_tab.schema.to_string():
                fill_value = pa.scalar(-1, type=pa.int8())
                df_tab = df_tab.set_column(6,'feature',pa.compute.fill_null(df_tab['feature'],fill_value))
                df_tab = df_tab.set_column(7,'display',pa.compute.fill_null(df_tab['display'],fill_value))

            # Compute unit price and year and add upc_ver_uc
            df_tab = df_tab.append_column('unit_price', pc.divide(df_tab['price'],df_tab['prmult']))
            df_tab = df_tab.append_column('panel_year', pc.cast(pc.year(df_tab['week_end']),pa.uint16()))
            df_tab = df_tab.append_column('revenue', pa.compute.multiply(df_tab['units'], df_tab['unit_price']))

            df_tab = df_tab.join(self.df_rms, keys=["upc","panel_year"],join_type='left outer')
            df_tab = df_tab.join(self.df_stores.select(['store_code_uc','panel_year','dma_code','retailer_code','parent_code']),
                keys=["store_code_uc","panel_year"],join_type='left outer')
            
            if add_dates:
                my_dates=pd.DataFrame({'week_end':pa.compute.unique(df_tab['week_end']).to_pandas().sort_values(ignore_index=True)})
                my_dates['quarter']=my_dates.week_end + pd.offsets.QuarterEnd(0)
                my_dates['month']=my_dates['week_end'].astype('datetime64[M]')
                df_tab=df_tab.join(pa.Table.from_pandas(my_dates,preserve_index=False), keys=["week_end"])

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
                                     for f in self.dict_sales[year]
                                     ])

            # still a table object, not a pandas dataframe
            # since we will be concatenating years together, presumably?
            return pa_y

        if self.verbose == True:
            print('Reading Sales')
            tick()
        
        
        # This does the work -- keep as PyArrow table
        self.df_sales = pa.concat_tables([aux_read_year(y, add_dates, agg_function, **kwargs) for y in self.dict_sales.keys()])
        
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
            if isinstance(self.df_products, pa.Table):
                self.df_products = self.df_products.filter(
                    pc.is_in(self.df_products['upc'], value_set=sales_upcs))
            else:
                self.df_products = self.df_products[
                    self.df_products.upc.isin(sales_upcs.to_numpy())]

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
    def __init__(self, dir_read = path.Path.cwd(), verbose = True):
        """
        Function: initialize a PanelReader object
        identifies file names and locations for each dataset
        Will throw errors if any critical files are missing or incorrectly named
        """
        self.verbose = verbose

        self.dir_read = dir_read
        self.files = get_files(self)

        # locate the common master files
        self.files_master = [f for f in self.files
                             if _is_master_files(f.parts[-3])]

        self.files_annual = [f for f in self.files
                             if f.parts[-2] == 'Annual_Files']

        self.files_product = [f for f in self.files if
                              (f.name == 'products.tsv')&
                              (f.parent.name == 'Latest')&
                              _is_master_files(f.parent.parent.name)&
                              ('Revised_Panelist_Files' not in f.parts)
                              ]

        self.files_variations = [f for f in self.files if
                                 (f.name == 'brand_variations.tsv')&
                                 (f.parent.name == 'Latest')&
                                 _is_master_files(f.parent.parent.name)&
                                 ('Revised_Panelist_Files' not in f.parts)
                                 ]


        self.files_retailers = [f for f in self.files if
                                (f.name == 'retailers.tsv')&
                                (f.parent.name == 'Latest')&
                                _is_master_files(f.parent.parent.name) &
                                ('Revised_Panelist_Files' not in f.parts)
                                ]



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


        self.df_products = pd.DataFrame()
        self.df_variations = pd.DataFrame()
        self.df_retailers = pd.DataFrame()

        self.df_panelists = []
        self.df_trips = []
        self.df_purchases = []

        self.df_extra = pd.DataFrame()

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
                          f"trips ({year})")

        # Get unique UPCs from products to filter purchases (if products were read)
        has_products = (isinstance(self.df_products, pa.Table) and self.df_products.num_rows > 0) or \
                       (isinstance(self.df_products, pd.DataFrame) and not self.df_products.empty)

        trip_filter_purchases = pads.field('trip_code_uc').isin(df_trips['trip_code_uc'].to_numpy())

        if has_products:
            if isinstance(self.df_products, pa.Table):
                unique_upcs = pc.unique(self.df_products['upc']).to_pylist()
            else:
                unique_upcs = self.df_products['upc'].unique().tolist()
            purchase_filter = trip_filter_purchases & pads.field('upc').isin(unique_upcs)
        else:
            purchase_filter = trip_filter_purchases

        ds_purchases = pads.dataset(_read_csv(self, f_purchases,
                    parse_options = parse_opt,
                    convert_options = conv_opt))\
                    .to_table(filter = purchase_filter)
        _validate_columns(ds_purchases.column_names, EXPECTED_PURCHASE_COLS,
                          f"purchases ({year})")

        df_purchases = ds_purchases.append_column('panel_year', pa.array([year]*ds_purchases.num_rows,pa.int16()))

        # Going through numpy and pandas map cannot be fastest solution here
        if add_household:
            df_purchases=df_purchases.join(df_trips.select(['trip_code_uc','household_code']), keys=['trip_code_uc'])

        # add to the list
        self.df_trips.append(df_trips)
        self.df_purchases.append(df_purchases)
        self.df_panelists.append(df_panelists)

        # pd.concat([self.df_panelists, df_panelists.copy()], ignore_index = True)

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

        for year in self.all_years:
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
        Function: writes pandas dataframes to parquets
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
            # Convert pandas to Arrow if needed
            if isinstance(df, pd.DataFrame):
                if df.empty:
                    return
                df = pa.Table.from_pandas(df, preserve_index=False)

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

        Must have already run the read_annual() function so that df_panelists
        is not an empty dataframe
        """

        self.files_revised = [f for f in self.files if
                              f.parent.parent.parent.name == 'Revised_Panelist_Files'
                              ]

        self.files_panelist_revised = [f for f in self.files_revised if
                                       'panelists' in f.name and
                                       int(f.parent.parent.name)
                                       in self.all_years]

        dict_files_panelist_revised = {get_year(f):
                                       f for f in self.files_panelist_revised
                                       }

        # Convert Arrow tables to pandas for .update() compatibility
        # then convert back at the end
        if isinstance(self.df_panelists, pa.Table):
            self.df_panelists = self.df_panelists.to_pandas()
        if isinstance(self.df_products, pa.Table):
            self.df_products = self.df_products.to_pandas()
        if isinstance(self.df_variations, pa.Table):
            self.df_variations = self.df_variations.to_pandas()
        if isinstance(self.df_retailers, pa.Table):
            self.df_retailers = self.df_retailers.to_pandas()

        for year in self.all_years:
            df_panelist_rev = pd.read_csv(dict_files_panelist_revised[year],
                                          delimiter = '\t')
            self.df_panelists.update(df_panelist_rev)

        # update the other files (if they are empty, they will stay empty)
        self.file_product_revised = [f for f in self.files_revised if
                                     'products' in f.name]
        df_products_rev = pd.read_csv(self.file_product_revised[0],
                                      delimiter = '\t')
        self.df_products.update(df_products_rev)

        # variations
        self.file_variations_revised = [f for f in self.files_revised if
                                        'brand_variations' in f.name]
        df_variations_rev = pd.read_csv(self.file_variations_revised[0],
                                        delimiter = '\t',
                                        engine = 'python',
                                        encoding = 'utf',
                                        quoting=3)
        self.df_variations.update(df_variations_rev)

        # retailers
        self.file_retailers_revised = [f for f in self.files_revised if
                                     'retailers' in f.name]
        df_retailers_rev = pd.read_csv(self.file_retailers_revised[0],
                                       delimiter = '\t',
                                       engine = 'python',
                                       encoding = 'utf',
                                       quoting=3)
        self.df_retailers.update(df_retailers_rev)

        # Convert back to Arrow tables
        self.df_panelists = pa.Table.from_pandas(self.df_panelists, preserve_index=False)
        self.df_products = pa.Table.from_pandas(self.df_products, preserve_index=False)
        self.df_variations = pa.Table.from_pandas(self.df_variations, preserve_index=False)
        self.df_retailers = pa.Table.from_pandas(self.df_retailers, preserve_index=False)

        return

    # Look through open issues
    # Currently somehwat ad-hoc: can fix the Flavor Code + Male Birth Month
    # Will have to udpate as issues close and open

    def process_open_issues(self):
        """
        Function: addresses two of the current (as of 10/01/2021) open issues
        in Nielsen Panel data
        Issue 1: Flavor Code in 2010 missing
        Issue 2: Male Head Birth Month incorrect
        See documentation within Panel files for a description of the issues

        Affected files: df_extra, df_panelists
        """

        self.files_issues = [f for f in self.files if
                            'OpenIssues_SupplementFiles' in f.parts]

        self.open_issues = set([f.parent.name for f in self.files_issues])

        print('Current Open Issues:', self.open_issues)

        # Convert Arrow tables to pandas for update/merge compatibility
        if isinstance(self.df_extra, pa.Table):
            self.df_extra = self.df_extra.to_pandas()
        if isinstance(self.df_panelists, pa.Table):
            self.df_panelists = self.df_panelists.to_pandas()

        # First, Address the ExtraAttributesFlavorCode
        if 'ExtraAttributes_FlavorCode' in self.open_issues:
            self.flavor_csv = [f for f in self.files_issues if
                               f.name == 'Latest_Flavor_2010.csv']

            df_flavor = pd.read_csv(self.flavor_csv[0], delimiter = '\t')
            self.df_extra.update(df_flavor)

        # Then, Address the Panelist Birth Years
        if 'Panelist_maleHeadBirth_femaleHeadBirth' in self.open_issues:
            files_birth = [f for f in self.files_issues
                           if f.parent.name ==
                           'Panelist_maleHeadBirth_femaleHeadBirth']
            dict_files_birth = {2000+int(f.name[6:8]): f for f in files_birth}
            dict_files_birth = {k: dict_files_birth[k]
                                for k in dict_files_birth.keys()
                                if k in self.all_years }

            for f in dict_files_birth:
                f_ann = pd.read_csv(dict_files_birth[f],
                                    delimiter = '\t')
                f_ann.columns = ['household_code',
                                 'panel_year',
                                 'Male_Head_Birth',
                                 'Female_Head_Birth'
                                 ]
                f_ann['Female_Head_Birth'] = f_ann['Female_Head_Birth'].replace('-', np.nan)
                f_ann['Male_Head_Birth'] = f_ann['Male_Head_Birth'].replace('-', np.nan)

                f_ann['Male_Head_Birth'] = (f_ann['Male_Head_Birth'].str[:4].fillna(-1)).astype(int)
                f_ann['Female_Head_Birth'] = (f_ann['Female_Head_Birth'].str[:4].fillna(-1)).astype(int)

                self.df_panelists = self.df_panelists.merge(
                    f_ann, on = ['panel_year', 'household_code'],
                    suffixes = ('', '_revised'),
                    how = 'left')

        # Convert back to Arrow tables
        self.df_extra = pa.Table.from_pandas(self.df_extra, preserve_index=False)
        self.df_panelists = pa.Table.from_pandas(self.df_panelists, preserve_index=False)

