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
import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.dataset as pads
import pyarrow.parquet as pq
from pyarrow import csv # Sometimes does not load properly, load separately

import pathlib as path


import time

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
              'feature': pa.uint8(),
              'display': pa.uint8(),
              'price':pa.float64(),
              'panel_year': pa.uint64(),
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
              }


dict_column_map = {'Household_Cd':'household_code',
                   'Panel_Year':'panel_year'}

# function to get all files within a folder
# NOTE: might be common to both
def get_files(self):
    """
    Input: RetailReader or PanelReader object        
    Get all Files for a PanelReader or RetailReader object
    """
    files = [i for i in self.dir_read.glob('**/*.*sv') if '._' not in i.stem]
    if len(files) == 0:
        str_err= ('Found no files! Check folder name ' +
        'and make sure folder is unzipped.')
        raise Exception(str_err)
    return files
    # NOTE: CC had the condition # if '._' in i.stem
    # i will remove that conditioning for now



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
                 keep_modules = None, drop_modules = None):
    """
    Arguments: 
        Required: RetailReader or PanelReader object
        Optional: keep_groups, drop_groups, keep_modules, drop_modules
        Each takes a list of group codes or module codes

    Select the Product file and read it in
    Common to both the Retail Reader and Panel Reader files
    Many Filter Options:
    upc_list: a list of integer UPCs to select, ignores versioning by Nielsen
    keep_groups, drop_groups: selects or drops product group codes
    keep_modules, drop_modules: selects or drops product module codes
    """
    if self.files_product:
        self.file_products = self.files_product[0]
    else:
        str_err = ('Could not find a valid products.tsv under ',
                   'Master_Files/Latest. Check folder name ',
                   'and make sure folder is unzipped.')
        raise Exception(str_err)


    read_opt = csv.ReadOptions(encoding='latin')
    parse_opt = csv.ParseOptions(delimiter = '\t')
    conv_opt = csv.ConvertOptions(column_types = dict_types)
    df_products = csv.read_csv(self.file_products,
                           read_options = read_opt,
                           parse_options = parse_opt,
                           convert_options = conv_opt).to_pandas()
    if keep_groups:
        mask_kg = df_products['product_group_code'].isin(keep_groups)
        df_products = df_products[mask_kg]
    if drop_groups:
        mask_dg = ~df_products['product_group_code'].isin(drop_groups)
        df_products = df_products[mask_dg]
    if keep_modules:
        mask_km = df_products['product_module_code'].isin(keep_modules)
        df_products = df_products[mask_km]
    if drop_modules:
        mask_dm = ~df_products['product_module_code'].isin(drop_modules)
        df_products = df_products[mask_dm]


    df_products['size1_units'] = df_products['size1_units'].astype('category')
    df_products['product_module_descr'] = df_products['product_module_descr'].astype('category')
    df_products['product_group_code'] = df_products['product_group_code'].astype('category')

    if upc_list: # if you want to trim more explicitly
        df_products = df_products[df_products['upc'].isin(upc_list)]


    df_products = df_products.sort_values(['upc']).reset_index(drop=True)
    self.df_products = df_products.copy()

    if self.verbose == True:
        print('Successfully Read in Products with Shape ', df_products.shape)

    del(df_products)
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
    # read in the extra files


    ## just do it for all years by default
    # but given the overwriting aspect, maybe you want a specific year
    # i won't judge
    if years == None:
        years = self.all_years

    files_extra_in = [f for f in self.files_extra
                      if get_year(f) in years]

    def aux_read_extra_year(filename):
        conv_opt = csv.ConvertOptions(column_types = dict_types)
        parse_opt = csv.ParseOptions(delimiter='\t')

        pa_ex_y = pads.dataset(csv.read_csv(filename,
                                            parse_options = parse_opt,
                                            convert_options = conv_opt))

        return pa_ex_y.to_table()

    df_extra = pa.concat_tables([aux_read_extra_year(f)
                                 for f in files_extra_in]).to_pandas()

    if upc_list:
        df_extra = df_extra[df_extra['upc'].isin(upc_list)]

    df_extra = df_extra.sort_values(['upc', 'panel_year']).reset_index(drop=True)

    self.df_extra = df_extra.copy()
    # not going to edit the missings or anything of the sort
    if self.verbose == True:
        print('Successfully Read in Extra Files with Shape', df_extra.shape)

    # sort by UPC
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
                              (f.parent.parent.name == 'Master_Files')]

        # Collect the Annual Files NOTE: currently unused
        self.files_annual = [f for f in self.files
                             if 'Annual_Files' in f.parts]


        # Collect the Movement Files, i.e. the store-weekly sales files
        self.files_sales = [f for f in self.files
                            if 'Movement_Files' in f.parts]
        if not self.files_sales:
            str_err = 'Could not find Movement Files!'
            raise Exception(str_err)

        # Collect groups, modules, and years represented in the sales files
        # throws errors if the files were renamed from the original structure
        # NOTE these will be automatically sorted, it seems
        try:
            self.all_groups = set(self.get_group(f) for f in self.files_sales)
        except:
            str_err = ('Could not get Group Code from Movement Files. ',
                       'Use original Nielsen naming conventions')
            raise Exception(str_err)

        try:
            self.all_modules = set(self.get_module(f) for f in self.files_sales)
        except:
            str_err = ('Could not get Module Code from Movement Files. ',
                       'Use original Nielsen naming conventions')
            raise Exception(str_err)
        
        try:
            self.all_years = set(get_year(f) for f in self.files_sales)
        except:
            str_err = ('Could not get Year from Movement Files. ',
                       'Use original Nielsen naming conventions')
            raise Exception(str_err)


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
        self.df_rms = pd.DataFrame()
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
        # requires concatenating all the tables left in dict_rms
        self.df_rms = pa.concat_tables([csv.read_csv(f,
                                                     parse_options = parse_opt,
                                                     convert_options = conv_opt
                                                     )
                                        for f in self.dict_rms.values()]
                                       ).to_pandas()
        if self.verbose == True:
            print('Successfully Read in the RMS Files')
        return

    def read_products(self, upc_list=None,
                     keep_groups = None, drop_groups = None,
                     keep_modules = None, drop_modules = None):
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
                     keep_modules=keep_modules, drop_modules=drop_modules)
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
        df_stores = pa.concat_tables([csv.read_csv(f,
                                                   parse_options = parse_opt,
                                                   convert_options = conv_opt
                                                   )
                                      for f in self.dict_stores.values()]
                                     ).to_pandas().rename(
                                         columns = {'year':'panel_year'})

        # fill blanks with zeroes
        df_stores = df_stores.fillna(0)
        self.df_stores = df_stores.copy()

        del(df_stores)
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

        df_stores = self.df_stores.copy()

        if keep_dmas:
            mask_kd = df_stores['dma_code'].isin(keep_dmas)
            df_stores = df_stores[mask_kd]
        if drop_dmas:
            mask_dd = df_stores['dma_code'].isin(drop_dmas)
            df_stores = df_stores[~mask_dd]

        if keep_channels:
            mask_kc = df_stores['channel_code'].isin(keep_channels)
            df_stores = df_stores[mask_kc]
        if drop_channels:
            mask_dc = df_stores['channel_code'].isin(drop_channels)
            df_stores = df_stores[~mask_dc]

        if keep_states:
            mask_ks = df_stores['fips_state_descr'].isin(keep_states)
            df_stores = df_stores[mask_ks]
        if drop_states:
            mask_ds = df_stores['fips_state_descr'].isin(drop_states)
            df_stores = df_stores[~mask_ds]

        self.df_stores = df_stores.copy()
        if self.verbose == True:
            print('Final Store Count: ', len(self.df_stores))
        del(df_stores)
        return

    # Now, turn our attention to the Movement Files, i.e. the Sales
    # you should have already filtered the years that you want
    # NOTE: read only those sales corresponding to the filtered stores
    # ask: do you want to include the promotional columns?

    def read_sales(self, incl_promo = True):
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


        # select columns
        my_cols = ['store_code_uc', 'upc', 'week_end',
                   'units', 'prmult', 'price']
        if incl_promo == True:
            my_cols = my_cols + ['feature', 'display']

        # have to read one module-year at a time
        # as a pandas table, which we will later concatenate
        def aux_read_mod_year(filename, list_stores = None, incl_promo = True):

            parse_opt = csv.ParseOptions(delimiter = '\t')
            conv_opt = csv.ConvertOptions(column_types = dict_types,
                                          include_columns = my_cols)
            # is a dataset object that can be turned into a table
            # but we can also filter immediately if we like
            pa_my = pads.dataset(csv.read_csv(filename,
                                              parse_options = parse_opt,
                                              convert_options=conv_opt))

            if list_stores is None:
                return pa_my.to_table()

            return pa_my.to_table(filter =pads.field('store_code_uc').isin(list_stores))

        # read all the modules (and groups) for one year
        def aux_read_year(year, incl_promo = True):
            if self.verbose == True:
                tick()
                print('Processing Year', year)

            # get the list of stores that were present in the year of choice
            mask_y = self.df_stores['panel_year'] == year
            list_stores = self.df_stores.loc[mask_y, 'store_code_uc'].unique()

            pa_y = pa.concat_tables([aux_read_mod_year(f,
                                                       list_stores,
                                                       incl_promo = incl_promo)
                                     for f in self.dict_sales[year]
                                     ])

            # still a table object, not a pandas dataframe
            # since we will be concatenating years together, presumably?
            if self.verbose == True:
                print('Done with Year', year)
                tock()
            return pa_y

        #print(aux_read_year(2015, incl_promo))

        df_sales = pa.concat_tables([
            aux_read_year(y, incl_promo)
            for y in self.dict_sales.keys()]).to_pandas()
        #print(df_sales.head(10))

#        df_sales['panel_year'] = np.floor(df_sales['week_end']/10000).astype(int)

        # after concatenation, clean up the full data frame
        def aux_clean(df):
            # first, fix prices to be a per-unit price
            # or at least create a new column
            df['unit_price'] = df['price'] / df['prmult']
            # dictionary of the unique dates we consider
            # original format is 20050731
            # NOTE different from the more formal year function
            df['panel_year'] = np.floor(df['week_end']/10000).astype(int)

            dict_date = {x: pd.to_datetime(x, format = '%Y%m%d')
                         for x in df['week_end'].unique()
                         }
            df['week_end'] = df['week_end'].map(dict_date)

            if 'feature' in df.columns:
                df['feature'].fillna(-1, inplace=True)
                df['display'].fillna(-1, inplace=True)

            # NOTE skipping the recasting of data types

            return df

        df_sales = aux_clean(df_sales)

        if len(self.df_rms) ==0:
            self.read_rms()

        # NOTE: ORIGINAL CODE MERGES THIS WITH df_stores
        # but I am skipping that here
        # only going to merge with RMS
        # which requires getting panel_year
        #self.df_sales = df_sales.copy()

        # this line might take a long time

        # merge in the versions
        cols_rs = ['upc', 'panel_year']
        # cols_ss = ['store_code_uc', 'panel_year']
        self.df_sales = pd.merge(df_sales, self.df_rms, on = cols_rs)


        # # finally, drop the stores that have no sales
        final_stores = self.df_sales['store_code_uc'].unique()
        mask_ss = self.df_stores['store_code_uc'].isin(final_stores)
        self.df_stores = self.df_stores[mask_ss].copy()



    def write_data(self, dir_write = path.Path.cwd(), stub = 'out',
                   compr = 'brotli', as_table = False,
                   separator = 'panel_year'):

        """
        Function: writes pandas dataframes to parquets
        Arguments: optional: dir_write: Path
        stub: default 'out'
        compression type: detault 'nbrotli'
        as_table: if you want to use the pyarrow separated row-tables,
        then set to True
        separator: column on which to separate rows for the pyarrow Table feature

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

        Always saves as parquets with compression of your choice
        (default: brotli)
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

        def aux_write_direct(df, filename, compr = 'brotli'):
            if df.empty:
                return
            df.to_parquet(filename, compression = compr)
            if self.verbose == True:
                print('Wrote as direct parquet to', filename)
            return

        if as_table == False:
            aux_write_direct(self.df_stores, f_stores)
            aux_write_direct(self.df_sales, f_sales)
            aux_write_direct(self.df_products, f_products)
            aux_write_direct(self.df_extra, f_extra)

            return # end the job right here


        def aux_write_separated(df, filename, separator = 'dma_code',
                            compr = 'brotli'):
            if df.empty:
                return
            if not separator in df:
                #print(filename)
                if self.verbose == True:
                    print('Separator not found in DataFrame. Writing directly')
                aux_write_direct(df, filename)
                return

            # get a list of separators
            seps = df[separator].unique()
            # separate the DataFrames
            dfs_sep = [df[df[separator] == sep] for sep in seps]

            # get the writer object from the first spearated dataframe
            table0 = pa.Table.from_pandas(dfs_sep[0])
            writer = pq.ParquetWriter(filename, table0.schema,
                                      compression = compr)
            for df in dfs_sep:
                table = pa.Table.from_pandas(df)
                writer.write_table(table)
            writer.close()

            if self.verbose == True:
                print('Wrote Data to {d} with stub {s} and row groups {sep}'.format(d=dir_write, s=stub, sep=separator))

            return

        # can separate out the files and write them as pyarrow tables
        # create separate dataframes and avoid overwhelming your system, i guess
        aux_write_separated(self.df_stores, f_stores, separator)
        aux_write_separated(self.df_sales, f_sales, separator)
        aux_write_separated(self.df_products, f_products, separator)
        aux_write_separated(self.df_extra, f_extra, separator)


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
                             if f.parts[-3] == 'Master_Files']

        self.files_annual = [f for f in self.files
                             if f.parts[-2] == 'Annual_Files']

        self.files_product = [f for f in self.files if
                              (f.name == 'products.tsv')&
                              (f.parent.name == 'Latest')&
                              (f.parent.parent.name == 'Master_Files')&
                              ('Revised_Panelist_Files' not in f.parts)
                              ]

        self.files_variations = [f for f in self.files if
                                 (f.name == 'brand_variations.tsv')&
                                 (f.parent.name == 'Latest')&
                                 (f.parent.parent.name == 'Master_Files')&
                                 ('Revised_Panelist_Files' not in f.parts)
                                 ]


        self.files_retailers = [f for f in self.files if
                                (f.name == 'retailers.tsv')&
                                (f.parent.name == 'Latest')&
                                (f.parent.parent.name == 'Master_Files') &
                                ('Revised_Panelist_Files' not in f.parts)
                                ]



        if not self.files_annual:
            str_err = ('Could not find Annual Files ',
                       '(panelists, purchases, trips)')
            raise Exception(str_err)

        try:
            self.all_years = set([get_year(x) for x in self.files_annual])
        except:
            str_err = ('Could not get year from Movement files. ',
                       'Keep original Nielsen file structure.')
            raise Exception(str_err)

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
        self.df_panelists = pd.DataFrame()

        self.df_trips = pd.DataFrame()
        self.df_trips = []
        self.df_purchases = pd.DataFrame()
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
            str_err = ('Could not find a valid retailers.tsv under ',
                       'Master_Files/Latest. Check folder name ',
                       'and make sure folder is unzipped.')
            raise Exception(str_err)
        
        

        df_retailers = csv.read_csv(self.file_retailers,
                               read_options = read_opt,
                               parse_options = parse_opt,
                               convert_options = conv_opt).to_pandas()

        self.df_retailers = df_retailers.copy()

        if self.verbose == True:
            print('Successfully Read in Retailers with Shape', df_retailers.shape)

        return

    def read_products(self, upc_list=None,
                     keep_groups = None, drop_groups = None,
                     keep_modules = None, drop_modules = None):
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
                     keep_modules=keep_modules, drop_modules=drop_modules)

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
            str_err = ('Could not find a valid brand_variations.tsv under ',
                       'Master_Files/Latest. Check folder name ',
                       'and make sure folder is unzipped.')
            raise Exception(str_err)
        
        
        
        df_variations = csv.read_csv(self.file_variations,
                               read_options = read_opt,
                               parse_options = parse_opt,
                               convert_options = conv_opt).to_pandas()
        
        self.df_variations = df_variations.copy()

        print('Successfully Read in Brand Variations with Shape', df_variations.shape)

        return


    def read_year(self, year, keep_dmas = None, drop_dmas = None,
        keep_states = None, drop_states = None, add_household=False):
        """
        Function: reads a single year of panel data (an auxiliary method)
        Arguments: required: year
        optional: keep_states, drop_states: list of states in two-letter format
        keep_dmas, drop_dmas: list of DMA codes

        See Nielsen documentation for a full description of these variables.        

        """
        try:
            f_trips = self.dict_trips[year][0] # should only be one per year
        except:
            str_err = ('Could not find trip file for year', year)
            raise Exception(str_err)

        try:
            f_purchases = self.dict_purchases[year][0]
        except:
            str_err = ('Could not find purchases file for year', year)
            raise Exception(str_err)
        try:
            f_panelists = self.dict_panelists[year][0]
        except:
            str_err = ('Could not find panelists file for year', year)
            raise Exception(str_err)

        parse_opt = csv.ParseOptions(delimiter = '\t')
        conv_opt = csv.ConvertOptions(column_types = dict_types,
                                      auto_dict_encode = True,
                                      auto_dict_max_cardinality = 1024)
        ds_panelists = pads.dataset(csv.read_csv(f_panelists,
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

        # Get the Panelist Table Filtered and as a DataFrame
        df_panelists = ds_panelists.to_table(filter = panelist_filter).to_pandas()
        df_panelists = df_panelists.rename(columns = dict_column_map)

        # Get a list of Unique HH
        unique_hh = df_panelists['household_code'].unique()

        df_trips = pads.dataset(csv.read_csv(f_trips,
                    parse_options = parse_opt,
                    convert_options = conv_opt)
                    ).to_table(filter = pads.field('household_code').isin(unique_hh))

        purchase_filter = (pads.field('trip_code_uc').isin(df_trips['trip_code_uc'].to_numpy()))

        ds_purchases = pads.dataset(csv.read_csv(f_purchases,
                    parse_options = parse_opt,
                    convert_options = conv_opt))

        df_purchases = ds_purchases\
            .to_table(filter = purchase_filter)\
            .append_column('panel_year', pa.array([year]*ds_purchases.count_rows(),pa.int16()))

        # Going through numpy and pandas map cannot be fastest solution here
        if add_household:
            d=dict(zip(df_trips['trip_code_uc'].to_numpy(),df_trips['household_code'].to_numpy()))
            df_purchases = df_purchases.append_column(
                'household_code',
                pa.array(df_purchases['trip_code_uc'].to_pandas().map(d), pa.uint32())
                )

        self.df_trips.append(df_trips)
        self.df_purchases.append(df_purchases)

        #self.df_trips = pd.concat([self.df_trips, df_trips.copy()], ignore_index = True)
        #self.df_purchases = pd.concat([self.df_purchases, df_purchases.copy()], ignore_index = True)

        self.df_panelists = pd.concat([self.df_panelists, df_panelists.copy()], ignore_index = True)

        return
        # need to have already read in products?
        # but our version of products has no differences
        #unique_upc = self.df_products['upc'].unique()


    def read_annual(self, keep_states = None, drop_states = None,
                    keep_dmas = None, drop_dmas = None, add_household=False):
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
                           add_household= add_household)
            tock()
        
        print('Concatenating Tables...')
        #self.df_trips = pa.concat_tables(self.df_trips, promote=True).to_pandas(self_destruct=True, split_blocks=True)
        #self.df_purchases = pa.concat_tables(self.df_purchases, promote=True).to_pandas(self_destruct=True, split_blocks=True)

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
    
        def aux_write_direct(df, filename, compr = 'brotli'):
            #print(filename)
            if df.empty:
                return
            df.to_parquet(filename, compression = compr)
            if self.verbose == True:
                print('Wrote as direct parquet to', filename)
            return

        print(dir_write)

        if as_table == False:
            aux_write_direct(self.df_products, f_products)
            aux_write_direct(self.df_variations, f_variations)
            aux_write_direct(self.df_retailers, f_retailers)
            aux_write_direct(self.df_trips, f_trips)
            aux_write_direct(self.df_panelists, f_panelists)
            aux_write_direct(self.df_purchases, f_purchases)
            aux_write_direct(self.df_extra, f_extra)
    
            return # end the job right here
    
    
        def aux_write_separated(df, filename, separator = 'panel_year',
                            compr = 'brotli'):
            if df.empty:
                return
            if not separator in df:
                #print(filename)
                if self.verbose == True:
                    print('Separator not found in DataFrame. Writing directly')
                aux_write_direct(df, filename)
                return
    
            # get a list of separators
            seps = df[separator].unique()
            # separate the DataFrames
            dfs_sep = [df[df[separator] == sep] for sep in seps]
    
            # get the writer object from the first spearated dataframe
            table0 = pa.Table.from_pandas(dfs_sep[0])
            writer = pq.ParquetWriter(filename, table0.schema,
                                      compression = compr)
            for df in dfs_sep:
                table = pa.Table.from_pandas(df)
                writer.write_table(table)
            writer.close()
    
            if self.verbose == True:
                print('Wrote Data to {d} with stub {s} and row groups {sep}'.format(d=dir_write/filename, s=stub, sep=separator))
    
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
    # Updates the usual Panel files with the
    def read_revised_panelists(self):

        """
        Function: corrects the panelist files using errata from the Panel files
        Every year, Nielsen has some panelists whose data they revise

        Must have already run the read_annual() function so that df_panelists
        is not an empty dataframe
        """

        # Note that the panelists appear to have various issues
        # so you should use the revised panelists if needed

        # must have already run the annual files

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

        for year in self.all_years:
            df_panelist_rev = pd.read_csv(dict_files_panelist_revised[year],
                                          delimiter = '\t')
            # update with the new revised files. does anything change?
            # yes, can confirm something changes, great
            self.df_panelists.update(df_panelist_rev)

        # update the other files (if they are empty, they will stay empty
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
        # runs into issues w/o these three lines
        self.df_variations.update(df_variations_rev)

        # retailers
        self.file_retailers_revised = [f for f in self.files_revised if
                                     'retailers' in f.name]
        df_retailers_rev = pd.read_csv(self.file_retailers_revised[0],
                                       delimiter = '\t',
                                       engine = 'python',
                                       encoding = 'utf',
                                       quoting=3)

        self.df_retailers.update(self.file_retailers_revised)

        return

    # Look through open issues
    # Currently somehwat ad-hoc: can fix the Flavor Code + Male Birth Month
    # Will have to udpate as issues close and open

    def process_open_issues(self):
        """
        Function: addresses two of the curren (as of 10/01/2021) open issues
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


        # First, Address the ExtraAttributesFlavorCode
        if 'ExtraAttributes_FlavorCode' in self.open_issues:
            self.flavor_csv = [f for f in self.files_issues if
                               f.name == 'Latest_Flavor_2010.csv']
    
            df_flavor = pd.read_csv(self.flavor_csv[0], delimiter = '\t')
    
            # update with df_flavor: updates in place
            self.df_extra.update(df_flavor)

        # Then, Address the Panelist Birth Years
        if 'Panelist_maleHeadBirth_femaleHeadBirth' in self.open_issues:
            files_birth = [f for f in self.files_issues
                           if f.parent.name ==
                           'Panelist_maleHeadBirth_femaleHeadBirth']
            # make a dictionary of these
            dict_files_birth = {2000+int(f.name[6:8]): f for f in files_birth}
            # select just the relevant years
            dict_files_birth = {k: dict_files_birth[k]
                                for k in dict_files_birth.keys()
                                if k in self.all_years }

            # then update the files

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

                # remove the month for male and female births
                f_ann['Male_Head_Birth'] = (f_ann['Male_Head_Birth'].str[:4].fillna(-1)).astype(int)
                f_ann['Female_Head_Birth'] = (f_ann['Female_Head_Birth'].str[:4].fillna(-1)).astype(int)


                self.df_panelists = self.df_panelists.merge(
                    f_ann, on = ['panel_year', 'household_code'],
                    suffixes = ('', '_revised'),
                    how = 'left')

