#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Oct  1 11:37:46 2021

@author: Chitra Marti

Example for how to read in Nielsen Retail Scanner and Panel data

Input dir_retail and dir_scanner on your own, using your own machine!

"""
# %% Import Libraries
# for the locations and names of file types
import pathlib as path

# for code
import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

# %% Import NielsenReader functions

# Make sure your current directory contains the NielsenReader file
from NielsenReader import RetailReader, PanelReader

# %% Offer an Example: Full Scanner Files
# Important: Replace dir_retail, dir_panel with your own file locations!

dir_retail = path.Path('/Volumes/Backup Plus/Scanner_Extracts/SoftDrinkNoCarb/')
dir_panel = path.Path('/Volumes/Backup Plus/Panel/')

KEEP_GROUPS = [1508]
KEEP_MODULES = [1481, 1482, 1483]
DROP_YEARS =[] #2009, 2011, 2012]
KEEP_YEARS = range(2004, 2013)
KEEP_STATES = ['RI']
KEEP_CHANNELS = ['F']

# %% Intialize a  Retail Reader object
RR = RetailReader(dir_retail)

# Test out all the potential filters:
RR.filter_years(drop = DROP_YEARS, keep = KEEP_YEARS)
# Note: the subsetting is not reversible without re-initializing
# so the line below will result in no years left
# RR.filter_years(keep = [2019])

# I keep only non-carbonated soda drink mixes (a small-ish group)
# and among those, I keep Cocktail Products (not bottled waters or syrups)
RR.filter_sales(keep_groups = KEEP_GROUPS,
                keep_modules = KEEP_MODULES)

# %% Read in all the types of files
# %% First, Product Information & Characteristics
RR.read_products(keep_groups = KEEP_GROUPS, keep_modules = KEEP_MODULES)
print(RR.df_products.head())

# %% Next, Extra Product Characteristics
RR.read_extra()
print(RR.df_extra.head())

# %% Next, RMS Versions, which you can combine later with sales or products
# to get detailed UPC information separated by versions
RR.read_rms()
print(RR.df_rms.head())

# %% Next, read in stores information
# Filter to just general grocery stores
# and then just in RI for size purposes
RR.read_stores()

# note the filtering comes after the reading in the case of stores
# unlike for sales
RR.filter_stores(keep_channels = KEEP_CHANNELS, keep_states = KEEP_STATES)


print(RR.df_stores.head())

# %% Finally, read in sales information (takes a long time!)
RR.read_sales()
print(RR.df_sales.sample(5))

# %% Finally, write the data using the pyarrow situation
dir_write = path.Path(path.Path.cwd() / 'outfiles')
RR.write_data(dir_write = dir_write,
              stub = 'test',
              as_table = True,
              separator='panel_year')


# %% Demonstrate how to re-read in the pyarrow situation
# we have "separated" the files by year -- what does that mean?
pq_sales = pq.ParquetFile(dir_write / 'test_sales.parquet')
print('Metadata', pq_sales.metadata, '\n')
print('Schema', pq_sales.schema, '\n')

# corresponds to the number of years we saved, which in this example is 2
print('Number of Row Groups:', pq_sales.num_row_groups, '\n')
# note the row groups don't have identities
# go back and inspect the filter years command -- will be in that order
# for example, here the 0th group is the 2018 group

# read in the first row group as a table
tab_sales0 = pq_sales.read_row_group(0)
pd_sales0 = tab_sales0.to_pandas()
print(pd_sales0.head(10))

# Thus concludes the example of how to set up RetailReader and read in tables


# %% Turn to PanelReader: to read in the Nielsen Panel data
# %% Initialize Panel Reader Object
PR = PanelReader(dir_read = dir_panel)

# %% Demonstrating how to Filter Years
PR.filter_years(drop = DROP_YEARS, keep = KEEP_YEARS)

# %% Read in broad Retailer information
PR.read_retailers()

# %% Read in Products: should be EXACTLY as RR.read_products above!
PR.read_products(keep_groups = KEEP_GROUPS, keep_modules = KEEP_MODULES)

# Print a confirmation of that fact
print((PR.df_products == RR.df_products).max())
# %% Read in Extra: should be EXACTLY as RR.read_extra above!
PR.read_extra()
# some small diferences: UPC, dosage_code apparently!
# not checking here because this depends on the years chosen
# extra characteristics not labeled with a year will not be included
print((PR.df_extra == RR.df_extra).max())

# %% Finally, read the annual files
# this takes forever, so I limit to Rhode Island
PR.read_annual(keep_states = KEEP_STATES)

# %% Before we are truly done: address revised panelist files and open issues
PR.read_revised_panelists()

# %%
PR.process_open_issues()

# %% Write the data: here, I will demonstrate how to write then read
# without using the pyarrow technique
# much easier!
PR.write_data(dir_write = dir_write,stub = 'test')

# %% Example: reading in a single file
f_purchases = dir_write / 'test_purchases.parquet'
df_purchases = pd.read_parquet(f_purchases)
print(df_purchases.head(10))


