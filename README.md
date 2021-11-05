# NielsenReader
NielsenReader defines the classes **RetailReader** and **PanelReader** to facilitate easy processing of the Kilts Center's Nielsen IQ Data. **RetailReader** processes Retail Scanner Data and **PanelReader** processes Consumer Panel Data

Information about the data can be found at the [Kilts Center's Website for the Nielsen Dataset][kilts].

Check with your institution to gain access to the data. Once you have gained access, download files as follows:

###### Retail Scanner Data:
1. Construct file extracts using the Kilts File Selection System. Note you must separately gain access to the File Selection System after applying for the data. 
2. The data are available in .tgz files. Data can be downloaded by group, module, and/or year
3. Unzip the .tgz files 

###### Consumer Panel Data
1. Panelist data can be downloaded directly from Globus. The data are small enough for a typical work machine. The data are available in .tgz files
2.  Unzip the .tgz files.

Importantly, make sure all files are unzipped and preserved in the original Nielsen structure before using the methods provided here. (Do not rearrange the directory structure.)

#### To use the Reader Classes
1. Download NielsenReader.py and Example.py
2. Make sure your python installation has the latest pyarrow, among other typical packages
Using pip:
```
pip install pyarrow
pip install --upgrade pyarrow
```
Using pip (requirements.txt):
```
python -m pip install -r requirements.txt
```

Using conda:
```
conda intall pyarrow
conda update pyarrow
```
This version was built on `pyarrow 4.0.1`
3. Locate your Nielsen Retail Scanner and Consumer Panel data separately
4. Open `Example.py`
5. Replace `dir_retail` and `dir_panel` with the locations of your Retail Scanner and Consumer Panel Data respectively
6. Replace the `KEEP_GROUPS`, `KEEP_MODULES`, `DROP_YEARS`, `KEEP_YEARS`, `KEEP_STATES`, and `KEEP_CHANNEL` with your relevant selection 
7. Run `Example.py` to verify the code works. 



[kilts]:<https://www.chicagobooth.edu/research/kilts/datasets/nielsenIQ-nielsen>
## Class Descriptions
# RetailReader

**class** NielsenReader.**RetailReader**(_**dir_read**=path.Path.cwd(), **verbose**=True_)

RetailReader defines the class object used to read in the Nielsen Retail Scanner Data (see above)

###### Parameters:
- **dir_read**(_pathlib Path object, optional_): points to the location of the Retail Scanner Files. Should be named ``nielsen_extract`` or something similar, containing the subfolder `RMS`. Default is the current working directory.
- **verbose**(_bool_): if `True`, prints updates after processing files. Displays size of files processed. Default is True.

###### Objects:
- `df_products` (_pandas DataFrame_): default empty, stores products data after processing
- `df_sales` (_pandas DataFrame_): default empty, stores sales data after processing
- `df_stores` (_pandas DataFrame_): default empty, stores store data after processing
- `df_rms` (_pandas DataFrame_): default empty, stores RMS versions data after processing
- `df_extra` (_pandas DataFrame_): default empty, stores extra product data after processing
- `all_years` (_list_): list of years for which data will be processed
- `files_product` (_pathlib Path object_): stores name of product characteristic file
- `files_rms` (_list of pathlib Path objects_): stores names of annual rms_versions files
- `files_stores` (_list of pathlib Path objects_): stores names of annual stores files
- `files_extra` (_list of pathlib Path objects_): stores names of annual extra files
- `files_sales` (_list of pathlib Path objects_)stores names of annual sales files
- `all_years` (_list_): list of years included in data. Updates with filtering

## Available Functions in the RetailReader Class
Functions also described with docstrings in the `NielsenReader.py` file

**RetailReader**.**filter_years**(_**keep**=None, **drop**=None_): 
Selects years for which to process annual sales files. Used in pre-processing to limit required memory if desired; otherwise later functions will process all available data. Populates `RetailReader.all_years`
- _**keep**(list of integers, optional)_: list of years to keep, e.g. range(2004, 2013). Can only include years that are already present in the data, e.g. specifying `keep=[1999]' will result in an empty set of years
- _**drop**(list of integers, optional)_: list of years to remove, e.g. [2006, 2009, 2013]

**RetailReader**.**filter_sales**(_**keep_groups**=None_, _**drop_groups**=None_, _**keep_modules**=None_, _**drop_modules**=None_): 
Selects product groups (outer category) and product modules (inner category) for which to process annual sales files. Used in pre-processing to limit required memory if desired; otherwise later functions will process all available data.
- _**keep_groups**(list of integers, optional)_: list of product groups to keep, e.g. `keep_groups=[1508, 1048]`
- _**drop_groups**(list of integers, optional)_: list of product groups to exclude, e.g. `drop_groups = [1046]`. Takes precedence if there is any overlap with _keep_groups_
- _**keep_groups**(list of integers, optional)_: list of product modules to keep, e.g. `keep_modules=[1481, 1482]`
- _**drop_groups**(list of integers, optional)_: list of product modules to exclude, e.g. `drop_groups = [1483]`. Takes precedence if there is any overlap with _keep_module_

**RetailReader**.**read_rms**(): 
Populates `RetailReader.df_rms`
Processes the annual RMS versions files, which map reused UPCs to the appropriate version based on year



**RetailReader**.**read_products**(_**upc_list**=None_, _**keep_groups**=None_, _**drop_groups**=None_, _**keep_modules**=None_, _**drop_modules**=None_): 
Populates `RetailReader.df_products`.
Reads in the set of product characteristics, typically located in `Master_Files/Latest/products.tsv`. Optionally elects product groups (outer category) and product modules (inner category) for which to process annual sales files. 

Note that the function does NOT carry over the filtered set of groups and modules from `RetailReader.filter_sales()`. The `RetailReader.read_products()` function is redundant with the `PanelReader.read_products()` function, and therefore allows the user to read in the full set of products and their characteristics even while reading only a subset of sales.
- _**upc_list**(list of integers, optional)_: list of Universal Product Codes to keep, e.g. `upc_list=[002111039080, 009017445929]` (leading zeros not required)
- _**keep_groups**(list of integers, optional)_: list of product groups to keep, e.g. `keep_groups=[1508, 1048]`
- _**drop_groups**(list of integers, optional)_: list of product groups to exclude, e.g. `drop_groups = [1046]`. Takes precedence if there is any overlap with _keep_groups_
- _**keep_groups**(list of integers, optional)_: list of product modules to keep, e.g. `keep_modules=[1481, 1482]`
- _**drop_groups**(list of integers, optional)_: list of product modules to exclude, e.g. `drop_groups = [1483]`. Takes precedence if there is any overlap with _keep_module_

**RetailReader**.**read_extra**(_**years**=None_, _**upc_list**=None_)
Populates `RetailReader.df_extra`
Selects annual Products Extra files for all post-filtering years. Redundant with `PanelReader.read_extra()`
Product group and module filtering are not possible.
Note that UPCs may be repeated for different years. Use RMS versions to select appropriate years. Differences between multiple years for a single UPC may be due not to actual product changes but rather due to Nielsen filling in previously missing data. See Nielsen documentation for an in-depth description

- _**upc_list**(list of integers, optional)_: list of Universal Product Codes to keep, e.g. `upc_list=[002111039080, 009017445929]` (leading zeros not required)
- **years**(list of integers, optional): selects years for which to. Note that previous year-filtering of the RetailReader object will carry over unless a new set of years is specified.


**RetailReader**.**read_stores**():
Populates `RetailReader.df_stores`
Stores files are common to all product groups and modules, so processing will be unaffected by `RetailReader.filter_years` or `RetailReader.filter_sales`


**RetailReader**.**filter_stores**(_**keep_dma**=None_, _**drop_dma**=None_, _**keep_states**=None_, _**drop_states**=None_, _**keep_channel**=None_, _**drop_dma**=None_):
Updates `RetailReader.df_stores`
- _**keep_dmas**(list of integers, optional)_: list of DMAs (Designated Market Areas) to keep, e.g. `keep_dma=[801, 503]`
- _**drop_dmas**(list of integers, optional)_: list of DMAs (Designated Market Areas) to exclude, e.g. `drop_dma=[602]`. Takes precedence if there is any overlap with _keep_dma_.
- _**keep_states**(list of strings, optional)_: list of states to keep, with list in two-character format, e.g. `keep_states=['TX', 'CA']`
- _**drop_states**(list of integers, optional)_: list of states to exclude, with list in two-character format, e.g. `drop_states=['NJ', 'NY']` Takes precedence if there is any overlap with _keep_states_.
- _**keep_channels**(list of characters, optional)_: list of channels (store types) to keep, e.g. `keep_channels=['F', 'G']`. See Nielsen documentation for explanation of channels and list of options. 
- _**drop_channels**(list of characters, optional)_: list of channels (store types) to exclude, e.g. `drop_channels = ['C']`. Takes precedence if there is any overlap with _keep_channels_. See Nielsen documentation for explanation of channels and list of options. 

Note: Must be run AFTER `RetailReader.read_stores()`. Pre-filtering is not possible.


**RetailReader**.**read_sales**(_**incl_promo** = True_)
Populates `RetailReader.df_sales`
Note this function call takes the longest time.
Reads in the weekly, store x upc level sales data, post-filter if any have been applied. Uses pyarrow methods to filter and read the data to minimize memory and time use. May still require large amounts of memory/CPU.

**RetailReader**.**write_data**(_**dir_write** = path.Path.cwd()_, _**stub** = 'out'), _**compr** = 'brotli'_, _**as_table** = False_, _**separator** = 'panel_year'_)
Writes the pandas DataFrames of the RetailReader class to parquet format (see class description abvove).
- _**dir_write**(pathlib Path object, optional)_: folder within which to write the parquets. Defalt is current working directory.
- _**stub**(str)_: initial string to name all files. Files will be named 'stub'_'[file type].parquet', e.g. 'out_stores.parquet'
- _**compr**(str)_: type of compression used for generating parquets. Default is brotli
- _**as_table**(bool)_: whether to write as pyarrow separated row-tables. See `Example.py` for instance of how to write and read row-groups. Requires a _**separator**_ to generate rows for the row-tables. Useful if you seek to preserve space when reading in files by using only one row-group at a time. 
- _**separator**(column name)_: variable on which to separate row-groups when saving as a pyarrow table. Note that [for now] the separator must be common to all files. Default is `panel_year`. If separator is not present in the file, it cannot be saved as a pyarrow table. If you are looking to save just a single file with a specific separator, modify the following snippet:
```
RetailReader.read_{FILE_TYPE}()
RR.write_data(dir_write, separator = {VARIABLE_NAME})
```

**RetailReader.**get_module**(_file_sales=_)**: returns product module (inner category) corresponding to particular sales file. Auxiliary, not commonly used.
- _file_sales(pathlib Path object)_: Retail Scanner sales file, e.g. 1046_2006.tsv


**RetailReader.**get_group**(_file_sales=_)**: returns product group (outer category) corresponding to particular sales file. Auxiliary, not commonly used.
- _file_sales(pathlib Path object)_: Retail Scanner sales file, e.g. 1046_2006.tsv

# PanelReader
**class** NielsenReader.*PanelReader(_**dir_read**=path.Path.cwd(), **verbose**=True_)*
PanelReader defines the class object used to read in the Nielsen Consumer Panel data (see above)

###### Parameters:
- **dir_read**(_pathlib Path object, optional_): points to the location of the Consumer Panel data files. Likely named `Panel` or something similar. Subfolders should be years. Default is the current working directory.
- **verbose**(_bool_): if `True`, prints updates after processing files. Displayes size of files processed. Default is true.

###### Objects:
- `df_products` (_pandas DataFrame_): default empty, stores products data after from Master Files processing 
- `df_variations` (_pandas DataFrame_): default empty, stores brand_variations data from Master Files after processing 
- `df_retailers` (_pandas DataFrame_): default empty, stores retailers data from Master Files after processing
- `df_trips` (_pandas DataFrame_): default empty, stores annual trips data after processing
- `df_panelists` (_pandas DataFrame_): default empty, stores annual panelists data after processing
- `df_purchases` (_pandas DataFrame_): default empty, stores annual purchases after processing
- `df_extra` (_pandas DataFrame_): default empty, stores annual products extra characteristics data after processing
- `files_annual` (_list of pathlib Path objects_)
- `files_product` (_list of pathlib Path objects_): stores name of _unrevised_ products file
- `files_variation` (_list of pathlib Path objects_): stores name of _unrevised_ brand variations file
- `files_retailers` (_list of pathlib Path objects_): stores name of _unrevised_ retailers file
- `files_trips` (_list of pathlib Path objects_): stores names of annual tripes files
- `files_panelists` (_list of pathlib Path objects_): stores names of annual panelists files
- `files_purchases` (_list of pathlib Path objects_): stores names of annual purchases files
- `files_extra` (_list of pathlib Path objects_): stores names of annual products extra files
- `all_years` (_list_): list of years included in data. Updates with filtering

## Available Functions in the RetailReader Class

**PanelReader**.**filter_years**(_**keep**=None_, _**drop**=None_)
Selects years for which to process annual panelist, purchase, trips, and extra files. Used in pre-processing to limit required memory if desired; otherwise later functions will process all available data. Updates `PanelReader.all_years`
- _**keep**(list of integers, optional)_:  list of years to keep, e.g. range(2004, 2013). Can only include years that are already present in the data, e.g. specifying `keep=[1999]` will result in an empty set of years
- drop(list of integers, optional): list of years to remove, e.g. [2006, 2009, 2013]

**PanelReader**.**read_retailers**():
Populates `PanelReader.df_retailers`
Processes the Master retailers file, which list retailer codes and channels.

Note that the file may be later revised following a call to `PanelReader.read_revised_panelists()` or `PanelReader.process_open_issues()`

**PanelReader**.**read_products**(_**upc_list**=None_, _**keep_groups**=None_, _**drop_groups**=None_,  _**keep_modules**=None_,  _**drop_modules**=None_)
Populates PanelReader.df_products, which should be identical to RetailReader.df_products following a call to RetailReader.read_products().

Reads in the set of product characteristics, typically located in `Master_Files/Latest/products.tsv`. Optionally elects product groups (outer category) and product modules (inner category) for which to process annual sales files. 

Note that the file may be later revised following a call to `PanelReader.read_revised_panelists()` or `PanelReader.process_open_issues()`. Such updating is only possible through `PanelReader`; the Retail Scanner files do not contain any revisions.

- _**upc_list**(list of integers, optional)_: list of Universal Product Codes to keep, e.g. `upc_list=[002111039080, 009017445929]` (leading zeros not required)
- _**keep_groups**(list of integers, optional)_: list of product groups to keep, e.g. `keep_groups=[1508, 1048]`
- _**drop_groups**(list of integers, optional)_: list of product groups to exclude, e.g. `drop_groups = [1046]`. Takes precedence if there is any overlap with _keep_groups_
- _**keep_groups**(list of integers, optional)_: list of product modules to keep, e.g. `keep_modules=[1481, 1482]`
- _**drop_groups**(list of integers, optional)_: list of product modules to exclude, e.g. `drop_groups = [1483]`. Takes precedence if there is any overlap with _keep_module_



**PanelReader**.**read_extra**(_**years**=None_, _**upc_list**=None_)
Populates `PanelReader.df_extra`
Selects annual Products Extra files for all post-filtering years. Redundant with RetailReader.read_extra(). Does not have any revisions as of October 2021.
Product group and module filtering are not possible; only UPC and year filtering are available.
Note that UPCs may be repeated for different years. Use RMS versions to select appropriate years. Differences between multiple years for a single UPC may be due not to actual product changes but rather due to Nielsen filling in previously missing data. See Nielsen documentation for an in-depth description.

- **years**(list of integers, optional): selects years for which to. Note that previous year-filtering of the PanelReader object will carry over unless a new set of years is specified
- _**upc_list**(list of integers, optional)_: list of Universal Product Codes to keep, e.g. `upc_list=[002111039080, 009017445929]` (leading zeros not required)



**PanelReader**.**read_variations**()
Populates `PanelReader.df_variations` with brand variations data, typically located in `MasterFiles/Latest/brand_variations.tsv`.
Lists brand codes, descriptions, and any alternative descriptions.

**PanelReader**.**read_year**(**year**, _**keep_dmas**=None_,  _**drop_dmas**=None_, _**keep_states**=None_, _**drop_states**=None_,)
Populates `PanelReader.df_panelists`, `PanelReader.df_purchases`, and `PanelReader.df_trips`
Processes a single year of annual data (panelists, purchases, and trips data). Useful if you seek to only process one year at a time; otherwise use `read_annual` as described below
- **year** (_int_): single year to process
- **keep_dmas** (_list of integers_):  list of DMAs (Designated Market Areas) to keep, e.g. `keep_dma=[801, 503]`
- **drop_dmas** (_list of integers_): list of DMAs (Designated Market Areas) to exclude, e.g. `drop_dma=[602]`. Takes precedence if there is any overlap with *keep_dma*.
- **keep_states** (_list of strings_): list of states to keep, with list in two-character format, e.g. `keep_states=['TX', 'CA']`
- **drop_states** (_list of strings_): list of states to exclude, with list in two-character format, e.g. `drop_states=['NJ', 'NY']` Takes precedence if there is any overlap with *keep_states*.


**PanelReader**.**read_annual**(_**keep_states**=None_, _**drop_states**=None_,  _**keep_dmas**=None_,  _**drop_dmas**=None_)
Processes all years (post-`PanelReader.filter_years()`) with repeated calls to `PanelReader.read_year()`
- **keep_dmas** (_list of integers_):  list of DMAs (Designated Market Areas) to keep, e.g. `keep_dma=[801, 503]`
- **drop_dmas** (_list of integers_): list of DMAs (Designated Market Areas) to exclude, e.g. `drop_dma=[602]`. Takes precedence if there is any overlap with *keep_dma*.
- **keep_states** (_list of strings_): list of states to keep, with list in two-character format, e.g. `keep_states=['TX', 'CA']`
- **drop_states** (_list of strings_): list of states to exclude, with list in two-character format, e.g. `drop_states=['NJ', 'NY']` Takes precedence if there is any overlap with *keep_states*.


**PanelReader**.**write_data**(_**dir_write** = path.Path.cwd()_, _**stub** = 'out'), _**compr** = 'brotli'_, _**as_table** = False_, _**separator** = 'panel_year'_)
Writes the pandas DataFrames of the RetailReader class to parquet format (see class description abvove).
- _**dir_write**(pathlib Path object, optional)_: folder within which to write the parquets. Defalt is current working directory.
- _**stub**(str)_: initial string to name all files. Files will be named 'stub'_'[file type].parquet', e.g. 'out_stores.parquet'
- _**compr**(str)_: type of compression used for generating parquets. Default is brotli
- _**as_table**(bool)_: whether to write as pyarrow separated row-tables. See `Example.py` for instance of how to write and read row-groups. Requires a _**separator**_ to generate rows for the row-tables. Useful if you seek to preserve space when reading in files by using only one row-group at a time. 
- _**separator**(column name)_: variable on which to separate row-groups when saving as a pyarrow table. Note that [for now] the separator must be common to all files. Default is `panel_year`. If separator is not present in the file, it cannot be saved as a pyarrow table. If you are looking to save just a single file with a specific separator, modify the following snippet:
```
RetailReader.read_{FILE_TYPE}()
RR.write_data(dir_write, separator = {VARIABLE_NAME})
```

**PanelReader**.**read_revised_panelists**()
Updates `PanelReader.df_products`, `PanelReader.df_variations`, `PanelReader.df_retailers`, and `PanelReader.df_panelists`
Corrects the Panelist data using errata provided by Nielsen for issues not yet incorporated into the data as of October 2021. Must have already called `PanelReader.read_annual()`, `PanelReader.read_products()`, `PanelReader.read_variations()`, `PanelReader.read_retailers()`


**PanelReader**.**process_open_issues**()
Updates `PanelReader.df_panelists` and `PanelReader.df_extra`
Corrects the product extra and panelist data using errata provided by Nielsen for two specific issues:
- `ExtraAttributes_FlavorCode`: adds missing flavor code and flavor description to 2010 products extra characteristics file
- `Panelist_maleHeadBirth_femaleHeadBirth`: corrects issue with male head of household birth month











