import time
import pyarrow as pa
import pyarrow.dataset as ds
from pyarrow import csv
import pandas as pd

import pathlib
from pathlib import Path

type_dict = {'panel_year': 'uint16', 'retailer_code': 'uint16', 'parent_code': 'uint16',
             'store_code_uc': 'uint32', 'dma_code': 'uint16', 'upc_ver_uc': 'int8',
             'feature': 'int8', 'display': 'int8', 'store_zip3': 'uint16',
             'fips_state_code': 'uint8', 'fips_county_code': 'uint16'}

# Pure functions here
def get_files(my_dir):
    return [i for i in my_dir.glob('**/*.tsv') if '._' not in i.stem]

def get_year(fn):
    return int(fn.stem.split('_')[-1])

def get_group(fn):
    return int(fn.parts[-2].split('_')[0])

def get_module(fn):
    return int(fn.parts[-1].split('_')[0])

def dict_filter(d, fun, mylist, keep=True):
    if keep:
        d2 = {y: [val for val in d[y] if fun(val) in mylist] for y in d.keys()}
    else:
        d2 = {y: [val for val in d[y] if fun(val) not in mylist] for y in d.keys()}
    return d2

def get_fns(my_dict):
    for x in my_dict:
        if 'purchases_' in x.stem:
            purch_fn = x
        if 'trips_' in x.stem:
            trip_fn = x
        if 'panelists_' in x.stem:
            panelist_fn = x
    try:
        purch_fn
    except:
        print(my_dict)
        raise Exception("Could not find Purchase files")
    try:
        trip_fn
    except:
        print(my_dict)
        raise Exception("Could not find Trips files")
    try:
        panelist_fn
    except:
        print(my_dict)
        raise Exception("Could not find Panelist files")
    return(purch_fn, trip_fn, panelist_fn)

# Constants for Panelist reader
hh_dict_rename = {'Household_Cd': 'household_code', 'Panel_Year': 'panel_year',
                  'Projection_Factor': 'projection_factor', 'Household_Income': 'household_income',
                  'Fips_State_Desc': 'fips_state_desc', 'DMA_Cd': 'dma_code'}


class NielsenReader(object):    
    # Constructor
    def __init__(self, read_dir=None):
        if read_dir:
            self.read_dir = read_dir
        else:
            self.read_dir = Path.cwd()

        self.file_list = self.get_file_list()

        # take last appearance of products.tsv
        prodlist = [x for x in self.file_list if x.name == 'products.tsv']
        if prodlist:
            self.product_file = prodlist[-1]
        else:
            raise Exception("Could not find a valid products.tsv")

        # strip temporary stuff and get Movement files only
        saleslist = [y for y in self.file_list if 'Movement_Files' in y.parts]
        if not saleslist:
            raise Exception("Could not find Movement Files (scanner data)")
        try:
            [get_group(x) for x in saleslist]
        except:
            raise Exception("Could not get Group Code from Movement files")
        try:
            [get_module(x) for x in saleslist]
        except:
            raise Exception("Could not get Module Code from Movement files")
        try:
            all_years = set([get_year(x) for x in saleslist])
        except:
            raise Exception("Could not get Year from Movement files")

        self.stores_dict = {get_year(x): x for x in [y for y in self.file_list if 'stores' in y.name]}
        self.rms_dict = {get_year(x): x for x in [y for y in self.file_list if 'rms_versions' in y.name]}

        self.sales_dict = {y: [x for x in saleslist if get_year(x) == y] for y in all_years}
        self.stores_df = pd.DataFrame()
        self.rms_df = pd.DataFrame()
        self.sales_df = pd.DataFrame()
        self.prod_df = pd.DataFrame()
        return

    def get_file_list(self):
        return(get_files(self.read_dir))

    def filter_years(self, keep=None, drop=None):
        def year_helper(my_dict, keep=None, drop=None):
            if keep:
                new_dict = {k: v for k, v in my_dict.items() if k in keep}
            if drop:
                new_dict = {k: v for k, v in my_dict.items() if k not in drop}
            return new_dict
        self.sales_dict = year_helper(self.sales_dict, keep, drop)
        self.stores_dict = year_helper(self.stores_dict, keep, drop)
        self.rms_dict = year_helper(self.rms_dict, keep, drop)
        return

    def filter_sales(self, keep_groups=[], drop_groups=[], keep_modules=[], drop_modules=[]):
        if not (isinstance(keep_groups, list) & isinstance(drop_groups, list) & isinstance(keep_modules, list) & isinstance(drop_modules, list)):
            raise Exception("Filters must all be lists")
        if drop_groups:
            self.sales_dict = dict_filter(self.sales_dict, get_group, drop_groups, keep=False)
        if keep_groups:
            self.sales_dict = dict_filter(self.sales_dict, get_group, keep_groups, keep=True)
        if drop_modules:
            self.sales_dict = dict_filter(self.sales_dict, get_module, drop_modules, keep=False)
        if keep_modules:
            self.sales_dict = dict_filter(self.sales_dict, get_module, keep_modules, keep=True)
        return

    def read_rms(self):
        self.rms_df = pa.concat_tables([csv.read_csv(fn, parse_options=csv.ParseOptions(delimiter='\t'),
            convert_options=csv.ConvertOptions(column_types={'upc': pa.int64(), 'upc_ver_uc': pa.uint8()})
            ) for fn in self.rms_dict.values()]).to_pandas()
        return

    def read_product(self, upc_list=None):
        print("Reading product data...")
        prod_df = csv.read_csv(self.product_file,parse_options=csv.ParseOptions(delimiter='\t')).to_pandas()
        if upc_list:
            prod_df = prod_df[prod_df.upc.isin(upc_list)]

        # Clean up product descriptions
        #prod_df['upc_descr'] = prod_df['upc_descr'].str.strip().str.replace('RTE', '')
        #prod_df['brand_descr'] = prod_df['brand_descr'].str.strip().str.replace('CTL BR', 'Private Label')

        self.prod_df = prod_df.copy()
        return

    def read_stores(self):
        s_cols = ['retailer_code', 'parent_code', 'fips_state_code', 'fips_county_code', 'dma_code', 'store_zip3']
        # To reduce space -- update with dictionary arrays later
        store_convert = {'year': pa.uint16(), 'dma_code': pa.uint16(), 'retailer_code': pa.uint16(), 'parent_code': pa.uint16(),
                         'store_zip3': pa.uint16(), 'fips_county_code': pa.uint16(), 'fips_state_code': pa.uint8()}

        # Use pyarrow to read CSVs and parse using the dict -- we have to fix some types again later.
        tmp = pa.concat_tables(
            [csv.read_csv(x, parse_options=csv.ParseOptions(delimiter='\t'),
                convert_options=csv.ConvertOptions(column_types=store_convert))
            for x in self.stores_dict.values()]).to_pandas().rename(columns={'year': 'panel_year'})

        # some columns have blanks --fill with zero to avoid converting to floats(!)
        tmp.loc[:, s_cols] = tmp.loc[:, s_cols].fillna(0)

        # use the compressed types
        my_dict = {key: value for (key, value) in type_dict.items() if key in tmp.columns}
        self.stores_df = tmp.astype(my_dict)
        return

    def filter_stores(self, keep_dma=None, drop_dma=None, keep_states=None, drop_states=None, keep_channel=None, drop_channel=None):
        self.read_stores()
        stores_df = self.stores_df.copy()
        print("Initial Store-Year Count:", len(stores_df))
        if keep_channel:
            stores_df = stores_df[stores_df.channel_code.isin(keep_channel)]
        if drop_channel:
            stores_df = stores_df[~stores_df.channel_code.isin(drop_channel)]
        if keep_states:
            stores_df = stores_df[stores_df.fips_state_descr.isin(keep_states)]
        if drop_states:
            stores_df = stores_df[~stores_df.fips_state_descr.isin(drop_states)]
        if keep_dma:
            stores_df = stores_df[stores_df.dma_code.isin(keep_dma)]
        if drop_dma:
            stores_df = stores_df[~stores_df.dma_code.isin(drop_dma)]
        print("Filtered Store-Year Count:", len(stores_df))
        self.stores_df = stores_df
        return

    def summarize_data(self):
        print("Sales Rows:\t", len(self.sales_df))
        print("Product Rows:\t", len(self.prod_df))
        print("Stores Rows:\t", len(self.stores_df))
        return

    def write_data(self, write_dir=None,stub=None,compr='brotli'):
        if not stub:
            stub = 'out'
        if write_dir:
            self.write_dir = write_dir
        else:
            self.write_dir = Path.cwd()

        fn_stores = self.write_dir / (stub+'_'+'stores.parquet')
        fn_sales = self.write_dir / (stub+'_'+'sales.parquet')
        fn_prods = self.write_dir / (stub+'_'+'products.parquet')

        self.stores_df.to_parquet(fn_stores, compression=compr)
        self.prod_df.to_parquet(fn_prods, compression=compr)
        self.sales_df.to_parquet(fn_sales, compression=compr)
        return

    # This does the bulk of the work
    # Notice that everything is nested inside here
    def process_sales(self, store_cols=['retailer_code', 'dma_code'], sales_promo=True):
        if len(self.stores_df) == 0:
            self.read_stores()

        # returns a pyarrow table filtered by stores_list
        def read_one_sales(fn, stores_list=None, incl_promo=True):
            my_cols = ['store_code_uc', 'upc', 'week_end', 'units', 'prmult', 'price']
            if incl_promo:
                my_cols = my_cols + ['feature', 'display']
            convert_dict = {'feature': pa.int8(), 'display': pa.int8(), 'prmult': pa.int8(), 'units': pa.uint16(), 'store_code_uc': pa.uint32()}
            dataset = ds.dataset(csv.read_csv(fn, parse_options=csv.ParseOptions(delimiter='\t'),
                convert_options=csv.ConvertOptions(column_types=convert_dict, include_columns=my_cols)))
            if stores_list is None:
                return dataset.to_table()
            else:
                return dataset.to_table(filter=ds.field('store_code_uc').isin(stores_list))

        # returns a py_arrow table (filterd by self.stores_df)
        def do_one_year(y, sales_promo=True):
            start = time.time()
            print("Processing Year:\t", y)
            stores_list = self.stores_df[self.stores_df.panel_year == y].store_code_uc.unique()
            out = pa.concat_tables([read_one_sales(f, stores_list, incl_promo=sales_promo) for f in self.sales_dict[y]])
            end = time.time()
            print("in ", end-start, " seconds.")
            return out

        # fix dates and types
        def do_cleaning(df):
            # fix 2 for $5.00 as $2.50
            df.loc[df.prmult > 1, 'price'] = df.loc[df.prmult > 1, 'price']/df.loc[df.prmult > 1, 'prmult']
            date_dict = {x: pd.to_datetime(x, format='%Y%m%d') for x in df.week_end.unique()}
            df['week_end'] = df['week_end'].map(date_dict)
            df['panel_year'] = df['week_end'].dt.year.astype('uint16')
            if 'feature' in df.columns:
                df.feature.fillna(-1, inplace=True)
                df.display.fillna(-1, inplace=True)
            # re-cast the datatypes in case something went wrong
            my_dict = {key: value for (key, value) in type_dict.items() if key in df.columns}
            return df.drop(columns=['prmult']).astype(my_dict)

        start = time.time()
        df = pa.concat_tables([do_one_year(y, sales_promo) for y in self.sales_dict.keys()]).to_pandas()

        # Read in and merge the RMS data for upc_ver_uc
        self.read_rms()

        # merge the sales with the upc_ver_uc from RMS and store/geography info
        self.sales_df = pd.merge(pd.merge(
            do_cleaning(df),
            self.rms_df, on=['upc', 'panel_year']),
            self.stores_df[['store_code_uc', 'panel_year']+store_cols], on=['store_code_uc', 'panel_year'])\
            .reset_index(drop=True)
        end = time.time()
        print("Total Time ", end-start, " seconds.")

        # update stores and products
        self.stores_df = self.stores_df[self.stores_df.store_code_uc.isin(self.sales_df.store_code_uc.unique())].copy()
        self.read_product(upc_list=list(self.sales_df.upc.unique()))
        self.summarize_data()
        return

class PanelistReader(object):   
    # Constructor
    def __init__(self, read_dir=None):
        if read_dir:
            self.read_dir = read_dir
        else:
            self.read_dir = Path.cwd()

        file_list = self.get_file_list()

        self.file_list_master = [x for x in file_list if 'Master_Files' == x.parts[-3]]
        self.file_list_annual = [x for x in file_list if 'Annual_Files' == x.parts[-2]]

        prodlist = [x for x in self.file_list_master if x.name == 'products.tsv']
        if prodlist:
            self.product_file = prodlist[-1]
        else:
            raise Exception("Could not find a valid products.tsv")

        if not self.file_list_annual:
            raise Exception("Could not find Annual Files (purchases, trips, households)")

        try:
            all_years = set([get_year(x) for x in self.file_list_annual])
        except:
            raise Exception("Could not get Year from Movement files")

        self.annual_dict = {y: [x for x in self.file_list_annual if get_year(x)== y] for y in all_years}

        self.stores_df = pd.DataFrame()
        self.rms_df = pd.DataFrame()

        self.purch_df = pd.DataFrame()
        self.prod_df = pd.DataFrame()
        self.trip_df = pd.DataFrame()
        self.hh_df = pd.DataFrame()

        self.hh_cols = []
        self.prod_cols = []

    def get_file_list(self):
        return get_files(self.read_dir)

    def filter_years(self, keep=None, drop=None):
        def year_helper(my_dict, keep=None, drop=None):
            if keep:
                new_dict = {k: v for k, v in my_dict.items() if k in keep}
            if drop:
                new_dict = {k: v for k, v in my_dict.items() if k not in drop}
            return new_dict
        self.annual_dict = year_helper(self.annual_dict, keep, drop)
        return

    # make sure we add keys to user supplied columns
    def set_prod_cols(self, prod_cols=None):
        if prod_cols:
            self.prod_cols = list(set(prod_cols).union(set(['upc', 'upc_ver_uc'])))
        else:
            self.prod_cols = ['upc', 'upc_ver_uc', 'product_module_code', 'product_group_code',
                              'multi', 'size1_code_uc', 'size1_amount', 'size1_units']
        return

    # make sure we add keys to user supplied columns
    def set_hh_cols(self, hh_cols=None):
        if hh_cols:
            self.hh_cols = list(set(hh_cols).union(set(['household_code', 'panel_year'])))
        else:
            self.hh_cols = ['household_code', 'panel_year', 'projection_factor', 'household_income', 'fips_state_desc']
        return

    # Filter the product list by groups or modules
    # Run this before reading in the other data
    def read_product(self, keep_groups=None, drop_groups=None, keep_modules=None, drop_modules=None):
        prod_cols = ['upc', 'upc_ver_uc', 'upc_descr', 'product_module_code', 'product_module_descr',
                     'product_group_code', 'product_group_descr', 'brand_code_uc',
                     'brand_descr', 'multi', 'size1_code_uc', 'size1_amount',
                     'size1_units', 'dataset_found_uc', 'size1_change_flag_uc']

        prod_dict = {'upc': pa.int64(), 'upc_ver_uc': pa.int8(), 'product_module_code': pa.uint16(),
                     'brand_code_uc': pa.uint32(), 'multi': pa.uint16(), 'size1_code_uc': pa.uint16()}

        prod_df = csv.read_csv(self.product_file, read_options=csv.ReadOptions(encoding='latin'),
                                               parse_options=csv.ParseOptions(delimiter='\t'),
                                               convert_options=csv.ConvertOptions(column_types=prod_dict, include_columns=prod_cols)
                                               ).to_pandas()
        if keep_groups:
            prod_df = prod_df[prod_df['product_group_code'].isin(keep_groups)]
        if drop_groups:
            prod_df = prod_df[~prod_df['product_group_code'].isin(drop_groups)]
        if keep_modules:
            prod_df = prod_df[prod_df['product_module_code'].isin(keep_modules)]
        if drop_modules:
            prod_df = prod_df[~prod_df['product_module_code'].isin(drop_modules)]

        # dictionary encoding to save space
        prod_df['size1_units'] = prod_df['size1_units'].astype('category')
        prod_df['product_module_descr'] = prod_df['product_module_descr'].astype('category')
        prod_df['product_group_code'] = prod_df['product_group_code'].astype('category')

        # clean up product info
        prod_df['upc_descr'] = prod_df['upc_descr'].str.strip().str.replace('RTE', '')
        prod_df['brand_descr'] = prod_df['brand_descr'].str.strip().str.replace('CTL BR', 'Private Label')
        self.prod_df = prod_df.copy()
        return

    def read_year(self, year, hh_states_keep=None, hh_states_drop=None, hh_dma_keep=None, hh_dma_drop=None):

        (purch_fn, trip_fn, panelist_fn) = get_fns(self.annual_dict[year])

        hh_ds = ds.dataset(csv.read_csv(panelist_fn, parse_options=csv.ParseOptions(delimiter='\t'),
            convert_options=csv.ConvertOptions(auto_dict_encode=True, auto_dict_max_cardinality=1024)))

        # build an arrow dataset filter object one by one
        my_filter = ds.field('Projection_Factor') > 0
        if hh_states_keep:
            my_filter = my_filter & (ds.field('Fips_State_Desc').isin(hh_states_keep))
        if hh_states_drop:
            my_filter = my_filter & (~ds.field('Fips_State_Desc').isin(hh_states_drop))
        if hh_dma_keep:
            my_filter = my_filter & (ds.field('DMA_Cd').isin(hh_dma_keep))
        if hh_dma_drop:
            my_filter = my_filter & (~ds.field('DMA_Cd').isin(hh_dma_drop))
            
        # convert to pandas and get unique HH list
        hh_df = hh_ds.to_table(filter=my_filter).to_pandas().rename(columns=hh_dict_rename)
        hh_list = hh_df.household_code.unique()

        # use pyarrrow filter to filter trips for just our households
        trip_df = ds.dataset(csv.read_csv(trip_fn, parse_options=csv.ParseOptions(delimiter='\t')))\
                  .to_table(filter=ds.field('household_code').isin(hh_list)).to_pandas()

        trip_list = trip_df.trip_code_uc.unique()
        upc_list = self.prod_df.upc.unique()

        # use pyarrow to filter purchases using trips and UPCs only
        purch_ds = ds.dataset(csv.read_csv(purch_fn, parse_options=csv.ParseOptions(delimiter='\t'),
                convert_options=csv.ConvertOptions(auto_dict_encode=True, auto_dict_max_cardinality=1024)))
        purch_filter = ds.field('trip_code_uc').isin(trip_list) & ds.field('upc').isin(upc_list)
        purch_df = purch_ds.to_table(filter=purch_filter).to_pandas()

        # Add the fields to the trips and purchases for convenience later
        trip_df2 = pd.merge(trip_df, hh_df[self.hh_cols], on=['household_code', 'panel_year'])
        purch_df2 = pd.merge(pd.merge(
            purch_df,
            self.prod_df[self.prod_cols], on=['upc', 'upc_ver_uc']),
            trip_df2[self.hh_cols+['trip_code_uc', 'purchase_date', 'store_code_uc']], on=['trip_code_uc'])\
            .rename(columns={'fips_state_desc': 'hh_state_desc'})

        self.purch_df = self.purch_df.append(purch_df2, ignore_index=True)
        self.trip_df = self.trip_df.append(trip_df2, ignore_index=True)
        self.hh_df = self.hh_df.append(hh_df, ignore_index=True)
        return

    def read_all(self, hh_states_keep=None, hh_states_drop=None, hh_dma_keep=None, hh_dma_drop=None):
        # make sure there is a product list and columns to keep, otherwise set defaults
        if self.prod_df.empty:
            self.read_product()
        if not self.prod_cols:
            self.set_prod_cols()
        if not self.hh_cols:
            self.set_hh_cols()

        print("Parse List:")
        for z in sorted({x for v in self.annual_dict.values() for x in v}):
            print(z)

        for year in self.annual_dict:
            start=time.time()
            print("Processing Year:\t", year)
            self.read_year(year, hh_states_keep=hh_states_keep, hh_states_drop=hh_states_drop, hh_dma_keep=hh_dma_keep, hh_dma_drop=hh_dma_drop)
            end=time.time()
            print("Time: ", end-start)

    def write_data(self, write_dir=None, stub=None, compr='brotli'):
        if not stub:
            stub = 'out'
        if write_dir:
            self.write_dir = write_dir
        else:
            self.write_dir = Path.cwd()

        fn_purch = self.write_dir / (stub+'_'+'purchases.parquet')
        fn_trip = self.write_dir / (stub+'_'+'trips.parquet')
        fn_hh = self.write_dir / (stub+'_'+'households.parquet')
        fn_prods = self.write_dir / (stub+'_'+'products.parquet')

        self.purch_df.to_parquet(fn_purch, compression=compr)
        self.trip_df.to_parquet(fn_trip, compression=compr)
        self.hh_df.to_parquet(fn_hh, compression=compr)
        self.prod_df.to_parquet(fn_prods, compression=compr)
        return
