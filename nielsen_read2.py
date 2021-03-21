import pandas as pd
import time
import pyarrow as pa
from pyarrow import csv
import pathlib
from pathlib import Path
import re

# Pure functions here
def get_year(fn):
    return int(fn.stem.split('_')[-1])

def get_group(fn):
    return int(fn.parts[-2].split('_')[0])

def get_module(fn):
    return int(fn.parts[-1].split('_')[0])

def get_yearp(fn):
    x = re.search('(\d{4})',str(fn))
    if x:
        return int(x[0])
    else:
        return None

def get_fns(my_dict):
    for x in my_dict:
        if 'purchases_' in str(x):
            purch_fn=x
        if 'trips_' in str(x):
            trip_fn=x
        if 'panelists_' in str(x):
            panelist_fn=x
    try:
        purch_fn
    except:
        print(my_dict)
        raise Exception("Could not find Purchase files")
    try:
        trip_fn
    except:
        print(trip_fn)
        raise Exception("Could not find Trips files")
    try:
        panelist_fn
    except:
        print(panelist_fn)
        raise Exception("Could not find Panelist files")
    return(purch_fn, trip_fn, panelist_fn)

# Constants for Panelist reader
prod_keep_cols = ['upc','upc_ver_uc','product_module_code','product_group_code','multi','size1_code_uc','size1_amount','size1_units']
hh_dict_rename = {'Household_Cd':'household_code','Panel_Year':'panel_year','Projection_Factor':'projection_factor','Household_Income':'household_income','Fips_State_Desc':'fips_state_desc','DMA_Cd':'dma_code'}
hh_keep_cols = ['household_code','panel_year','projection_factor','household_income','fips_state_desc']


class NielsenReader(object):    
    # Constructor
    def __init__(self, read_dir=None):
        #self.bins = bins  # Create an instance variable
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

    def get_file_list(self):
        return [i for i in self.read_dir.glob('**/*.tsv') and '._' not in i.stem]

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

    def filter_sales(self, keep_groups=None, drop_groups=None, keep_modules=None, drop_modules=None):
        if not (isinstance(keep_groups, list) & isinstance(drop_groups, list)  & isinstance(keep_modules, list) & isinstance(drop_modules, list)):
            raise Exception("Filters must all be lists")
        if drop_groups:
            self.sales_dict = {y: [x for x in self.sales_dict[y] if get_group(x) not in drop] for y in self.sales_dict.keys()}
        if keep_groups:
            self.sales_dict = {y: [x for x in self.sales_dict[y] if get_group(x) in keep] for y in self.sales_dict.keys()}
        if drop_modules:
            self.sales_dict = {y: [x for x in self.sales_dict[y] if get_module(x) not in drop] for y in self.sales_dict.keys()}
        if keep_modules:
            self.sales_dict = {y: [x for x in self.sales_dict[y] if get_module(x) in keep] for y in self.sales_dict.keys()}
        return

    def read_rms(self):
        self.rms_df = pa.concat_tables([csv.read_csv(fn,parse_options=csv.ParseOptions(delimiter='\t')) for fn in self.rms_dict.values()]).to_pandas()
        return

    def read_product(self, upc_list=None):
        prod_df = csv.read_csv(self.product_file,parse_options=csv.ParseOptions(delimiter='\t')).to_pandas()
        if upc_list:
            prod_df = prod_df[prod_df.upc.isin(upc_list)]
        self.prod_df = prod_df.copy()
        return

    def read_stores(self):
        # To reduce space -- update with dictionary arrays later
        store_convert={'panel_year':pa.uint16(),'dma_code':pa.uint16(), 'retailer_code':pa.uint16(),'parent_code':pa.uint16(),'store_zip3':pa.uint16(),
        'fips_county_code':pa.uint16(),'fips_state_code':pa.uint8()}
        self.stores_df=pa.concat_tables([ \
            csv.read_csv(x,parse_options=csv.ParseOptions(delimiter='\t'), convert_options=csv.ConvertOptions(column_types=store_convert)) \
            for x in self.stores_dict.values() \
            ]).to_pandas()
        return

    def filter_stores(self, keep_dma=None, drop_dma=None, keep_states=None, drop_states=None, keep_channel=None, drop_channel=None):
        self.read_stores()
        stores_df = self.stores_df.copy()
        print("Initial Store-Year Count:",len(stores_df))
        if keep_channel:
            stores_df=stores_df[stores_df.channel_code.isin(keep_channel)]
        if drop_channel:
            stores_df=stores_df[~stores_df.channel_code.isin(drop_channel)]
        if keep_states:
            stores_df=stores_df[stores_df.fips_state_descr.isin(keep_states)]
        if drop_states:
            stores_df=stores_df[~stores_df.fips_state_descr.isin(drop_states)]
        if keep_dma:
            stores_df=stores_df[stores_df.dma_code.isin(keep_dma)]
        if drop_dma:
            stores_df=stores_df[~stores_df.dma_code.isin(drop_dma)]
        print("Filtered Store-Year Count:",len(stores_df))
        self.stores_df = stores_df

    def summarize_data(self):
        print("Sales Rows:\t",len(self.sales_df))
        print("Product Rows:\t",len(self.prod_df))
        print("Stores Rows:\t",len(self.stores_df))

    def write_data(self,write_dir=None,stub=None,compr='brotli'):
        if not stub:
            stub ='out'
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

    # This does the bulk of the work
    def process_sales(self, store_cols=['retailer_code', 'dma_code'], sales_promo=True):
        if len(self.stores_df) == 0:
            self.read_stores()
        stores = self.stores_df[['store_code_uc', 'year']+store_cols]

        def read_sales_list(my_list, incl_promo=True):
            return pa.concat_tables([read_one_sales(x,incl_promo=incl_promo) for x in my_list]).to_pandas()

        def read_one_sales(fn, incl_promo=True):
            my_cols = ['store_code_uc','upc','week_end','units','prmult','price']
            if incl_promo:
                my_cols = my_cols + ['feature', 'display']
            convert_dict = {'feature': pa.int8(), 'display': pa.int8(), 'prmult': pa.int8(), 'units': pa.uint16()}
            return csv.read_csv(fn, parse_options=csv.ParseOptions(delimiter='\t'),
                convert_options=csv.ConvertOptions(column_types=convert_dict, include_columns=my_cols))

        def do_one_year(y, sales_promo=True):
            start = time.time()
            print("Processing Year:\t",y)
            tmp = pd.merge(stores[stores.year == y], read_sales_list(self.sales_dict[y], incl_promo=sales_promo), on=['store_code_uc'])
            end = time.time()
            print("in ", end-start, " seconds.")
            return tmp

        def do_cleaning(df, sales_promo=True):
            # fix 2 for $5.00 as $2.50
            df.loc[df.prmult > 1, 'price'] = df.loc[df.prmult > 1, 'price']/df.loc[df.prmult > 1, 'prmult']
            date_dict = {x: pd.to_datetime(x, format='%Y%m%d') for x in df.week_end.unique()}
            df['week_end'] = df['week_end'].map(date_dict)

            if sales_promo:
                df.feature.fillna(-1, inplace=True)
                df.display.fillna(-1, inplace=True)
                df['display'] = df['display'].astype('int8')
                df['feature'] = df['feature'].astype('int8')
            return df.drop(columns=['prmult']).rename(columns={'year': 'panel_year'})

        start = time.time()
        df = pd.concat([do_one_year(y, sales_promo) for y in self.sales_dict.keys()], axis=0)

        # Read in and merge the RMS data
        self.read_rms()
        #self.sales_df=do_cleaning(df,sales_promo)
        self.sales_df = pd.merge(do_cleaning(df, sales_promo), self.rms_df, on=['upc', 'panel_year'])
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
        #self.bins = bins  # Create an instance variable
        if read_dir:
            self.read_dir = read_dir
        else:
            self.read_dir = Path.cwd()

        self.file_list_master =  [x for x in self.get_file_list() if re.search('Master_Files',str(x))] 
        self.file_list_annual =  [x for x in self.get_file_list() if re.search('Annual_Files',str(x))] 

        prodlist = [x for x in self.file_list_master if x.name == 'products.tsv']
        if prodlist:
            self.product_file = prodlist[-1]
        else:
            raise Exception("Could not find a valid products.tsv")
        
        if not self.file_list_annual:
            raise Exception("Could not find Annual Files (purchases, trips, households)")

        all_years=list(set([get_yearp(x) for x in  self.file_list_annual]))

        self.annual_dict = {y:[x for x in self.file_list_annual if get_yearp(x)==y] for y in all_years}

        self.stores_df = pd.DataFrame()
        self.rms_df = pd.DataFrame()

        self.purch_df = pd.DataFrame()
        self.prod_df = pd.DataFrame()
        self.trip_df = pd.DataFrame()
        self.hh_df = pd.DataFrame()
        
    def get_file_list(self):
        return [i for i in self.read_dir.glob('**/*.tsv')]

    def filter_years(self, keep=None, drop=None):
        def year_helper(my_dict, keep=None, drop=None):
            if keep:
                new_dict = {k: v for k, v in my_dict.items() if k in keep}
            if drop:
                new_dict = {k: v for k, v in my_dict.items() if k not in drop}
            return new_dict
        self.sales_dict = year_helper(self.annual_dict, keep, drop)
        return

    # Filter the product list by groups or modules
    # Run this before reading in the other data
    def read_product(self, keep_groups=None, drop_groups=None, keep_modules=None, drop_modules=None):

        prod_cols=['upc', 'upc_ver_uc', 'upc_descr', 'product_module_code','product_module_descr', 'product_group_code', 'product_group_descr',
        'brand_code_uc', 'brand_descr','multi', 'size1_code_uc', 'size1_amount', 'size1_units','dataset_found_uc', 'size1_change_flag_uc']

        prod_dict={'upc':pa.int64(), 'upc_ver_uc':pa.int8(), 'product_module_code':pa.uint16(),'brand_code_uc':pa.uint32(),
           'multi':pa.uint16(),'size1_code_uc':pa.uint16()}

        prod_df=csv.read_csv(self.product_file,read_options=csv.ReadOptions(encoding='latin'), 
                                               parse_options=csv.ParseOptions(delimiter='\t'),
                                               convert_options=csv.ConvertOptions(column_types=prod_dict,include_columns=prod_cols)
                                               ).to_pandas()
        if keep_groups:
            prod_df=prod_df[prod_df['product_group_code'].isin(keep_groups)]
        if drop_groups:
            prod_df=prod_df[~prod_df['product_group_code'].isin(drop_groups)]
        if keep_modules:
            prod_df=prod_df[prod_df['product_module_code'].isin(keep_modules)]
        if drop_modules:
            prod_df=prod_df[~prod_df['product_module_code'].isin(drop_modules)]
        
        prod_df['size1_units']=prod_df['size1_units'].astype('category')
        prod_df['product_module_descr']=prod_df['product_module_descr'].astype('category')
        prod_df['product_group_code']=prod_df['product_group_code'].astype('category')
        
        self.prod_df = prod_df.copy()
        return

    def read_year(self,year,hh_states_keep=None,hh_states_drop=None,hh_dma_keep=None,hh_dma_drop=None):

        (purch_fn,trip_fn,panelist_fn)=get_fns(self.sales_dict[year])

        hh_df = csv.read_csv(panelist_fn,parse_options=csv.ParseOptions(delimiter='\t'),
            convert_options=csv.ConvertOptions(auto_dict_encode=True,auto_dict_max_cardinality=1024)
            ).to_pandas().rename(columns=hh_dict_rename)
        if hh_states_keep:
            hh_df = hh_df[hh_df['fips_state_desc'].isin(hh_states_keep)]
        if hh_states_drop:
            hh_df = hh_df[~hh_df['fips_state_desc'].isin(hh_states_drop)]
        if hh_dma_keep:
            hh_df = hh_df[hh_df['dma_code'].isin(hh_dma_keep)]
        if hh_dma_drop:
            hh_df = hh_df[~hh_df['dma_code'].isin(hh_dma_drop)]

        trip_df = pd.merge(csv.read_csv(trip_fn, parse_options=csv.ParseOptions(delimiter='\t')).to_pandas(),
            hh_df[hh_keep_cols],
            on=['household_code', 'panel_year']
        )

        purch_df = pd.merge(pd.merge(
            csv.read_csv(purch_fn, parse_options=csv.ParseOptions(delimiter='\t')).to_pandas(),
            self.prod_df[prod_keep_cols], on=['upc','upc_ver_uc']),
             trip_df[hh_keep_cols+['trip_code_uc', 'purchase_date', 'store_code_uc']], on=['trip_code_uc']).rename(columns={'fips_state_desc': 'hh_state_desc'})
        self.purch_df = self.purch_df.append(purch_df, ignore_index=True)
        self.trip_df = self.trip_df.append(trip_df, ignore_index=True)
        self.hh_df = self.hh_df.append(hh_df, ignore_index=True)
        return

    def read_all(self, hh_states_keep=None, hh_states_drop=None, hh_dma_keep=None, hh_dma_drop=None):
        # make sure there is a product list first
        if self.prod_df.empty:
            self.read_product()

        print("Parse List:")
        for z in sorted({x for v in self.sales_dict.values() for x in v}):
            print(z)

        for year in self.sales_dict:
            start = time.time()
            print("Processing Year:\t", year)
            self.read_year(year, hh_states_keep=hh_states_keep, hh_states_drop=hh_states_drop, hh_dma_keep=hh_dma_keep, hh_dma_drop=hh_dma_drop)
            end = time.time()
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
