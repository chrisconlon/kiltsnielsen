import pandas as pd
import time
import pyarrow as pa
from pyarrow import csv
import pathlib
from pathlib import Path

# Pure functions here
def get_year(fn):
	return int(fn.split('_')[-1].split('.')[0])

def get_group(fn):
	return int(fn.parent.name.split('_')[0])

def get_module(fn):
	return int(fn.name.split('_')[0])


class NielsenReader(object):	
    # Constructor
    def __init__(self, read_dir=None):
        #self.bins = bins  # Create an instance variable
        if read_dir:
        	self.read_dir = read_dir
        else:
        	self.read_dir = Path.cwd()

        self.file_list = self.get_file_list()
        
        prodlist=[x for x in self.file_list if x.name=='products.tsv']
        if prodlist:
        	self.product_file = prodlist[-1]
        else:
        	raise Exception("Could not find a valid products.tsv")
        
        saleslist=[y for y in self.file_list if 'Movement_Files'  in str(y)]
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


        self.stores_dict={get_year(x.name):x for x in [y for y in self.file_list if 'stores' in y.name]}
        self.rms_dict={get_year(x.name):x for x in [y for y in self.file_list if 'rms_versions' in y.name]}

        all_years=set([get_year(x.name) for x in  saleslist])
        self.sales_dict={y:[x for x in saleslist if get_year(x.name)==y] for y in all_years}
        self.stores_df = pd.DataFrame()
        self.rms_df = pd.DataFrame()
        self.sales_df = pd.DataFrame()
        self.prod_df = pd.DataFrame()
        
    def get_file_list(self):
    	return [i  for i in self.read_dir.glob('**/*.tsv')]

    def filter_years(self,keep=None,drop=None):
    	def year_helper(my_dict,keep=None,drop=None):
    		if keep:
    			new_dict={k: v for k, v in my_dict.items() if k in keep}
    		if drop:
    			new_dict={k: v for k, v in my_dict.items() if k not in drop}
    		return new_dict
    	self.sales_dict = year_helper(self.sales_dict,keep,drop)
    	self.stores_dict = year_helper(self.stores_dict,keep,drop)
    	self.rms_dict=year_helper(self.rms_dict,keep,drop)
    	return

    def filter_sales(self,keep_groups=None,drop_groups=None,keep_modules=None,drop_modules=None):
    	if not (isinstance(keep_groups,list) & isinstance(drop_groups,list)  & isinstance(keep_modules,list) & isinstance(drop_modules,list)):
    		raise Exception("Filters must all be lists")
    	if drop_groups:
    		self.sales_dict={y: [x for x in self.sales_dict[y] if get_group(x) not in drop] for y in self.sales_dict.keys()}
    	if keep_groups:
    		self.sales_dict={y: [x for x in self.sales_dict[y] if get_group(x) in keep] for y in self.sales_dict.keys()}
    	if drop_modules:
    		self.sales_dict={y: [x for x in self.sales_dict[y] if get_module(x) not in drop] for y in self.sales_dict.keys()}
    	if keep_modules:
    		self.sales_dict={y: [x for x in self.sales_dict[y] if get_module(x) in keep] for y in self.sales_dict.keys()}
    	return
    
    def read_rms(self):
    	self.rms_df=pa.concat_tables([csv.read_csv(fn,parse_options=csv.ParseOptions(delimiter='\t'))  for fn in self.rms_dict.values()]).to_pandas()
    	return

    def read_product(self,upc_list=None):
    	prod_df=csv.read_csv(self.product_file,parse_options=csv.ParseOptions(delimiter='\t')).to_pandas()
    	if upc_list:
    		prod_df=prod_df[prod_df.upc.isin(upc_list)]
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

    def filter_stores(self,keep_dma=None,drop_dma=None,keep_states=None,drop_states=None,keep_channel=None,drop_channel=None):
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

        fn_stores= self.write_dir / (stub+'_'+'stores.parquet')
        fn_sales= self.write_dir / (stub+'_'+'sales.parquet')
        fn_prods= self.write_dir / (stub+'_'+'products.parquet')

        self.stores_df.to_parquet(fn_stores,compression=compr)
        self.prod_df.to_parquet(fn_prods,compression=compr)
        self.sales_df.to_parquet(fn_sales,compression=compr)

    # This does the bulk of the work
    def process_sales(self,store_cols=['retailer_code','dma_code'],sales_promo=True):
    	if len(self.stores_df)==0:
    		self.read_stores()
    	stores=self.stores_df[['store_code_uc','year']+store_cols]

    	def read_sales_list(my_list,incl_promo=True):
    		return pa.concat_tables([read_one_sales(x,incl_promo=incl_promo) for x in my_list]).to_pandas()

    	def read_one_sales(fn,incl_promo=True):
    		my_cols=['store_code_uc','upc','week_end','units','prmult','price']
    		if incl_promo:
    			my_cols = my_cols + ['feature','display']
    		convert_dict={'feature':pa.int8(),'display':pa.int8(),'prmult':pa.int8(),'units':pa.uint16()}
    		return csv.read_csv(fn,parse_options=csv.ParseOptions(delimiter='\t'),convert_options=csv.ConvertOptions(column_types=convert_dict,include_columns=my_cols))

    	def do_one_year(y,sales_promo=True):
    		start = time.time()
    		print("Processing Year:\t",y)
    		tmp=pd.merge(stores[stores.year==y],read_sales_list(self.sales_dict[y],incl_promo=sales_promo),on=['store_code_uc'])
    		end = time.time()
    		print("in ",end-start," seconds.")
    		return tmp

    	def do_cleaning(df,sales_promo=True):
    		# fix 2 for $5.00 as $2.50
    		df.loc[df.prmult>1,'price']=df.loc[df.prmult>1,'price']/df.loc[df.prmult>1,'prmult']
    		date_dict={x: pd.to_datetime(x,format='%Y%m%d') for x  in df.week_end.unique()}
    		df['week_end']=df['week_end'].map(date_dict)

    		if sales_promo:
    			df.feature.fillna(-1,inplace=True)
    			df.display.fillna(-1,inplace=True)
    			df['display']=df['display'].astype('int8')
    			df['feature']=df['feature'].astype('int8')
    		return df.drop(columns=['prmult']).rename(columns={'year':'panel_year'})

    	start = time.time()
    	df=pd.concat([do_one_year(y,sales_promo) for y in self.sales_dict.keys()],axis=0)

    	# Read in and merge the RMS data
    	self.read_rms()
    	#self.sales_df=do_cleaning(df,sales_promo)
    	self.sales_df=pd.merge(do_cleaning(df,sales_promo),self.rms_df,on=['upc','panel_year'])
    	end = time.time()
    	print("Total Time ",end-start," seconds.")

    	# update stores and products
    	self.stores_df=self.stores_df[self.stores_df.store_code_uc.isin(self.sales_df.store_code_uc.unique())].copy()
    	self.read_product(upc_list=list(self.sales_df.upc.unique()))
    	self.summarize_data()
    	return
