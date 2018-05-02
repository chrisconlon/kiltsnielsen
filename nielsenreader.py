import sys,os
import pandas as pd
import fnmatch
import pyarrow as pa
import pyarrow.parquet as pq
import datetime

defaultcols=['store_code_uc','upc','units','price','feature','display','dma_code','retailer_code','upc_ver_uc','week_end']

# This is the main interface
# This pre-processes files directly downloaded from Kilts and understands the Kilts-Nielsen directory structure
# The processed results are saved by DMA code in a parquet file for fast reading and processing
#
# Arguments:
# - read_dir: base directory of Kilts-Nielsen files to search through
# - outfile: stub of file name for processed output, creates two files:
# 	- .parquet: with price and quantity data
# 	- .hdf: with stores and product tables (named "stores", and "prods")
# Filtering the data
# Optional Arguments:
#	- statelist (list of two letter state abbreviations: eg ['CT','NY','NJ'])
#	- dmalist (list of dma codes eg: [603, 506])
#   - module_code a single module code (eg. 1344 for Cereal, etc.)
#   - channel_filter: a list of channels (e.g ['F','D'] for food and drug stores)

def read_all_data_new(read_dir,outfile,statelist=None,dmalist=None,module_code=None,channel_filter=['F','M'],cols=defaultcols):
	# list of price-quantity files
	fns=get_files(read_dir,'Movement')
	# filter numeric part of module code out of filename
	if module_code:
		fns=[s for s in fns if module_code ==int(os.path.split(s)[1].split('_')[0])]

	# this does all of the work
	
	df=pd.concat([read_single_file_new(fn,read_dir,statelist,dmalist,channel_filter) for fn in fns],ignore_index=True)

	# some cleaning up to reduce space
	df.feature.fillna(-1,inplace=True)
	df.display.fillna(-1,inplace=True)
	df['display']=df['display'].astype('int8')
	df['feature']=df['feature'].astype('int8')
	df['panel_year']=df['panel_year'].astype('int16')
	df['store_zip3']=df.store_zip3.astype('int16')
	df['retailer_code']=df['retailer_code'].astype('int16')
	df['dma_code']=df['dma_code'].astype('int16')
	df['prmult']=df['prmult'].astype('int8')

	# fix 2 for $5.00 as $2.50
	df.loc[df.prmult>1,'price']=df.loc[df.prmult>1,'price']/df.loc[df.prmult>1,'prmult']

	# Read the products (matching only)
	prods=pd.merge(pd.read_table(get_files(read_dir,'products.tsv')[0]),df[['upc','upc_ver_uc']].drop_duplicates(),on=['upc','upc_ver_uc'])
	print("Number of Products: ",str(len(prods)))

	# Read the stores (matching only)
	stores=pd.concat([get_stores(read_dir,statelist=None,my_year=my_year,dmalist=dmalist) for my_year in range(2006,2015+1)]).groupby(level=0).last()

	# Use python dates not Nielsen dates
	df=fix_dates(df)

	# Write to an parquet file (too big for other formats!)
	write_by_dma(df[cols],outfile+'.parquet')
	# Write the rest as HDF5 tables
	stores.to_hdf(outfile+'.hdf','stores',table=False,append=False)
	prods.drop_duplicates().to_hdf(outfile+'.hdf','prods',table=False,append=False)

# This reads a single movement file
def read_single_file_new(fn,read_dir,statelist=None,dmalist=None,channel_filter=None):
	print ("Processing ",fn)
	my_year=int(fn.split('_')[-1].split('.')[0])
	rms=pd.read_table(filter_year(get_files(read_dir,'rms_version'),my_year))
	all_stores = get_stores(read_dir,statelist,my_year,storelist=None,dmalist=dmalist)[['store_zip3','dma_code','channel_code','retailer_code']]
	if channel_filter:
		our_stores=all_stores[all_stores.channel_code.isin(list(channel_filter))]
	else:
		our_stores=all_stores
	return pd.merge(pd.merge(pd.read_table(fn),our_stores,left_on='store_code_uc',right_index=True),rms,on='upc')

# This fixes nielsen dates to python dates
def fix_dates(df):
    a=df.week_end.unique()
    x=pd.Series(a,index=a,name='week')
    return pd.merge(df,pd.DataFrame(x.apply(lambda z :split_date(z))),left_on='week_end',right_index=True).drop(columns='week_end').rename(columns={'week':'week_end'})
def split_date(x):
	y=str(x)
	return datetime.datetime(int(y[0:4]),  int(y[4:6]), int(y[6:8]))

# Some file utilities
def get_files(mydir,myfilter):
	matches = []
	for root, dirnames, filenames in os.walk(mydir):
		for filename in fnmatch.filter(filenames, '*.tsv'):
			matches.append(os.path.join(root, filename))
	return [s for s in matches if myfilter in s]

def filter_year(fns,year):
	return [x for x in fns if str(year) in x][0]

def get_stores(read_dir,statelist,my_year,storelist=None,dmalist=None):
	fns=[s for s in get_files(read_dir,'stores') if str(my_year) in s]
	stores=pd.read_table(fns[0],index_col='store_code_uc')
	stores.fips_state_descr.value_counts()
	stores['channel_code']=stores.channel_code.astype('category')
	stores['retailer_code']=stores.retailer_code.combine_first(stores.parent_code)

	if statelist:
		stores=stores[stores.fips_state_descr.isin(statelist)]
	if dmalist:
		stores=stores[stores.dma_code.isin(dmalist)]
	stores.dma_descr.value_counts()
	if storelist:
		stores=stores[stores.index.isin(storelist)]
	return stores

##
# Utilities to read and write parquet files
##

# can pass a wrapper that processes each group and list of columns
def read_parquet_groups(pq_file,read_func=pd.DataFrame,col_list=None):
    parquet_file = pq.ParquetFile(pq_file)
    super_df =[]
    for i in range(0,parquet_file.num_row_groups):
        super_df.append(read_func(parquet_file.read_row_group(i,nthreads=4,columns=col_list,use_pandas_metadata=True).to_pandas(nthreads=4)))
    return pd.concat(super_df, axis=0)

def write_by_dma(super_df,filename):
	# Write our data to a parquet file -- each row group is a a DMA
	arrow_Table=pa.Table.from_pandas(super_df)
	writer = pq.ParquetWriter(filename,arrow_Table.schema,compression='brotli')
	for x in super_df.dma_code.unique():
		writer.write_table(pa.Table.from_pandas(super_df[super_df.dma_code==x]))
	if writer:
		writer.close()