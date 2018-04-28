import sys,os

# import the nielsenreader
import nielsenreader as nielsen
from nielsenreader import read_all_data_new

# where to read and write from
read_dir = os.path.abspath(os.path.expanduser('~') + '/Cereal/')
outfile=read_dir+'raw-cereal-all'

# Specify which dmas /modules /columns to keep
dmas=[506,517,556, 602, 751,753]
module_code=1344
our_cols=['dma_code','retailer_code','store_code_uc','week_end','upc','upc_ver_uc','units','price','feature','display']

read_all_data_new(read_dir,outfile,statelist=None,dmalist=dmas,module_code=module_code,channel_filter=['F'],cols=our_cols)