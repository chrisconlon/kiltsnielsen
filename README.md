# kiltsreader

A fast Python package for reading [NielsenIQ data][kilts] from the Kilts Center and saving it as `.parquet` files.

- **RetailReader** — Retail Scanner Data (store-level weekly sales)
- **PanelReader** — Consumer Panel Data (household purchases)

Built on [PyArrow][apache] (`>= 17.0`). All data is returned as PyArrow Tables for speed and memory efficiency. Convert to pandas anytime with `.to_pandas()`.

## Installation

```
pip install git+https://github.com/chrisconlon/kiltsnielsen
```

Requires Python >= 3.9. Installs `pyarrow`, `pandas`, and `numpy` automatically.

## Data Setup

Get access through your institution at the [Kilts Center][kilts], then download:

- **Scanner data:** Build extracts via the Kilts File Selection System. Available as `.tgz` by group/module/year.
- **Panel data:** Download from Globus. Available as `.tgz` by year.

You can either extract the `.tgz` files or use them directly. If extracting, preserve the original directory structure.

> **Performance tip:** For large scanner archives (5GB+), extracting is **~100x faster** than reading from `.tgz`. Panel archives (~500MB each) show little difference. Extract for repeated use; use `.tgz` directly for one-off reads.

## Quick Start

#### Retail Scanner Data

```python
from kiltsreader import RetailReader
from pathlib import Path

rr = RetailReader(Path('/path/to/scanner/data'))
rr.filter_years(drop=[2006, 2019])
rr.read_stores()
rr.filter_stores(keep_dmas=[506, 517], keep_channels=['F'])
rr.read_products(keep_modules=[1344])
rr.read_sales()
rr.write_data(Path('output'), stub='cereal')
```

#### Consumer Panel Data

```python
from kiltsreader import PanelReader
from pathlib import Path

pr = PanelReader(Path('/path/to/panel/data'))
pr.filter_years(keep=range(2007, 2014))
pr.read_retailers()
pr.read_products(keep_groups=[5002])
pr.read_annual(keep_states=['CT'])
pr.write_data(Path('output'), stub='ct_liquor')
```

See [Example.py](kiltsreader/Example.py) for a more detailed walkthrough, and the [API Guide](API_GUIDE.md) for full method documentation.

[apache]: <https://arrow.apache.org>
[kilts]: <https://www.chicagobooth.edu/research/kilts/datasets/nielsenIQ-nielsen>
