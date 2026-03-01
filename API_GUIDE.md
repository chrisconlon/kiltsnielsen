# kiltsreader API Guide

Detailed method reference for **RetailReader** and **PanelReader**. For installation and quick start, see [README.md](README.md).

## RetailReader

```python
RetailReader(dir_read=Path.cwd(), verbose=True)
```

**Typical workflow:** init &rarr; `filter_years` &rarr; `read_stores` &rarr; `filter_stores` &rarr; `read_products` &rarr; `filter_sales` &rarr; `read_sales` &rarr; `write_data`

### Filtering

**`filter_years(keep=None, drop=None)`**
Limit which years to process. Applies to stores, RMS, extra, and sales files.
- `keep` — list of years to keep, e.g. `range(2007, 2014)`
- `drop` — list of years to exclude, e.g. `[2006, 2019]`

**`filter_sales(keep_groups=None, drop_groups=None, keep_modules=None, drop_modules=None)`**
Limit which product groups and modules are read during `read_sales()`. Does **not** affect `read_products()`.

**`filter_stores(keep_dmas, drop_dmas, keep_states, drop_states, keep_channels, drop_channels)`**
Filter the stores table by geography and channel type. Must call `read_stores()` first.

### Reading

**`read_products(upc_list, keep_groups, drop_groups, keep_modules, drop_modules, keep_departments, drop_departments)`**
&rarr; `df_products` (PyArrow Table)

Reads `Master_Files/Latest/products.tsv`. Filters are independent of `filter_sales()` — you can read all products while only processing a subset of sales. Shared with `PanelReader.read_products()`.

**`read_stores()`**
&rarr; `df_stores` (PyArrow Table)

Reads annual store files. Columns: `store_code_uc`, `panel_year`, `parent_code`, `retailer_code`, `channel_code`, `store_zip3`, `fips_state_code`, `fips_state_descr`, `fips_county_code`, `fips_county_descr`, `dma_code`, `dma_descr`.

**`read_rms()`**
&rarr; `df_rms` (PyArrow Table)

Reads RMS version files. Maps reused UPCs to the correct version by year. Columns: `upc`, `upc_ver_uc`, `panel_year`.

**`read_extra(years=None, upc_list=None)`**
&rarr; `df_extra` (PyArrow Table)

Reads annual extra product characteristics (flavor, form, formula, container, etc.). UPCs may repeat across years.

**`read_sales(incl_promo=True, add_dates=False, agg_function=None, **kwargs)`**
&rarr; `df_sales` (PyArrow Table)

Reads weekly store x UPC sales data. Automatically joins RMS version info and store geography (`dma_code`, `retailer_code`, `parent_code`).
- `incl_promo=False` — skip `feature` and `display` columns
- `add_dates=True` — compute `month` and `quarter` from `week_end`
- `agg_function` — callable applied to each module-year table; receives a PyArrow Table plus any `**kwargs`

### Writing

**`write_data(dir_write=Path.cwd(), stub='out', compr='brotli', as_table=False, separator='panel_year')`**

Writes all non-empty datasets as `.parquet` files named `{stub}_{type}.parquet`.
- `as_table=True` — partition the output using `write_to_dataset`, partitioned by `separator`
- `compr` — compression codec (default `'brotli'`)


## PanelReader

```python
PanelReader(dir_read=Path.cwd(), verbose=True)
```

**Typical workflow:** init &rarr; `filter_years` &rarr; `read_retailers` &rarr; `read_products` &rarr; `read_annual` &rarr; `write_data`

### Filtering

**`filter_years(keep=None, drop=None)`**
Same as RetailReader.

### Reading

**`read_products(...)`**
&rarr; `df_products` (PyArrow Table)

Same parameters and behavior as `RetailReader.read_products()`. Reads the shared `products.tsv`.

**`read_retailers()`**
&rarr; `df_retailers` (PyArrow Table)

Reads `Master_Files/Latest/retailers.tsv`. Columns: `retailer_code`, `channel_type`.

**`read_variations()`**
&rarr; `df_variations` (PyArrow Table)

Reads `Master_Files/Latest/brand_variations.tsv`. Lists brand codes with alternative descriptions and date ranges.

**`read_extra(years=None, upc_list=None)`**
&rarr; `df_extra` (PyArrow Table)

Same as RetailReader.

**`read_annual(keep_states, drop_states, keep_dmas, drop_dmas, keep_stores=None, add_household=False)`**
&rarr; `df_panelists`, `df_trips`, `df_purchases` (PyArrow Tables)

Reads all annual files for the selected years. Filters households by geography, then reads only matching trips and purchases.
- `keep_states` / `drop_states` — two-letter state codes, e.g. `['CT', 'NY']`
- `keep_dmas` / `drop_dmas` — DMA codes
- `keep_stores` — list of `store_code_uc` values to filter trips
- `add_household=True` — join `household_code` onto purchases

If `read_products()` was called first, purchases are filtered to matching UPCs.

**`read_year(year, ...)`**
Single-year version of `read_annual` with the same parameters. Appends to the existing lists, which are concatenated by `read_annual`.

### Writing

**`write_data(dir_write=Path.cwd(), stub='out', compr='brotli', as_table=False, separator='panel_year')`**

Same as RetailReader. Writes panelists, trips, purchases, products, retailers, and extra as separate `.parquet` files.

### Errata

**`read_revised_panelists()`**
Applies Nielsen errata to `df_products`, `df_variations`, `df_retailers`, and `df_panelists`. Must call `read_annual()`, `read_products()`, `read_variations()`, and `read_retailers()` first.

**`process_open_issues()`**
Fixes two known issues:
- Adds missing flavor codes to 2010 extra characteristics
- Corrects male head of household birth month


## Common Filter Parameters

Most filtering methods accept `keep_*` and `drop_*` lists. When both are specified for the same dimension, `drop_*` takes precedence.

| Parameter | Type | Description |
|-----------|------|-------------|
| `keep_groups` / `drop_groups` | `list[int]` | Product group codes |
| `keep_modules` / `drop_modules` | `list[int]` | Product module codes |
| `keep_departments` / `drop_departments` | `list[int]` | Department codes |
| `keep_states` / `drop_states` | `list[str]` | Two-letter state codes |
| `keep_dmas` / `drop_dmas` | `list[int]` | DMA codes |
| `keep_channels` / `drop_channels` | `list[str]` | Store channel types (`'F'` = grocery) |
| `upc_list` | `list[int]` | Specific UPC codes |
