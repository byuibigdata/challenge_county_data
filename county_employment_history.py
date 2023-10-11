# %%
import polars as pl
import pyarrow.parquet as pq
# %%

# Data was obtained from https://www.ers.usda.gov/data-products/county-level-data-sets/county-level-data-sets-download-data/

# %%
us_counties = pl.read_csv("../COUNTY_EMPLOYMENT_DATA/US-counties-employment-info.csv")
# %%
us_counties

# %%
columns_to_remove = ["State", "Area_Name"] 
us_counties = us_counties.select([col for col in us_counties.columns if col not in columns_to_remove])
# %%
us_counties
# %%
lslice = 800
values = list(range(0, us_counties.shape[0], lslice))

previous = 0
for i in values:
    print(str(i))
    us_counties.slice(i, lslice).write_parquet("county_employment_history_meta/meta_" + str(i) +".parquet")
# %%
df = pl.read_parquet("county_employment_history_meta/*")
# %%
df.shape
# %%
df.head(20)
# %%
