# %%
import polars as pl
import pyarrow.parquet as pq
# %%
county = pl.read_csv("https://github.com/kjhealy/fips-codes/raw/master/state_and_county_fips_master.csv")\
    .filter(pl.col("state") != "NA").drop("state")
other = pl.read_csv("https://github.com/kjhealy/fips-codes/raw/master/county_fips_master.csv",
        use_pyarrow=True)\
    .select("fips", "state_name", "region", "division", "state", "crosswalk", "region_name", "division_name")
# %%

dat = other.join(county, on=["fips"], how="left")\
    .select(
        "fips", 
        "name", 
        "state_name", 
        "region_name", 
        "division_name", 
        "state", 
        "region", 
        "division")

# Put the code to add the needed columns here
#%%
ourdat = pl.read_csv("Educationdata.csv")
# The URL that we took this from is: 
# https://www.ers.usda.gov/data-products/county-level-data-sets/county-level-data-sets-download-data/
dat = dat\
    .join(ourdat, right_on = "FIPS", left_on="fips", how = "left")\
    .select(
        'fips',
        '2003 Rural-urban Continuum Code',
        '2003 Urban Influence Code',
        '2013 Rural-urban Continuum Code',
        '2013 Urban Influence Code',
        'Less than a high school diploma, 2017-21',
        'High school diploma only, 2017-21',
        "Some college or associate's degree, 2017-21",
        "Bachelor's degree or higher, 2017-21",
        'Percent of adults with less than a high school diploma, 2017-21',
        'Percent of adults with a high school diploma only, 2017-21',
        "Percent of adults completing some college or associate's degree, 2017-21",
        "Percent of adults with a bachelor's degree or higher, 2017-21"
    )

# %%
# dat.shape
# (3146, 8) shape

lslice = 500
values = list(range(0, dat.shape[0], lslice))

previous = 0
for i in values:
    print(str(i))
    dat.slice(i, lslice)\
        .write_parquet("county_meta/education_and_urban-rural_" + str(i) +".parquet")
    


# %%
# read in.
df = pl.read_parquet("county_meta/*")
df.shape

#%%

df.filter(pl.col("2003 Rural-urban Continuum Code").is_null()).select(pl.col("fips"))
