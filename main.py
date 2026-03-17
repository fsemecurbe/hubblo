from fastapi import FastAPI
import pandas as pd 
import duckdb
from fastapi.middleware.cors import CORSMiddleware

duckdb.sql('''
install spatial;
load spatial;''')

duckdb.execute('''SET memory_limit = '.5GB';''')





duckdb.sql('''
CREATE OR REPLACE TABLE filosofi AS 
select * , geometry as geom
from read_parquet('https://www.data.gouv.fr/api/1/datasets/r/55432374-a91d-43d0-923d-4514dc3eb951');
''')


duckdb.sql('''
CREATE INDEX my_idx ON filosofi USING RTREE (geom);
''')

vars = [
'ind', 'men', 'men_pauv', 'men_1ind', 'men_5ind',
'men_prop', 'men_fmp', 'ind_snv', 'men_surf', 'men_coll', 'men_mais',
'log_av45', 'log_45_70', 'log_70_90', 'log_ap90', 'log_inc', 'log_soc',
'ind_0_3', 'ind_4_5', 'ind_6_10', 'ind_11_17', 'ind_18_24', 'ind_25_39',
'ind_40_54', 'ind_55_64', 'ind_65_79', 'ind_80p', 'ind_inc'
]

agg = ",\n".join([f"sum({v}*weight) as {v}" for v in vars])

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)




@app.get("/filosofi")
def filosofi_stats(x: float, y: float, radius: float = 1000):
    hubblo = f"st_buffer(ST_Point({x}, {y}), {radius})"
    query = f'''
        select 'hubblo' as unit, {agg}
        from (
        SELECT *,
           st_area(st_intersection(geometry, {hubblo})) / st_area(geometry) as weight
        from filosofi
        where ST_Intersects(geometry, {hubblo})
        ) '''

    return {"message": duckdb.sql(query).df()}

