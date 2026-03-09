from fastapi import FastAPI
import pandas as pd 
import geopandas as gpd 
import duckdb


duckdb.sql('''
install spatial;
load spatial;''')

duckdb.sql('''
CREATE OR REPLACE VIEW filosofi AS from  read_parquet('https://www.data.gouv.fr/api/1/datasets/r/55432374-a91d-43d0-923d-4514dc3eb951');
''')

vars = [
'ind', 'men', 'men_pauv', 'men_1ind', 'men_5ind',
'men_prop', 'men_fmp', 'ind_snv', 'men_surf', 'men_coll', 'men_mais',
'log_av45', 'log_45_70', 'log_70_90', 'log_ap90', 'log_inc', 'log_soc',
…eight) as {v}" for v in vars])


app = FastAPI()



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

