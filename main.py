from fastapi import FastAPI
import pandas as pd 
import duckdb
from fastapi.middleware.cors import CORSMiddleware


def init_duckdb():
    duckdb.sql('''
        install spatial;
        load spatial;
    ''')

    duckdb.execute('''SET memory_limit = '.5GB';''')

    duckdb.sql('''
        CREATE OR REPLACE TABLE filosofi AS 
        select * 
        from read_parquet('https://www.data.gouv.fr/api/1/datasets/r/55432374-a91d-43d0-923d-4514dc3eb951');
    ''')

    duckdb.sql('''
        ALTER TABLE filosofi
        ALTER COLUMN geometry TYPE GEOMETRY USING ST_GeomFromText(ST_AsText(geometry));           
        CREATE INDEX my_idx ON filosofi USING RTREE (geometry);
    ''')

    duckdb.sql('''
        CREATE OR REPLACE TABLE commune AS 
        select * 
        from read_parquet('commune_2025_carreaux_2021.parquet');
    ''')

    duckdb.sql('''
        ALTER TABLE commune
        ALTER COLUMN geometry TYPE GEOMETRY USING ST_GeomFromText(ST_AsText(geometry));          
        CREATE INDEX spatial_index ON commune USING RTREE (geometry);
    ''')

init_duckdb()


vars = ['ind', 'men', 'men_pauv', 'men_1ind', 'men_5ind', 'men_prop', 'men_fmp', 'ind_snv', 'men_surf', 'men_coll', 'men_mais', 'log_av45', 'log_45_70', 'log_70_90', 'log_ap90', 'log_inc', 'log_soc', 'ind_0_3', 'ind_4_5', 'ind_6_10', 'ind_11_17', 'ind_18_24', 'ind_25_39', 'ind_40_54', 'ind_55_64', 'ind_65_79', 'ind_80p', 'ind_inc']

agg = ",\n".join([f"COALESCE(sum({v}*weight), 0) as {v}" for v in vars])
agg_commune = ",\n".join([f"COALESCE({v},0) as {v}" for v in vars])

query = '''       
WITH intersected AS (
    SELECT *, st_area(st_intersection(geometry, {})) / st_area(geometry) as weight
    from filosofi
    where ST_Intersects(geometry, {})
)
SELECT
    'hubblo' AS unit, {}, ST_AsText({}) as geometry
FROM intersected

UNION 

select concat(code, '-', libelle)as unit , {}  , ST_AsText(geometry)  as geometry
from commune 
where   ST_Intersects(geometry, ST_centroid({}))   
'''

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

@app.get("/filosofi")
def filosofi_stats(x: float = 3756295, y: float = 2889313, radius: float = 1000):
    hubblo = f"st_buffer(ST_Point({x}, {y}), {radius})"
    return {"message": duckdb.sql(query.format(hubblo, hubblo, agg, hubblo, agg_commune, hubblo)).df()}