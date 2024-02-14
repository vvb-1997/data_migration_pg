import pandas as pd
import json
import glob
import re
import os
import sys
import config
import multiprocessing as mp

def get_column_names(
        schemas: dict, ds_name: str, sorting_key: str='column_position'
        ) -> list():
    column_details = schemas[ds_name]
    columns = sorted(column_details, key=lambda col: col[sorting_key])
    return [col['column_name'] for col in columns]

def read_csv(
        file: str, schemas: dict
        ) -> pd.DataFrame:
    file_path_list = re.split(r'/', file)
    ds_name = file_path_list[-2]
    column_names = get_column_names(schemas, ds_name)

    df_reader = pd.read_csv(file, names=column_names, chunksize=10000)
    return df_reader

def to_sql(
        df: pd.DataFrame, db_conn_uri: str,
        ds_name: str
    ) -> None:
    df.to_sql(ds_name, db_conn_uri, if_exists='replace', index=False)

def db_loader(
        src_base_dir: str, db_conn_uri: str,
        ds_name:str
    ) -> None:
    schemas = json.load(open(f'{src_base_dir}/schemas.json'))
    files = glob.glob(f'{src_base_dir}/{ds_name}/part-*', recursive=True)
    
    if len(files) == 0:
        raise NameError(f"No files found for {ds_name}")

    for file in files:
        print(f"Processing {file}")
        df_reader = read_csv(file, schemas)
        for idx,df in enumerate(df_reader):
            print(f"Populating chunck {idx} of {ds_name}")
            to_sql(df, db_conn_uri, ds_name)

def process_dataset(
        ds_name: str, src_base_dir: str, db_conn_uri: str
    ) -> None:
    print(f"Processing {ds_name}")
    try:
        db_loader(src_base_dir, db_conn_uri, ds_name)
    except NameError as ne:
        print(f"Error processing {ds_name}: {ne}")
    except Exception as e:
        print(f"Error Encountered {ds_name}: {e}")
    finally:
        print(f"Completed processing for {ds_name}")

def process_files(ds_names = None) -> None:
    src_base_dir = config.SRC_BASE_DIR
    db_host = config.DB_HOST
    db_port = config.DB_PORT
    db_name = config.DB_NAME
    db_user = config.DB_USER
    db_pass = config.DB_PASS
    
    db_conn_uri = f'postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}'
    schemas = json.load(open(f'{src_base_dir}/schemas.json'))
    
    if not ds_names:
        ds_names = schemas.keys()
    
    processes = min(4, len(ds_names))
    pool = mp.Pool(processes)
    
    # synchronous
    # for ds_name in ds_names:
    #     process_dataset(ds_name, src_base_dir, db_conn_uri)
    
    # asynchronous
    pool.starmap(
        process_dataset,
        [(ds_name, src_base_dir, db_conn_uri,) for ds_name in ds_names]
    )

if __name__ == "__main__":
    ds_names = None
    if len(sys.argv) > 1:
        ds_names = [x.strip() for x in sys.argv[1].split(',')]
    print(ds_names)
    process_files(ds_names)