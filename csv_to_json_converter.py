import pandas as pd
import json
import glob
import re
import os
import sys
import config

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

    df = pd.read_csv(file, names=column_names)
    return df

def to_json(
        df: pd.DataFrame, tgt_base_dir: str,
        ds_name: str, file_name: str
        ) -> None:
    json_file_path = f'{tgt_base_dir}/{ds_name}/{file_name}'
    os.makedirs(f"{tgt_base_dir}/{ds_name}", exist_ok=True)

    df.to_json(json_file_path, orient='records', lines=True)

def file_converter(src_base_dir: str, tgt_base_dir: str, ds_name: str) -> None:
    schemas = json.load(open(f'{src_base_dir}/schemas.json'))
    files = glob.glob(f'{src_base_dir}/{ds_name}/part-*', recursive=True)

    if len(files) == 0:
        raise NameError(f"No files found for {ds_name}")

    for file in files:
        print(f"Processing {file}")
        df = read_csv(file, schemas)
        file_name = re.split(r'/', file)[-1]
        to_json(df, tgt_base_dir, ds_name, file_name)

def process_files(ds_names = None) -> None:
    src_base_dir = config.SRC_BASE_DIR
    tgt_base_dir = config.TGT_BASE_DIR
    schemas = json.load(open(f'{src_base_dir}/schemas.json'))
    if not ds_names:
        ds_names = schemas.keys()
    
    for ds_name in ds_names:
        print(f"Processing {ds_name}")
        try:
            file_converter(src_base_dir, tgt_base_dir, ds_name)
        except NameError as ne:
            print(f"Error processing {ds_name}: {ne}")

if __name__ == "__main__":
    ds_names = None
    if len(sys.argv) > 1:
        ds_names = [x.strip() for x in sys.argv[1].split(',')]
    print(ds_names)
    process_files(ds_names)