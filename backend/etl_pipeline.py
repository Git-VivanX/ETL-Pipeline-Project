import pandas as pd
import requests
import yaml
import os
import logging
import json
import time

def setup_logging():
    logging.basicConfig(
        filename="etl.log",
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s"
    )

def extract(cfg):
    typ = cfg['type']
    src = cfg['source']
    retries = cfg.get('retry_count', 3)
    retry_delay = cfg.get('retry_delay', 1)
    attempt = 0
    while attempt < retries:
        try:
            if typ == "csv":
                df = pd.read_csv(src, low_memory=False)
            elif typ == "json":
                with open(src, 'r', encoding='utf-8') as f:
                    obj = json.load(f)
                    if isinstance(obj, list):
                        df = pd.DataFrame(obj)
                    elif isinstance(obj, dict):
                        # Find the first list-of-dicts key
                        main_key = None
                        for k, v in obj.items():
                            if isinstance(v, list) and len(v) > 0 and isinstance(v[0], dict):
                                main_key = k
                                break
                        if main_key:
                            df = pd.json_normalize(obj[main_key])
                        else:
                            df = pd.json_normalize(obj)
                    else:
                        raise ValueError("Unsupported JSON structure")
            elif typ == "api":
                res = requests.get(src)
                res.raise_for_status()
                df = pd.json_normalize(res.json())
            else:
                raise ValueError(f"Unknown extract type: {typ}")
            logging.info(f"Extracted {typ} data successfully on attempt {attempt+1}")
            print(f"[ETL DEBUG] Extracted shape: {df.shape}")
            return df
        except Exception as e:
            print(f"[ETL DEBUG] Extract error on attempt {attempt+1}: {e}")
            logging.warning(f"Extract failed ({attempt+1}/{retries}): {e}")
            attempt += 1
            if attempt < retries:
                time.sleep(retry_delay)
    logging.error("Extraction failed after max retries.")
    raise RuntimeError(f"Extraction failed after {retries} attempts.")

def flatten_value(val, parent_key="", sep="_"):
    items = []
    if isinstance(val, dict):
        for k, v in val.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            items.extend(flatten_value(v, new_key, sep=sep).items())
    elif isinstance(val, list):
        if all(isinstance(i, dict) for i in val):
            for idx, e in enumerate(val):
                new_key = f"{parent_key}{sep}{idx}" if parent_key else f"{idx}"
                items.extend(flatten_value(e, new_key, sep=sep).items())
        else:
            aggregated = ",".join(str(i) for i in val)
            items.append((parent_key, aggregated))
    else:
        items.append((parent_key, val))
    return dict(items)

def flatten_dataframe(df):
    records = df.to_dict(orient="records")
    flattened_records = []
    for rec in records:
        flat = flatten_value(rec)
        # Always include each row, even if sparse or missing fields
        if len(flat) == 0:
            flat = {"warning": "empty_record"}
        flattened_records.append(flat)
    return pd.DataFrame.from_records(flattened_records)

def is_hashable(val):
    try:
        hash(val)
        return True
    except Exception:
        return False

def detect_record_type(record):
    if 'userId' in record or 'contact' in record:
        return 'user'
    elif 'transactionId' in record or 'items' in record:
        return 'transaction'
    else:
        return 'unknown'

def transform(df, cfg):
    # Identify and tag record type for each entry (extra robustness for mixed JSON records)
    df_records = df.to_dict(orient='records')
    record_types = [detect_record_type(r) for r in df_records]
    df['record_type'] = record_types

    # Robust flatten: each record, all schema variations, no row dropped
    df = flatten_dataframe(df)

    # Deduplicate safely (hashable columns only)
    hashable_cols = [c for c in df.columns if df[c].map(is_hashable).all()]
    if cfg.get('drop_duplicates', False) and hashable_cols:
        df = df.drop_duplicates(subset=hashable_cols)
        logging.info(f"Dropped duplicates on {len(hashable_cols)} safe columns.")
    if cfg.get('dropna', False):
        # Only drop rows that are all NaN, not partly empty (keeps mixed schema)
        df = df.dropna(how="all")
        logging.info("Dropped all-NaN rows only.")

    # Add profiling columns
    try:
        df['num_columns'] = df.count(axis=1)
        df['num_nulls'] = df.isnull().sum(axis=1)
    except Exception:
        pass
    return df

def load(df, cfg):
    dest = cfg['destination']
    print(f"[ETL DEBUG] Saving output to: {dest}")
    out_dir = os.path.dirname(dest)
    if out_dir and not os.path.exists(out_dir):
        os.makedirs(out_dir)
    df.to_csv(dest, index=False)
    print(f"[ETL DEBUG] Saved {len(df)} records to {dest}")

def run_etl_pipeline():
    setup_logging()
    with open("config.yaml") as f:
        cfg = yaml.safe_load(f)
    logging.info("ETL job started.")
    print(f"[ETL DEBUG] Loaded config: {cfg}")
    df = extract(cfg['extract'])
    print(f"[ETL DEBUG] Extracted {len(df)} rows and {len(df.columns)} columns")
    df = transform(df, cfg.get('transform', {}))
    print(f"[ETL DEBUG] Transformed {len(df)} rows and {len(df.columns)} columns")
    load(df, cfg['load'])
    logging.info("ETL pipeline finished.")
    print("ETL complete. Output saved to:", cfg['load']['destination'])

if __name__ == "__main__":
    run_etl_pipeline()
