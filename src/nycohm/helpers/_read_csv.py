from pathlib import Path
import pandas as pd
import logging
from src.nycohm.helpers.log_config import configure_logging
from dagster import AssetExecutionContext

configure_logging()

def _read_csv(context: AssetExecutionContext, file_path: Path) -> tuple[pd.DataFrame, Path]:
    if not file_path.exists():
        msg = f"CSV not found: {file_path}"
        context.log.error(msg)
        raise FileNotFoundError(msg)
    context.log.info(f"Reading CSV: {file_path}")
    logging.info(f"Reading CSV: {file_path}")
    return pd.read_csv(file_path), file_path

# def _read_csv(context: AssetExecutionContext, asset_name: str) -> tuple[pd.DataFrame, Path]:
#     relative_path = CSV_FILE_MAP[asset_name]
#     csv_path = Path(DATA_DIR) / relative_path
#     if not csv_path.exists():
#         msg = f"CSV not found: {csv_path}"
#         context.log.error(msg)
#         raise FileNotFoundError(msg)
#     context.log.info(f"Reading CSV: {csv_path}")
#     logging.info(f"Reading CSV: {csv_path}")
#     return pd.read_csv(csv_path), csv_path