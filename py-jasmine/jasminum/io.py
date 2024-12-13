import glob
import os
from pathlib import Path

import polars as pl

from .exceptions import JasmineEvalException
from .j import J, JType


# write partition df
def wpart(
    hdb_path: J, partition: J, table: J, df: J, sort_series: J, rechunk: J, overwrite: J
) -> J:
    base_path = Path(hdb_path.to_str())
    partition.assert_types([JType.INT, JType.DATE, JType.NONE])
    partition = (
        partition.date_num() if partition.j_type == JType.DATE else partition.data
    )
    sort_series = sort_series.to_strs()
    rechunk = rechunk.to_bool()

    table_path = base_path.joinpath(table.to_str())
    if table_path.is_file() and partition is not None:
        raise JasmineEvalException(
            "single file exists, not allow partition '%s'", partition
        )
    if partition:
        part_pattern = str(table_path) + str(partition) + "_*"
        if overwrite:
            table_path.mkdir(parents=True, exist_ok=True)
            part_path = table_path.joinpath(str(partition) + "_0000")
            for filepath in glob.glob(part_pattern):
                os.remove(filepath)
            df.to_df().sort(sort_series).write_parquet(part_path)
            return J(str(part_path), JType.STRING)
        else:
            max_file_num = -1
            table_path.mkdir(parents=True, exist_ok=True)
            for filepath in glob.glob(part_pattern):
                sub = int(filepath[-4:])
                if sub > max_file_num:
                    max_file_num = sub
            part_path = table_path.joinpath(str(partition) + "_%04d" % max_file_num + 1)
            df.to_df().sort(sort_series).write_parquet(part_path)
            if rechunk and max_file_num > -1:
                tmp_path = table_path.joinpath(str(partition) + "tmp")
                pl.scan_parquet(part_pattern).sort(sort_series).sink_parquet(tmp_path)
                for filepath in glob.glob(part_pattern):
                    os.remove(filepath)
                target_path = table_path.joinpath(str(partition) + "_0000")
                os.rename(tmp_path, target_path)
                return J(str(target_path), JType.STRING)
            else:
                return J(str(part_path), JType.STRING)
    else:
        base_path.mkdir(parents=True, exist_ok=True)
        df.to_df().sort(sort_series).write_parquet(table_path)
        return J(str(table_path))
