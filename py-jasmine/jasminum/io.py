import glob
import os
from pathlib import Path

import polars as pl

from .constant import PL_DATA_TYPE
from .exceptions import JasmineEvalException
from .j import J, JType


# write partition df
def wpart(
    hdb_path: J, partition: J, table: J, df: J, sort_series: J, rechunk: J, overwrite: J
) -> J:
    base_path = Path(hdb_path.to_str())
    partition.assert_types([JType.INT, JType.DATE, JType.NULL])
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


# read parquet
def rparquet(source: J, n_rows: J) -> J:
    if source.j_type == JType.CAT or source.j_type == JType.STRING:
        source_path = source.to_str()
    else:
        source_path = source.to_strs()
    if n_rows.j_type is None:
        return J(pl.read_parquet(source_path))
    else:
        n = n_rows.int()
        return J(pl.read_parquet(source_path, n))


# write parquet
def wparquet(data: J, file: J, level: J) -> J:
    filepath = file.to_str()
    df = data.to_df()
    compression_level = level.int()
    if compression_level < 1 or compression_level > 22:
        raise JasmineEvalException(
            "compression min-level: 1, max-level: 22, got %s", compression_level
        )
    df.write_parquet(filepath, compression_level=compression_level)
    return file


def rcsv(source: J, has_header: J, sep: J, ignore_errors: J, dtypes: J) -> J:
    if source.j_type == JType.CAT or source.j_type == JType.STRING:
        source_path = source.to_str()
    else:
        source_path = source.to_strs()
    args = {
        "has_header": has_header.to_bool(),
        "separator": sep.to_str(),
        "ignore_errors": ignore_errors.to_bool(),
        "try_parse_dates": True,
    }
    dtype_dict = {}
    if dtypes.j_type == JType.DICT:
        dtype_dict = dtypes.data
        for k, v in dtype_dict.items():
            dtype = v.to_str()
            if dtype not in PL_DATA_TYPE:
                raise JasmineEvalException("unrecognized data type name '%s'" % dtype)
            dtype_dict[k] = PL_DATA_TYPE[v.to_str()]
    if len(dtype_dict) == 0:
        return pl.read_csv(source_path, **args)
    else:
        return pl.read_csv(
            source_path,
            columns=list(dtype_dict.keys()),
            schema_overrides=dtype_dict,
            **args,
        )


def wcsv(data: J, file: J, sep: J) -> J:
    filepath = file.to_str()
    df = data.to_df()
    separator = sep.to_str()
    df.write_csv(filepath, separator=separator)
    return file
