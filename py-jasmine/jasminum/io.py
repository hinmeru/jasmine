import glob
import os
from datetime import datetime
from pathlib import Path

import polars as pl

from .constant import PL_DATA_TYPE
from .exceptions import JasmineEvalException
from .j import J, JType
from .j_conn import JConn


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
def rparquet(source: J, n_rows: J, include_file_paths: J) -> J:
    if source.j_type == JType.CAT or source.j_type == JType.STRING:
        source_path = source.to_str()
    else:
        source_path = source.to_strs()
    path_column = None
    if include_file_paths.to_bool():
        path_column = "source_file"
    if n_rows.j_type is None:
        return J(pl.scan_parquet(source_path, include_file_paths=path_column).collect())
    else:
        n = n_rows.int()
        return J(
            pl.scan_parquet(
                source_path, n_rows=n, include_file_paths=include_file_paths
            ).collect()
        )


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


def rcsv(
    source: J,
    has_header: J,
    sep: J,
    ignore_errors: J,
    dtypes: J,
    include_file_paths: J,
) -> J:
    if source.j_type == JType.CAT or source.j_type == JType.STRING:
        source_path = source.to_str()
    else:
        source_path = source.to_strs()
    dtype_dict = {}
    if dtypes.j_type == JType.DICT:
        dtype_dict = dtypes.data
        for k, v in dtype_dict.items():
            dtype = v.to_str()
            if dtype not in PL_DATA_TYPE:
                raise JasmineEvalException("unrecognized data type name '%s'" % dtype)
            dtype_dict[k] = PL_DATA_TYPE[v.to_str()]
    path_column = None
    if include_file_paths.to_bool():
        path_column = "source_file"
    args = {
        "has_header": has_header.to_bool(),
        "separator": sep.to_str(),
        "ignore_errors": ignore_errors.to_bool(),
        "try_parse_dates": True,
        "include_file_paths": path_column,
    }
    if len(dtype_dict) == 0:
        return J(pl.scan_csv(source_path, **args).collect())
    else:
        return J(
            pl.scan_csv(
                source_path,
                schema_overrides=dtype_dict,
                **args,
            )
            .select(list(dtype_dict.keys()))
            .collect()
        )


def wcsv(data: J, file: J, sep: J) -> J:
    filepath = file.to_str()
    df = data.to_df()
    separator = sep.to_str()
    df.write_csv(filepath, separator=separator)
    return file


def ls(pathname: J) -> J:
    files = glob.glob(pathname.to_str(), recursive=True)
    mod_times = []
    sizes = []
    for file in files:
        mod_times.append(datetime.fromtimestamp(os.path.getmtime(file)))
        sizes.append(os.path.getsize(file))
    return J(
        pl.DataFrame(
            [
                pl.Series("filepath", files),
                pl.Series("mtime", mod_times, pl.Datetime("ms")),
                pl.Series("size", sizes),
            ]
        )
    )


def rm(pathname: J) -> J:
    if pathname.j_type == JType.STRING or pathname.j_type == JType.CAT:
        files = glob.glob(pathname.to_str(), recursive=True)
    else:
        files = pathname.to_strs()
    removed_files = []
    for file in files:
        try:
            os.remove(file)
            removed_files.append(file)
        except OSError as e:
            print(f"Error removing {file}: {e}")
    return J(
        pl.Series("removed_filepath", removed_files),
    )


def hopen(url: J) -> J:
    pass


def hclose(handle: J) -> J:
    pass


def hsync(handle: J, data: J) -> J:
    pass


def hasync(handle: J, data: J) -> J:
    pass
