import datetime as dt 
import pandas as pd
import numpy as np
from typing import Union, Optional, Any, Dict, List
from copy import deepcopy
import itertools


def pandas_strptime(df: pd.DataFrame, 
    index_name: Optional[Union[str, List[str]]]=None,
    index_iloc: Optional[Union[int, List[str]]]=None,
    axis: Union[str, int]=0,
    datetime_format: str ="%Y-%m-%d",
    inplace: bool=False):
    """converts str datetime to np.datetime64
    :param index_name: index or column name to be processed
    :param index_iloc: positional index of the row/column to be processed
    :param axis: takes either 0/1, or 'index'/'columns'
    :param datetime_format: datetime.strptime format
    :param inplace: False by default, will create a deepcopy of the original 
        frame. Otherwise will changed the original frame inplace
    """
    assert index_name or index_iloc, 'index_name and index_iloc cannot be both unspecified'
    axes = {'index': 0, 'columns': 1}
    if isinstance(axis, str):
        axis = axes.get(axis)
    if inplace:
        if index_name:
            if isinstance(index_name, str):
                if axis:
                    df.loc[:, index_name] = df.loc[:, index_name]\
                        .apply(lambda x: dt.datetime.strptime(x, datetime_format))
                else:
                    df.loc[index_name, :] = df.loc[index_name, :]\
                        .apply(lambda x: dt.datetime.strptime(x, datetime_format))
            elif isinstance(index_name, list):
                if axis:
                    for ind, s in df.loc[:, index_name].iteritems():
                        df.loc[:, ind] = s.apply(lambda x: dt.datetime.strptime(x, datetime_format))
                else:
                    for ind, s in df.loc[index_name, :].iterrows():
                        df.loc[ind, :] = s.apply(lambda x: dt.datetime.strptime(x, datetime_format))

        else:   
            if isinstance(index_iloc, int):
                if axis:
                    df.iloc[:, index_iloc] = df.iloc[:, index_iloc]\
                        .apply(lambda x: dt.datetime.strptime(x, datetime_format))
                else:
                    df.iloc[index_iloc, :] = df.iloc[index_iloc, :]\
                        .apply(lambda x: dt.datetime.strptime(x, datetime_format))
            elif isinstance(index_iloc, list):
                if axis:
                    for ind, s in df.iloc[:, index_iloc].iteritems():
                        df.loc[:, ind] = s.apply(lambda x: dt.datetime.strptime(x, datetime_format))
                else:
                    for ind, s in df.iloc[index_iloc, :].iterrows():
                        df.loc[ind, :] = s.apply(lambda x: dt.datetime.strptime(x, datetime_format))
        return df

    else:
        newdf = deepcopy(df)
        if index_name:
            if isinstance(index_name, str):
                if axis:
                    newdf.loc[:, index_name] = newdf.loc[:, index_name]\
                        .apply(lambda x: dt.datetime.strptime(x, datetime_format))
                else:
                    newdf.loc[index_name, :] = newdf.loc[index_name, :]\
                        .apply(lambda x: dt.datetime.strptime(x, datetime_format))
            elif isinstance(index_name, list):
                if axis:
                    for ind, s in newdf.loc[:, index_name].iteritems():
                        newdf.loc[:, ind] = s.apply(lambda x: dt.datetime.strptime(x, datetime_format))
                else:
                    for ind, s in df.loc[index_name, :].iterrows():
                        newdf.loc[ind, :] = s.apply(lambda x: dt.datetime.strptime(x, datetime_format))

        else:   
            if isinstance(index_iloc, int):
                if axis:
                    newdf.iloc[:, index_iloc] = newdf.iloc[:, index_iloc]\
                        .apply(lambda x: dt.datetime.strptime(x, datetime_format))
                else:
                    newdf.iloc[index_iloc, :] = newdf.iloc[index_iloc, :]\
                        .apply(lambda x: dt.datetime.strptime(x, datetime_format))
            elif isinstance(index_iloc, list):
                if axis:
                    for ind, s in df.iloc[:, index_iloc].iteritems():
                        newdf.loc[:, ind] = s.apply(lambda x: dt.datetime.strptime(x, datetime_format))
                else:
                    for ind, s in df.iloc[index_iloc, :].iterrows():
                        newdf.loc[ind, :] = s.apply(lambda x: dt.datetime.strptime(x, datetime_format))
      
        return newdf


def iter_by_chunk(iterable: Any, chunk_size: int):
    """iterate by chunk size"""
    it = iter(iterable)
    while True:
        chunk = tuple(itertools.islice(it, chunk_size))
        if not chunk:
            break
        yield chunk