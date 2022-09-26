from api import PlacesQuery
import datetime as dt
import numpy as np
import streamlit as st
from itertools import chain
import pandas as pd
from typing import Union, Optional, Sequence
from itertools import product


def get_country_count(company_name: str, res: pd.DataFrame, selected_types: Sequence[str]) -> pd.Series:
    res = res.explode("types")
    # st.text(f"""res.loc[{filter}]""")
    filtered = res.loc[res.types.isin(selected_types)]#eval(fr"""res.loc[({filter})]""")
    summary = filtered.groupby("country").count()\
        .place_id\
        .sort_values(ascending=False)
    summary.name = "address count"
    # st.write(list(product([company_name], 
    #     summary.index.values)))
    summary.index = pd.MultiIndex.from_tuples(product([company_name], 
        summary.index.values))
    return summary


##########################
### single comp search ###
##########################
st.title("Geolocation extractor - power by google map")
st.text("Author: Paul Peng\nCopyright 2022\nUse it wisely!")
st.header("Single Company Search")

company_name = st.text_input(label="Please enter the company you would like to search")
limit = st.number_input(label="please enter the upper limit of places you can accept")
apikey = st.text_input(label="please enter your apikey", )
res = PlacesQuery.search_place(apikey=apikey, keyword=company_name, limit=limit)
types = np.unique(list(chain(*res.types.values)))
selected_types = st.multiselect(label="select the type of locations", options=types)
summary = get_country_count(company_name, res, selected_types)
# res = res.explode("types")
# # st.text(f"""res.loc[{filter}]""")
# filtered = res.loc[res.types.isin(selected_types)]#eval(fr"""res.loc[({filter})]""")
# summary = filtered.groupby("country").count()\
#     .place_id\
#     .sort_values(ascending=False)
# summary.name = "address count"
st.write(summary)

######################
### batched search ###
######################
st.header("Batched Company Search")

company_list_str = st.text_input(label="Please enter the companies you would like to search, and seperate them with comma.")
limit_batch = st.number_input(label="please enter the upper limit of places you can accept",
    key=1)
# apikey = st.text_input(label="please enter your apikey", )

company_list = company_list_str.split(",")
st.write(company_list)
res_dict = dict(zip(company_list, 
    PlacesQuery.batch_place_lookup(apikey=apikey, 
        keywords=company_list,
        limit=limit_batch,
    )))

types = np.unique(list(
    chain(*[
        chain(*res.types.values) 
        for res in res_dict.values()])))
selected_types = st.multiselect(label="select the type of locations", 
    options=types)

summary = pd.concat(get_country_count(co_name, res, selected_types).to_frame() 
    for co_name, res in res_dict.items())
summary = summary.T.stack(1).T

st.write(summary)