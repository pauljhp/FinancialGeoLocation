from api import PlacesQuery
import datetime as dt
import numpy as np
import streamlit as st

st.title("Thanks for using this geolocation extractor")
st.markdown("Author: Paul Peng\nCopyright 2022")
st.header("Singple Company Search")
company_name = st.text_input(label="Please enter the company you would like to search")
limit = st.number_input(label="please enter the upper limit of places you can accept")
apikey = st.text_input(label="please enter your apikey", )
res = PlacesQuery.search_place(apikey=apikey, keyword=company_name, limit=limit)

st.write(res)

