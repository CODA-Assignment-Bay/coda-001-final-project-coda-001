import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import psycopg2
from PIL import Image



#page configuration
st.set_page_config(
    page_title="Dashboard",
    page_icon=":earth_asia:",
    layout="wide",
    initial_sidebar_state="expanded")

# Initialize connection.
conn = st.connection("postgresql", type="sql")

# Perform query.
df = conn.query('SELECT * FROM fact_sales;', ttl="10m")

#setup layout
image = Image.open("src/img/logo.png")
title = """
  <style>
  .title-test {
  font-weight:bold;
  padding:5px;
  border-radius:6px;
  }
  </style>
  <center><h1 class="title" style="color:#A9A9A9;">Dashboard BBBBBBBB</h1></center>"""
creator = """<p>Created by :<br> - Amsiki Bagus R<br> - Mirza Rendra S<br> - Lusitania Ragil C<br> - Felix Giancarlo<br> - Arief Joko W</p>"""

#chart genre distribution
fig = px.bar(df, x='status_id', y='quantity')

logo, page_title = st.columns([0.1,0.9])
with logo:
  st.image(image, width=100)
with page_title:
  st.markdown(title, unsafe_allow_html=True)

created_by, chart1, chart2 = st.columns([0.1,0.45,0.45])
with created_by:
  st.markdown(creator, unsafe_allow_html=True)
with chart1:
  st.plotly_chart(fig, use_container_width=True)

