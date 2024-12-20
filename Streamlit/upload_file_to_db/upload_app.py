import os
import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import psycopg2
from PIL import Image
from typing import Generator


#page configuration
st.set_page_config(
    page_title="Dashboard",
    page_icon=":earth_asia:",
    layout="wide",
    initial_sidebar_state="expanded")

# Initialize connection.
db_user = "postgres"
db_password = "Postgres001"
db_host = "database-1.c1uguiucyb5i.ap-southeast-2.rds.amazonaws.com"
db_port = "5432"
db_name = "coda001_final_project"

connection_string = f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
engine = create_engine(connection_string)

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
  <center><h1 class="title" style="color:#A9A9A9;">Kumpulkan Data</h1></center>"""

creator = """<p>Created by :<br> - Amsiki Bagus R<br> - Mirza Rendra S<br> - Lusitania Ragil C<br> - Felix Giancarlo<br> - Arief Joko W</p>"""



logo, page_title = st.columns([0.15,0.85])
with logo:
  st.image(image, width=100)
with page_title:
  st.markdown(title, unsafe_allow_html=True)
st.markdown('---')

created_by, chart1, chart2 = st.columns([0.15,0.4,0.4])
with created_by:
  st.markdown(creator, unsafe_allow_html=True)
with chart1:
  uploaded_file = st.file_uploader("Choose a file")
  if uploaded_file is not None:
      dataframe = pd.read_csv(uploaded_file)
      st.write(dataframe)

_1_, upbutton, bank = st.columns([0.15,0.4,0.4])
with upbutton:
  if st.button('Upload ke Database'):
    if uploaded_file:
      dataframe.to_sql("data_raw", engine, if_exists='append', index=False)
      st.write('Upload Suksess !!!!!!!!!!!!!')
    else:
      st.write('Belum upload file bosss')
    


