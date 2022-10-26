import os
import streamlit as st
import snowflake.connector  #upm package(snowflake-connector-python==2.7.0)
import pandas as pd
import snowflake.snowpark as snp
# Initialize connection, using st.experimental_singleton to only run once.
@st.experimental_singleton
def init_connection():
   con = snowflake.connector.connect(
       user=os.getenv("SFUSER"),
       password=os.getenv("PASSWORD"),
       account=os.getenv("ACCOUNT"),
       role=os.getenv("ROLE"),
       warehouse=os.getenv("WAREHOUSE"),
   )
   return con
def init_snowpark_session():
   connection_parameters = {
   "account": os.environ["ACCOUNT"],
   "user": os.environ["SFUSER"],
   "password": os.environ["PASSWORD"],
   "role": os.environ["ROLE"],
   "warehouse": os.environ["WAREHOUSE"],
     }
   session = snp.Session.builder.configs(connection_parameters).create()
   return session
 
# Perform query, using st.experimental_memo to only rerun when the query changes or after 10 min.
@st.experimental_memo(ttl=600)
def run_query(query):
   with conn.cursor() as cur:
       cur.execute(query)
       return cur.fetchall()
 
#Run the extract_semantic_category and apply the tags to the column
def associate_semantics(session,tblName):
   resp='Your column has been classified.'
  
   try:
       rows=session.sql("call associate_semantic_category_tags('"+tblName+"',extract_semantic_categories('"+tblName+"'));").collect()
       for r in rows:
           print(r);
   except BaseException as err:
       resp=err
   return resp
# rows = run_query("SHOW TABLES;")
conn = init_connection()
session = init_snowpark_session()
query = "CREATE OR REPLACE DATABASE STREAMLIT_HACK;"
rows = run_query(query)
 
# Present the Upload Component and process the uploaded Excel file
uploaded_file = st.file_uploader("Choose an Excel file")
if uploaded_file is not None:
   # To read file as bytes:
   #bytes_data = uploaded_file.getvalue()
   #st.write(bytes_data)
 
   # To convert to a string based IO:
   #stringio = StringIO(uploaded_file.getvalue().decode("utf-8"))
   #st.write(stringio)
 
   # To read file as string:
   #string_data = stringio.read()
   #st.write(string_data)
 
   # Can be used wherever a "file-like" object is accepted:
   dataframe = pd.read_excel(uploaded_file)
   st.write(dataframe)
   database="STREAMLIT_HACK"
   schema="PUBLIC"
   table=uploaded_file.name.split(".")[0]
# Present a button to save to snowflake.
if st.button('Save to Snowflake'):
   session.use_database(database)
   session.use_schema(schema)
   sdf = session.create_dataframe(dataframe)
   sdf.write.mode("overwrite").save_as_table(table,table_type="transient")
   st.write('Your table {table} has been created.')
else:
   st.write('Maybe upload the file again??')
 
if st.button("Extract Semantics and Apply Tags"):
  
       associate_semantics(session, database+"."+schema+"."+table)
       st.write("Classifications applied")
else:
   st.write("well that was a waste")
