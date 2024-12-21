from sqlalchemy import create_engine, text
import pandas as pd
import transform_fp

def load_data():
    '''
    Function to load data into postgres.
    '''
    # Define the connection parameters
    host = "database-1.c1uguiucyb5i.ap-southeast-2.rds.amazonaws.com"
    database = "data_warehouse_fp_coda001"
    user = "postgres"
    password = "Postgres001"

    # Create SQLAlchemy engine
    engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}/{database}")

    # Declare select queries
    select_country = "SELECT DISTINCT(country) FROM data_clean_fp"
    select_gender = "SELECT DISTINCT(gender) FROM data_clean_fp"
    select_age = "SELECT DISTINCT(age) FROM data_clean_fp"

    # Transform the data (assuming the data has been cleaned and transformed)
    df_raw, df_depedency_ratio, df_unemployment_ratio, df_workforce_ratio = transform_fp.transform_data()

    # Insert cleaned df_raw into data_clean_fp table
    try:
        # Insert the dataframe into PostgreSQL
        print("Inserting data into data_clean_fp...")
        df_raw.to_sql('data_clean_fp', engine, if_exists='replace', index=False)         

    except Exception as e:
        print("An error occurred:", e)
        return None
    
    # Insert cleaned df_depedency_ratio into fact_depedency_ratio table
    try:
        # Insert the dataframe into PostgreSQL
        print("Inserting data into fact_depedency_ratio table...")
        df_depedency_ratio.to_sql('fact_depedency_ratio', engine, if_exists='replace', index=False) 
    except Exception as e:
        print("An error occurred:", e)
        return None
    
    # Insert cleaned df_unemployment_ratio into fact_unemployment_ratio table
    try:
        # Insert the dataframe into PostgreSQL
        print("Inserting data into fact_unemployment_ratio...")
        df_unemployment_ratio.to_sql('fact_unemployment_ratio', engine, if_exists='replace', index=False) 
    except Exception as e:
        print("An error occurred:", e)
        return None
    
    # Insert cleaned df_workforce_ratio into fact_workforce_ratio table
    try:
        # Insert the dataframe into PostgreSQL
        print("Inserting data into fact_workforce_ratio...")
        df_workforce_ratio.to_sql('fact_workforce_ratio', engine, if_exists='replace', index=False) 
    except Exception as e:
        print("An error occurred:", e)
        return None

    # Select data and insert into corresponding dimension tabe
    try:
        with engine.connect() as connection:

            print("Running Query...")

            result_countries = connection.execute(text(select_country))
            df_country = pd.DataFrame(result_countries.fetchall(), columns=result_countries.keys())
            df_country.to_sql('dimension_country', con=engine, if_exists='replace', index=True)

            result_gender = connection.execute(text(select_gender))
            df_gender = pd.DataFrame(result_gender.fetchall(), columns=result_gender.keys())
            df_gender.to_sql('dimension_gender', con=engine, if_exists='replace', index=True)

            result_age = connection.execute(text(select_country))
            df_age = pd.DataFrame(result_age.fetchall(), columns=result_age.keys())
            df_age.to_sql('dimension_age', con=engine, if_exists='replace', index=True)


    except Exception as e:
        print("An error occurred during SELECT:", e)

if __name__ == "__main__":
    load_data()
