from sqlalchemy import create_engine, text
import pandas as pd

def extract_data():
    '''
    Function to extract data from the database into a pandas DataFrame.
    '''
    # Define the connection parameters
    host = "database-1.c1uguiucyb5i.ap-southeast-2.rds.amazonaws.com"
    database = "coda001_final_project"
    user = "postgres"
    password = "Postgres001"

    # Create SQLAlchemy engine
    engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}/{database}")
    
    select_query = "SELECT * FROM data_raw_fp;"
    
    try:
        # Create the connection and execute the query
        with engine.connect() as connection:
            print("Running SELECT query...")
            result = connection.execute(text(select_query))
            
            # Convert the result into a pandas DataFrame
            df = pd.DataFrame(result.fetchall(), columns=result.keys())

            return df
    except Exception as e:
        print("An error occurred during SELECT:", e)
        return None

if __name__ == "__main__":
    extract_data()