import pandas as pd
import extract_fp  # Import the extract module correctly

def transform_data():
    '''
    Function to extract and transform data.
    '''
    # Extract data
    df_raw = extract_fp.extract_data()
    df_raw = df_raw.drop_duplicates()
    df_raw.fillna(0, inplace=True)
    df_raw.drop_duplicates()

    # Make a depedency ratio fact dataframe
    df_depedency_ratio = df_raw

    df_depedency_ratio['population_<15'] = df_depedency_ratio.apply(lambda row: row['Population'] if row['Age'] == '<15' else None, axis=1)
    df_depedency_ratio['population_>60'] = df_depedency_ratio.apply(lambda row: row['Population'] if row['Age'] == '>60' else None, axis=1)
    df_depedency_ratio['population_15_59'] = df_depedency_ratio.apply(lambda row: row['Population'] if row['Age'] == '15-59' else None, axis=1)

    df_depedency_ratio = df_depedency_ratio.groupby('Country or Area').agg({
    'population_<15': 'sum',
    'population_>60': 'sum',
    'population_15_59' : 'sum'  
    }).reset_index()    

    df_depedency_ratio['df_depedency_ratio'] = (df_depedency_ratio['population_<15'] + df_depedency_ratio['population_>60']) / df_depedency_ratio['population_15_59'].replace(0, 1)*100

    df_depedency_ratio = df_depedency_ratio.rename(columns={
                "Country or Area": "country",
                "Dependency_Ratio": "dependency_ratio",
                "population_<15": "population_under_15",
                "population_>60": "population_above_60",
            })
    
    # Make a workforce ratio fact dataframe
    df_workforce_ratio = df_raw[df_raw['Age']=='15-59'].groupby(['Country or Area']).agg({
        'Value_Employed': 'sum',
        'Value_Unemployed': 'sum',
        'Population': 'sum'
    }).reset_index()

    df_workforce_ratio['LaborForceRatio'] = (df_workforce_ratio['Value_Employed'] + df_workforce_ratio['Value_Unemployed']) / df_workforce_ratio['Population'] * 100

    df_workforce_ratio = df_workforce_ratio.rename(columns={
            "Country or Area": "country",
            "Value_Employed": "employed",
            "Value_Unemployed": "unemployed",
            "Population": "population",
            "LaborForceRatio": "labor_force_ratio",
        })
    
    # Make a unemployed rate fact dataframe
    df_unemployment_ratio = df_raw[df_raw['Age']=='15-59'].groupby(['Country or Area']).agg({
    'Value_Unemployed': 'sum',
    'Population': 'sum'
    }).reset_index()

    # Menghitung rasio pengangguran
    df_unemployment_ratio['UnemploymentRatio'] = df_unemployment_ratio['Value_Unemployed'] / df_unemployment_ratio['Population']*100

    df_unemployment_ratio = df_unemployment_ratio.rename(columns={
            "Country or Area": "country",
            "Value_Unemployed": "unemployed",
            "UnemploymentRatio": "unemployed_ratio",
            "Population": "population",
        })
    
    # Rename the column of df_raw
    df_raw = df_raw.rename(columns={"Country or Area": "country"})
    df_raw = df_raw.rename(columns={"Sex": "gender"})
    df_raw = df_raw.rename(columns={"Age": "age"})
    df_raw = df_raw.rename(columns={"Value_Employed": "employed"})
    df_raw = df_raw.rename(columns={"Value_Unemployed": "unemployed"})
    df_raw = df_raw.rename(columns={"Value_Total_economically_active_population": "economically_active_population"})
    df_raw = df_raw.rename(columns={"Value_Not_economically_active_population": "not_economically_active_population"})
    df_raw = df_raw.rename(columns={"Attending": "attending_school"})
    df_raw = df_raw.rename(columns={"Not attending school": "not_attending_school"})
    df_raw = df_raw.rename(columns={"Population": "population"})

    return df_raw, df_depedency_ratio, df_unemployment_ratio, df_workforce_ratio

if __name__ == "__main__":
    transform_data()
