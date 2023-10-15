# -*- coding: utf-8 -*-
"""
Created on Fri Sep  8 13:23:32 2023

@author: Akhilesh
"""

import pandas as pd
pd.options.mode.chained_assignment = None
import sys
from sqlalchemy import create_engine


def read_file(arg1 = 'https://data.austintexas.gov/api/views/9t4d-g238/rows.csv'):
    '''
    

    Parameters
    ----------
    arg1 : str, optional
        DESCRIPTION. The default is 'https://data.austintexas.gov/api/views/9t4d-g238/rows.csv'.

    Returns
    -------
    df_sample : pd.Dataframe
        a sampled dataframe.

    '''
    df = pd.read_csv(arg1)
    df_sample = df
    return df_sample

def clean_name(df_sample):
    '''
    Parameters
    ----------
    df_sample : pd.Dataframe
        the sampled data.

    Returns
    -------
    df_sample : pd.Dataframe
        necessary cleaning for the column name is done and the cleaned dataframe is returned. The cleaning steps include - 
        1. Some records have IDs as Names. These are checked by a regular expression match and replaced with empty string
        2. NAs are filled with Unknown
        3. Leading and trailing white spaces are removed
    '''
    df_sample['Name'] = df_sample['Name'].str.replace(r'[^a-zA-Z0-9]', '', regex=True)
    df_sample["Name"].fillna("Unknown", inplace = True)
    df_sample["Name"] = df_sample["Name"].str.strip()
    return df_sample

def clean_date(df_sample):
    '''
    Parameters
    ----------
    df_sample : pd.DataFrame
        the cleaned dataframe after cleaning the Name column.

    Returns
    -------
    df_sample : pd.DataFrame
        necessary cleaning for the columns that are associate with a data is done and the cleaned dataframe is returned. 
        The cleaning steps include - 
        1. The DateTime column is converted to pd.datetime type
        2. The Date of Birth column is converted to pd.datetime type
        3. The MontYear column is split to Month and Year as two separate columns        
    '''
    #df_sample["DateTime"] = pd.to_datetime(df_sample["DateTime"])
    df_sample["DateTime"] = df_sample["DateTime"].str.replace(" AM", "")
    df_sample["DateTime"] = df_sample["DateTime"].str.replace(" PM", "")
    #df_sample["Date of Birth"] = pd.to_datetime(df_sample["Date of Birth"])
    df_sample[["Month", "Year"]] = df_sample["MonthYear"].str.split(" ", expand=True)
    df_sample.drop(columns=["MonthYear"], inplace = True)
    return df_sample

def weeks_in_years(num_years):
    # Number of days in a standard year
    days_in_standard_year = 365

    # Number of days in a leap year
    days_in_leap_year = 366

    # Calculate the total number of days
    total_days = num_years * days_in_standard_year

    # Add extra day for each leap year
    leap_years = num_years // 4
    total_days += leap_years

    # Calculate the number of weeks
    total_weeks = total_days / 7

    return int(total_weeks)

def months_to_weeks(months):
    weeks_per_month = 4.33
    weeks = months * weeks_per_month
    return int(weeks)

def convert_age(x):
    if "year" in x.lower() or "years" in x.lower():
        num = x.split(" ")[0]
        weeks = weeks_in_years(int(num))
    elif "month" in x.lower() or "months" in x.lower():
        num = x.split(" ")[0]
        weeks = months_to_weeks(int(num))
    elif "day" in x.lower() or "days" in x.lower():
        num = x.split(" ")[0]
        weeks = int(int(num)//7)
    else:
        num = x.split(" ")[0]
        try:
            return int(num)
        except:
            return 0    
    return abs(weeks)

def clean_types(df_sample):
    '''    
    Parameters
    ----------
    df_sample : pd.DataFrame
        the cleaned dataframe after cleaning up the date columns
        
    Returns
    -------
    final_df : pd.DataFrame
        necessary cleaning for the columns type and subtype is done and the cleaned dataframe is returned. 
        The cleaning steps include:
            1. The OutCome Type column NA values are filled with the string 'Unknown'
            2. The Outcome Subtype column NA values are filled according to the below logic
                Find the most common subtype under a outcome type and replace it with the most common subtype
    '''
    df_sample["Outcome Type"].fillna("Unknown", inplace =  True)
    li = []
    for k in df_sample["Outcome Type"].value_counts().keys(): 
        
        if k!='Unknown':
            outcome_type = k
            df_outcome_type = df_sample[df_sample["Outcome Type"] == outcome_type]        
            if len(df_outcome_type["Outcome Subtype"].mode()) > 0:
                df_outcome_type.fillna(df_outcome_type["Outcome Subtype"].mode()[0], inplace=True)
            else:
                df_outcome_type.fillna("Unkown", inplace = True)
            li.append(df_outcome_type)
    
    final_df = pd.concat(li, axis = 0)
    final_df["Outcome Subtype"].fillna("Unknown", inplace = True)
    final_df["Age upon Outcome"] = final_df["Age upon Outcome"].apply(lambda x: convert_age(x))
    final_df[['Reproductive Status', 'Sex']] = final_df['Sex upon Outcome'].str.split(' ', n=1, expand=True)
    final_df.drop(columns = ["Sex upon Outcome", "Month", "Year"], inplace = True)
    return final_df

def prepare_dim_outcome_table(df):
    dim_outcome_table = df.drop_duplicates(subset=["Outcome Type", "Outcome Subtype", "Age upon Outcome"]).reset_index(drop = True)
    dim_outcome_table["Outcome_Id"] = [10000+i for i in range(len(dim_outcome_table))]
    dim_outcome_table = dim_outcome_table[["Outcome_Id","Outcome Type", "Outcome Subtype", "Age upon Outcome"]]
    return dim_outcome_table

def prepare_dim_animal_name_table(df):
    dim_animal_name = df.drop_duplicates(subset = ["Animal ID", "Name"]).reset_index(drop = True)
    dim_animal_name["Animal_Key"] = [20000+i for i in range(len(dim_animal_name))]
    dim_animal_name = dim_animal_name[["Animal_Key", "Animal ID", "Name"]]
    return dim_animal_name

def prepare_dim_animal_char_table(df):
    dim_animal_char = df.drop_duplicates(subset = ["Animal Type", "Breed", "Color"])
    dim_animal_char["Animal_Char_Key"] = [30000+i for i in range(len(dim_animal_char))]
    dim_animal_char = dim_animal_char[["Animal_Char_Key", "Animal Type", "Breed", "Color"]]
    return dim_animal_char

def preapre_dim_reproduction_table(df):
    dim_repro_table = df.drop_duplicates(subset = ["Reproductive Status", "Sex"]).reset_index(drop = True)
    dim_repro_table["Repro_Key"] = [40000+i for i in range(len(dim_repro_table))]
    dim_repro_table = dim_repro_table[["Repro_Key", "Reproductive Status", "Sex"]]
    return dim_repro_table

def prepare_dim_date_table(df):
    df["DateTime"] = pd.to_datetime(df["DateTime"])
    dim_date = df.drop_duplicates(subset = ["DateTime"]).reset_index(drop = True)
    dim_date["DateTime_Key"] = [500000+i for i in range(len(dim_date))]
    
    
    # Separate datetime column into Month, Year, and Time
    dim_date['month'] = dim_date['DateTime'].dt.month
    dim_date['year'] = dim_date['DateTime'].dt.year
    dim_date['time'] = dim_date['DateTime'].dt.time
    dim_date = dim_date[["DateTime_Key", "DateTime", "month", "year", "time"]]
    return dim_date

def prepare_fact_table(df, dim_outcome_table, dim_animal_name, dim_animal_char, dim_repro_table, dim_date):
    fct1 = df.merge(dim_outcome_table, on = ["Outcome Type", "Outcome Subtype", "Age upon Outcome"])
    fct1.drop(columns = ["Outcome Type", "Outcome Subtype", "Age upon Outcome"], inplace = True)
    
    fct2 = fct1.merge(dim_animal_name, on = ["Animal ID", "Name"])
    fct2.drop(columns = ["Animal ID", "Name"], inplace = True)
    
    fct3 = fct2.merge(dim_animal_char, on = ["Animal Type", "Breed", "Color"])
    fct3.drop(columns = ["Animal Type", "Breed", "Color"], inplace = True)
    
    
    fct4 = fct3.merge(dim_repro_table, on = ["Reproductive Status", "Sex"])
    fct4.drop(columns = ["Reproductive Status", "Sex"], inplace = True)
    
    
    fct5 = fct4.merge(dim_date, on = ["DateTime"])
    fct5.drop(columns = ["month", "year", "time"], inplace = True)
    fct5["Animal_ID_KEY"] = ['12345'+str(100+i) for i in range(len(fct5))]
    fct5.rename(columns = {"Animal_ID_KEY":"Fact_Id"}, inplace = True)
    fct5 = fct5[["Fact_Id", "DateTime","DateTime_Key", "Outcome_Id","Animal_Key", "Animal_Char_Key", "Repro_Key"]]

    return fct5
    
def transform_data(df_sample):
    '''
    Parameters
    ----------
    df_sample : pd.DataFrame
        the sampled dataframe

    Returns
    -------
    df_sample_s3 : pd.DataFrame
        the final dataframe after performing the data cleaning steps

    '''
    print("Cleaning column - 'name'...")
    print("-"*50)
    df_sample_s1 = clean_name(df_sample)
    print("Done.")
    print("*"*100)
    print("Cleaning column - 'date")
    print("-"*50)
    df_sample_s2 = clean_date(df_sample_s1)
    print("Done.")
    print("*"*100)
    print("Cleaning column - 'type' and 'sub type'")
    print("-"*50)
    df_sample_s3 = clean_types(df_sample_s2)
    print("Done.")
    print("*"*100)
    print("Preparing Diemension Tables")
    print("-"*50)
    dim_outcome_table = prepare_dim_outcome_table(df_sample_s3)
    dim_animal_name = prepare_dim_animal_name_table(df_sample_s3)
    dim_animal_char = prepare_dim_animal_char_table(df_sample_s3)
    dim_repro_table = preapre_dim_reproduction_table(df_sample_s3)
    dim_date = prepare_dim_date_table(df_sample_s3)
    print("Done")
    print("*"*100)
    print("Preparing Fact Table")
    print("-"*50)
    fact_table = prepare_fact_table(df_sample_s3, dim_outcome_table, dim_animal_name, dim_animal_char, dim_repro_table, dim_date)
    print("Done")
    print("*"*100)
    return df_sample_s3, dim_outcome_table, dim_animal_name, dim_animal_char, dim_repro_table, dim_date, fact_table

def write_data(processed_df, dim_outcome_table, dim_animal_name, dim_animal_char, dim_repro_table, dim_date, fact_table,  arg2):
    '''    
    Parameters
    ----------
    final_df : pd.DataFrame
        DESCRIPTION.
    arg2 : str
        the output file name.
    Returns
    -------
    None.

    '''
    print("Writing to Data Warehouse...")
    print("-"*50)

    fact_cols = []
    dim_animal_name_cols = []
    dim_animal_char_cols = []
    dim_repro_cols = []
    dim_datetime_cols = []
    dim_outcome_cols = []

    for cols in fact_table.columns:
        fact_cols.append(cols.replace(" ", "_"))
    for cols in dim_animal_name.columns:
        dim_animal_name_cols.append(cols.replace(" ", "_"))
    for cols in dim_date.columns:
        dim_datetime_cols.append(cols.replace(" ", "_"))
    for cols in dim_outcome_table.columns:
        dim_outcome_cols.append(cols.replace(" ", "_"))
    for cols in dim_repro_table.columns:
        dim_repro_cols.append(cols.replace(" ", "_"))
    for cols in dim_animal_char.columns:
        dim_animal_char_cols.append(cols.replace(" ", "_"))        


    fact_table.columns = fact_cols    
    dim_animal_name.columns = dim_animal_name_cols
    dim_animal_char.columns = dim_animal_char_cols
    dim_outcome_table.columns = dim_outcome_cols
    dim_date.columns = dim_datetime_cols
    dim_repro_table.columns = dim_repro_cols

    conn = create_engine('postgresql+psycopg2://akhil97:akhil199797@db-warehouse:5432/mydatabase')


    processed_df.to_sql("processed_df", conn, if_exists = "fail", index = False)
    fact_table.to_sql("fact_table", conn, if_exists = "fail", index = False)
    dim_animal_name.to_sql("dim_animal_name", conn, if_exists = "fail", index = False)
    dim_animal_char.to_sql("dim_animal_char", conn, if_exists = "fail", index = False)
    dim_outcome_table.to_sql("dim_outcome_table", conn, if_exists = "fail", index = False)
    dim_date.to_sql("dim_date", conn, if_exists = "fail", index = False)
    dim_repro_table.to_sql("dim_repro_table", conn, if_exists = "fail", index = False)
    print("Done Writing")
    print("*"*100)

def main():
    
    if len(sys.argv) > 1:
        arg1 = sys.argv[1]
        arg2 = sys.argv[2]
        df_sample = read_file(arg1)
        processed_df, dim_outcome_table, dim_animal_name, dim_animal_char, dim_repro_table, dim_date, fact_table = transform_data(df_sample)
        write_data(processed_df, dim_outcome_table, dim_animal_name, dim_animal_char, dim_repro_table, dim_date, fact_table, arg2)
                
    else:
        print("Please provide the required number of arguments")
        print("Hint: arg1 should be input file path such as: https://data.austintexas.gov/api/views/9t4d-g238/rows.csv and arg2 should be output file path such as processed.csv")
    
if __name__ == '__main__' :
    main()    


