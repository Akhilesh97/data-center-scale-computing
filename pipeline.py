# -*- coding: utf-8 -*-
"""
Created on Fri Sep  8 13:23:32 2023

@author: Akhilesh
"""

import pandas as pd
pd.options.mode.chained_assignment = None
import sys

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
    df_sample = df.sample(frac = 0.08)
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
    df_sample["DateTime"] = pd.to_datetime(df_sample["DateTime"])
    df_sample["Date of Birth"] = pd.to_datetime(df_sample["Date of Birth"])
    df_sample[["Month", "Year"]] = df_sample["MonthYear"].str.split(" ", expand=True)
    df_sample.drop(columns=["MonthYear"], inplace = True)
    return df_sample

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
                print(k)
            li.append(df_outcome_type)
    
    final_df = pd.concat(li, axis = 0)
    final_df["Outcome Subtype"].fillna("Unknown", inplace = True)
    return final_df

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
    df_sample_s1 = clean_name(df_sample)
    print("Done.")
    print("Cleaning column - 'date")
    df_sample_s2 = clean_date(df_sample_s1)
    print("Done.")
    print("Cleaning column - 'type' and 'sub type'")
    df_sample_s3 = clean_types(df_sample_s2)
    print("Done.")
    return df_sample_s3

def write_data(final_df, arg2):
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
    final_df.to_csv(arg2)
    
def main():
    
    if len(sys.argv) > 1:
        arg1 = sys.argv[1]
        arg2 = sys.argv[2]
        df_sample = read_file(arg1)
        final_df = transform_data(df_sample)
        write_data(final_df, arg2)
                
    else:
        print("Please provide the required number of arguments")
        print("Hint: arg1 should be input file path such as: https://data.austintexas.gov/api/views/9t4d-g238/rows.csv and arg2 should be output file path such as processed.csv")
    
if __name__ == '__main__' :
    main()    
