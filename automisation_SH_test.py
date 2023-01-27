#adding information needed

import pandas as pd
import sqlalchemy as sql

#name convention and calling wantet column as amount 

def rename_df(df_name, amount):
    df_name.columns = df_name.columns.str.replace(" ", "_")
    df_name.columns = df_name.columns.str.lower()
    df_name.rename(columns={amount:"amount"}, inplace=True)
    return df_name

#adding state_id, year and amountx1000

def add_numbers(df_name, state_id, year):
    df_name.insert(len(df_name.columns), "year", year )
    df_name.insert(len(df_name.columns), "state_id", state_id)
    df_name.amount = df_name['amount'].str.replace('.', '').str.replace(",", ".").astype(float)
    df_name.amount = df_name['amount'].apply(lambda x:int(x*1000))
    return df_name

#making stuff to string, fill leading 0 and create gruppe and counter out of titel

def string_and_fill(df_name, to_string1, to_string2, to_string3):
    df_name[to_string1] = df_name[to_string1].astype(str)
    df_name[to_string2] = df_name[to_string2].astype(str)
    df_name[to_string3] = df_name[to_string3].astype(str)

    df_name[to_string1] = df_name[to_string1].apply(lambda x:x.zfill(2))
    df_name[to_string2] = df_name[to_string2].str[-2:]
    df_name[to_string2] = df_name[to_string2].apply(lambda x:x.zfill(3))
    df_name["gruppe"] = df_name["titel"].str[:3]
    df_name["counter"] = df_name["titel"].str[3:]
    return df_name

#drop all columns which are not needed anymore

def drop_columns(df_name, drop1, drop2, drop3):
    df_name = df_name.drop([drop1, drop2, drop3], axis=1)
    return df_name


