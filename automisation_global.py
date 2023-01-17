import pandas as pd
import sqlalchemy as sql



#insert dataframe by pd.read_csv() or pd.read_xls()

#renames ur df and prepares it for further use
def rename_df(df, amount):
    df.rename(columns={amount:"amount"}, inplace=True)
    df.columns = df.columns.str.replace(" ", "_")
    df.columns = df.columns.str.lower()
    df.rename(columns = {'bezeichnung':'ep_txt', 'bezeichnung.1':'kapitel_txt', 'bezeichnung_fz' : 'fkz_txt', "fz":"fkz",  #brb
                                'einzelplan' : 'ep', 'titelbezeichnung' : 'zweckbestimmung', 'kapitelbezeichnung' : 'kapitel_txt', 'einzelplanbezeichnung' : 'ep_txt', 'gruppenbezeichnung' : 'gruppe_txt', 'funktionsbezeichnung' : 'fkz_txt', #berlin
                                'zählnummer':'counter', 'funktion':'fkz', #nrw 
                                'einzel-plan':'ep', 'Bezeichnung Funktionenkennzahl':'fkz_txt', #Sachsen
                                'fz':'fkz'}, inplace = True) #sh
    return df

#adds gruppe and counter and alters data types. Adds leading 0s and adds 
def augment_stuff(df_name):
    df_name['ep'] = df_name['ep'].astype(str)
    df_name['ep'] = df_name['ep'].apply(lambda x:x.zfill(2))
    df_name['kapitel'] = df_name['kapitel'].astype(str)
    df_name['kapitel'] = df_name['kapitel'].str[-3:]
    df_name['titel'] = df_name['titel'].astype(str)
    df_name['titel'] = df_name['titel'].apply(lambda x:x.zfill(5))
    df_name['fkz'] = df_name['fkz'].astype(str)
    df_name['fkz'] = df_name['fkz'].apply(lambda x:x.zfill(3))
    df_name["gruppe"] = df_name["titel"].str[:3]
    df_name["counter"] = df_name["titel"].str[3:]
    df_name['amount'] =df_name[['amount', 'gruppe']].apply(lambda x:-x[0] if int(x[1]) > 400 else x[0], axis=1)
    return df_name

#drops NaN in amount. Changes usage of . and , in amount. Adds year and state_id. Choose the first for files, which dont need x1000 in amount.
def add_numbers(df_name, state_id, year):
    df_name.dropna(subset=['amount'],inplace=True)
    df_name['amount'] = df_name['amount'].astype(str)
    df_name.insert(len(df_name.columns), "year", year )
    df_name.insert(len(df_name.columns), "state_id", state_id)
    df_name.amount = df_name['amount'].str.replace('.', '').str.replace(",", ".").astype(float)
    df_name['amount'] = df_name['amount'].astype(int)
    return df_name

def add_numbers_times1000(df_name, state_id, year):
    df_name.dropna(subset=['amount'],inplace=True)
    df_name['amount'] = df_name['amount'].astype(str)
    df_name.insert(len(df_name.columns), "year", year )
    df_name.insert(len(df_name.columns), "state_id", state_id)
    df_name.amount = df_name['amount'].str.replace('.', '').str.replace(",", ".").astype(float)
    df_name.amount = df_name['amount'].apply(lambda x:int(x*1000))
    return df_name

#splits df into df_kapitel, df_einzelplaene, df_budget  --> those are ready for upload

def split_into_dfs(df):
    df_einzelplaene = df[['ep','ep_txt', 'state_id', 'year']].copy()
    df_kapitel = df[['ep', 'kapitel', 'kapitel_txt', 'state_id', 'year']].copy()
    df_budget = df[['ep', 'kapitel', 'gruppe', 'counter', 'state_id', 'year', 'fkz', 'amount', 'zweckbestimmung']].copy()
    return [df_kapitel, df_einzelplaene, df_budget]


#use get_sql_config(), get_engine() and push_to_database() afterwards. Dont upload the df u entered into this function!


#Dinge für später:
# Zudem vielleicht doch noch state statt state_id ermöglichen.