import pandas as pd
import sqlalchemy as sql

#renames ur df and prepares it for further use
def rename_df(df, amount):
    df.rename(columns={amount:"amount"}, inplace=True)
    df.columns = df.columns.str.replace(" ", "_")
    df.columns = df.columns.str.lower()
    df.rename(columns = {'bezeichnung':'ep_txt', 'bezeichnung.1':'kapitel_txt', 'bezeichnung_fz' : 'fkz_txt', "fz":"fkz",  #brb
                                'einzelplan' : 'ep', 'titelbezeichnung' : 'zweckbestimmung', 'kapitelbezeichnung' : 'kapitel_txt', 'einzelplanbezeichnung' : 'ep_txt', 'gruppenbezeichnung' : 'gruppe_txt', 'funktionsbezeichnung' : 'fkz_txt', #berlin
                                'zählnummer':'counter', 'funktion':'fkz', #nrw 
                                'einzel-plan':'ep', 'Bezeichnung Funktionenkennzahl':'fkz_txt', #Sachsen hatte keine Ergänzungen)
                                'fz':'fkz'}, inplace = True) #sh
    return df

#adds gruppe and counter and alters data types. Adds leading 0s.
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
    return df_name

#drops NaN in amount. Changes usage of . and , in amount. Adds year and state_id. Choose the first for files, which dont need x1000 in amount.
def add_numbers(df_name, state_id, year):
    df_name.dropna(subset=['amount'],inplace=True)
    df_name['amount'] = df_name['amount'].astype(str)
    df_name.insert(len(df_name.columns), "year", year )
    df_name.insert(len(df_name.columns), "state_id", state_id)
    df_name.amount = df_name['amount'].str.replace('.', '').str.replace(",", ".").astype(float)
    return df_name

def add_numbers_times1000(df_name, state_id, year):
    df_name.dropna(subset=['amount'],inplace=True)
    df_name['amount'] = df_name['amount'].astype(str)
    df_name.insert(len(df_name.columns), "year", year )
    df_name.insert(len(df_name.columns), "state_id", state_id)
    df_name.amount = df_name['amount'].str.replace('.', '').str.replace(",", ".").astype(float)
    df_name.amount = df_name['amount'].apply(lambda x:int(x*1000))
    return df_name

#Dinge für später: Brauche ich leading 0 für gruppe und counter? Ich brauche noch die umwandlung von float zu string für add_numbers, da kein x1000. 
# Zudem vielleicht doch noch state statt state_id ermöglichen. Ich brauche eine Funktion für das aufsplitten, sodass die ihre Tabellen erzeugen können.
# Zum Abschluss dann eine einfache Funktion, die den ganzen Müll dropt. Lässt sich eventuell mit der Split kombinieren.