import pandas as pd
import sqlalchemy as sql
import sql_functions as sf
#from sql_functions import get_sql_config
#from sql_functions import get_engine
#from sql_functions import get_dataframe
import psycopg2


def update_fkz(dataframe):
    sf.get_sql_config()
    engine = sf.get_engine()

    # get data from database
    sql_query = 'select * from capstone_public_budgeting.funktionsbezeichnungen'
    df_funktion = sf.get_dataframe(sql_query)

    # compare what is new in this dataframe
    df_function_con = df_funktion.merge(dataframe.drop_duplicates(),on=['fkz','fkz_txt'],how='right',indicator=True)
    df_function_con = df_function_con[df_function_con['_merge'] == 'right_only'][['fkz','fkz_txt']]

    #push to database
    sf.push_to_database(df_function_con, "funktionsbezeichnungen", engine, "capstone_public_budgeting")
    return


def update_group(dataframe):
    sf.get_sql_config()
    engine = sf.get_engine()

    # get data from database
    sql_query = 'select * from capstone_public_budgeting.gruppen'
    df_group = sf.get_dataframe(sql_query)

    # compare what is new in this dataframe
    df_group_con = df_group.merge(dataframe.drop_duplicates(),on=['gruppe','gruppe_txt'],how='right',indicator=True)
    df_group_con = df_group_con[df_group_con['_merge'] == 'right_only'][['gruppe','gruppe_txt']]

    #push to database
    sf.push_to_database(df_group_con, "gruppen", engine, "capstone_public_budgeting")
    return