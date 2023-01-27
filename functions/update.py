import pandas as pd
import sqlalchemy as sql
import sql_functions as sf
#from sql_functions import get_sql_config
#from sql_functions import get_engine
#from sql_functions import get_dataframe
import psycopg2

# function for updating the functionlist in database
def update_fkz(dataframe):
    sf.get_sql_config()
    engine = sf.get_engine()

    # get data from database
    sql_query = 'select * from capstone_public_budgeting.funktionsbezeichnungen'
    df_funktion = sf.get_dataframe(sql_query)
    df_funktion['origin'] = 'SQL'

    dataframe['origin'] = 'append'

    # compare what is new in this dataframe
    df_function_con = pd.concat([df_funktion,dataframe])
    #delete lines in both df's
    df_function_con.drop_duplicates(subset=['fkz'],keep=False ,inplace=True)
    df_function_con = df_function_con[df_function_con['origin'] == 'append'][['fkz','fkz_txt']]

    #push to database
    sf.push_to_database(df_function_con, "funktionsbezeichnungen", engine, "capstone_public_budgeting")
    return

# function for updating the grouplist in database
def update_group(dataframe):
    sf.get_sql_config()
    engine = sf.get_engine()

    # get data from database
    sql_query = 'select * from capstone_public_budgeting.gruppen'
    df_group = sf.get_dataframe(sql_query)
    df_group['origin'] = 'SQL'

    dataframe['origin'] = 'append' 

    # compare what is new in this dataframe
    df_group_con = pd.concat([df_group,dataframe])
    df_group_con.drop_duplicates(subset=['gruppe'],keep= False,inplace=True)
    df_group_con = df_group_con[df_group_con['origin'] == 'append'][['gruppe','gruppe_txt']]

    #push to database
    sf.push_to_database(df_group_con, "gruppen", engine, "capstone_public_budgeting")
    return

def update_sql_table(dataframe,table_name: str):
    sf.get_sql_config()
    engine = sf.get_engine()

    sql_query = 'select * from capstone_public_budgeting.' + table_name
    df_sql = sf.get_dataframe(sql_query)
    columns = df_sql.columns.to_list()
    df_sql['origin'] = 'SQL'

    dataframe['origin'] = 'append'

    df_con = pd.concat([df_sql,dataframe])
    df_con.drop_duplicates(subset=columns, keep=False,inplace=True)
    df_con = df_con[df_con['origin'] == 'append']
    df_con.drop(['origin'], axis=1,inplace=True)
    
    sf.push_to_database(df_con,table_name,engine,'capstone_public_budgeting')
    return