# We import a method from the  modules to address environment variables and 
# we use that method in a function that will return the variables we need from .env 
# to a dictionary we call sql_config

from dotenv import dotenv_values

def get_sql_config():
    '''
        Function loads credentials from .env file and
        returns a dictionary containing the data needed for sqlalchemy.create_engine()
    '''
    needed_keys = ['host', 'port', 'database','user','password']
    dotenv_dict = dotenv_values(".env")
    sql_config = {key:dotenv_dict[key] for key in needed_keys if key in dotenv_dict}
    return sql_config

# Import sqlalchemy and pandas - do this only when instructed

import sqlalchemy as sql
import pandas as pd
import psycopg2

# Insert the get_data() function definition below - do this only when instructed in the notebook

# Write the get data function
def get_data(sql_query):
    sql_config = get_sql_config()
    engine = sql.create_engine('postgresql://user:pass@host/database',
                    connect_args=sql_config 
                    )
    with engine.begin() as conn: 
        results = conn.execute(sql_query)
        return results.fetchall()

# Insert the get_dataframe() function definition below - do this only when instructed in the notebook

def get_dataframe(sql_query):
    sql_config = get_sql_config()
    engine = sql.create_engine('postgresql://user:pass@host/database',
                    connect_args=sql_config
                    )
    return pd.read_sql_query(sql=sql_query, con=engine)

# Insert the get_engine() function definition below - when instructed

def get_engine():
    sql_config = get_sql_config()
    engine = sql.create_engine('postgresql://user:pass@host/database',
                        connect_args=sql_config
                        )
    return engine       

def push_to_database(df, table_name, engine, schema):
    if engine!=None:
        try:
            df.to_sql(name=table_name, # Name of SQL table
                            con=engine, # Engine or connection
                            if_exists='append', # Drop the table before inserting new values
                            schema=schema, # Use schmea that was defined earlier
                            index=False, # Write DataFrame index as a column
                            chunksize=5000, # Specify the number of rows in each batch to be written at a time
                            method='multi') # Pass multiple values in a single INSERT clause
            print(f"The {table_name} table was imported successfully.")
        # Error handling
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            engine = None