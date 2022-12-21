def sh_df_rename_columns(path, year):
    import pandas as pd
    import sqlalchemy as sql

    sh_df = pd.read_csv(path, delimiter= ";", encoding= 'windows-1252'))
    sh_df.columns = sh_2022.columns.str.replace(" ", "_")
    sh_df.columns = sh_2022.columns.str.lower()
    sh_df.rename(columns={"ansatz_2022_t€":"ansatz_2022_euro"}, inplace=True)



def sh_df_add_columns(df):




def sh_df_delet_columns(df2):