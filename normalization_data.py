import pandas as pd
import re
import numpy as np

def normalize_municipio_nombre(name):
    return re.sub(r'(?i)^(.*),\s*(El|La|Los|Las)\.?$', r'\1 (\2)', name)

def clear_unknown_values(df):
    df.replace(['-', ''], np.nan, inplace=True)
    return df

def normalize_dataframe_municipio_nombre(df):
    df["municipio_nombre"] = df["municipio_nombre"].apply(normalize_municipio_nombre)
    return clear_unknown_values(df)

censo = "Nuevos datos/norm/normalizadores/censo.xlsx"
df_censo = pd.read_excel(censo)

years = range(2023, 1995, -1)
df_censo.columns = ["municipio_nombre"]+ list(years)
df_censo['municipio_nombre'] = df_censo['municipio_nombre'].apply(lambda x: re.sub(r'^\d+\s+', '', x))

new_data = []

for i in range(0, len(df_censo), 2):
    municipio = df_censo.iloc[i, 0]
    total_values = df_censo.iloc[i+1, 1:].values
    new_data.append([municipio] + list(total_values))

df_restructured = pd.DataFrame(new_data, columns=["municipio_nombre"]+ list(years))

columns_final = ["municipio_nombre"] + [year for year in range(2014, 2024)]
df_restructured = normalize_dataframe_municipio_nombre(df_restructured)[columns_final]

df_restructured.to_excel("Nuevos datos/norm/normalizadores/censo_rest.xlsx", index=False)
