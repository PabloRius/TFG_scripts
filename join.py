"""Join the data from different sources and formats into timed or timeless files"""
import re
import pandas as pd
import numpy as np

# Year group that will be considered to extract the data
YEARS = range(2014, 2024)
# Statistic groups that exist in the research
N_GRUPOS = 5
GRUPOS = {}
GRUPOS_NEW = {}
MIN_YEAR = 9999
MAX_YEAR = 0
cols = ["municipio_nombre",]

GROUP_FILE_LABEL = "Nuevos datos/grupos/Municipios_agrupados_label_"
for j in range(N_GRUPOS):
    GRUPOS_NEW[j] = pd.read_excel(f"{GROUP_FILE_LABEL}{j}.xlsx", names=["municipio_nombre"])
GRUPOS["yearless"] = {k:v.copy() for k,v in GRUPOS_NEW.items()}
for i in YEARS:
    GRUPOS[i] = {k:v.copy() for k,v in GRUPOS_NEW.items()}

# CSV routes for the timeless datasets

# Plazas en centros de atención social
ATS = "Nuevos datos/Centros de Atención Social.csv"
# Centros educativos por tipo de enseñanza
CT = "Nuevos datos/Centros educativos por tipo de enseñanza.csv"
# Número de Asociaciones
ASOC = "Nuevos datos/Asociaciones por municipio.csv"

# CSV routes for the timed datasets (by años)
CentSsoc = "Nuevos datos/Centros de Servicios Sociales.csv"             # Centros de Servicios Sociales (2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023)
EP = "Nuevos datos/indices/estudios/primaria_inferior_perctg.xlsx"      # Porcentaje de población con sólo educación primaria o inferior (2021, 2022)
ES1 = "Nuevos datos/indices/estudios/primera_secundaria_perctg.xlsx"    # Porcentaje de población con sólo educación secundaria (2021, 2022)
ES2 = "Nuevos datos/indices/estudios/segunda_secundaria_perctg.xlsx"    # Porcentaje de población con sólo educación bachillerato (2021, 2022)
ESup = "Nuevos datos/indices/estudios/superior_perctg.xlsx"             # Porcentaje de población con educación superior (UNI/FP/...) (2021, 2022)

T1 = "Nuevos datos/Número de trabajadores - agricultura y ganadería.csv"
T2 = "Nuevos datos/Número de trabajadores - construcción.csv"
T3 = "Nuevos datos/Número de trabajadores - distribución y hostelería.csv"
T4 = "Nuevos datos/Número de trabajadores - empresas y finanzas.csv"
T5 = "Nuevos datos/Número de trabajadores - inmobiliarias.csv"
T6 = "Nuevos datos/Número de trabajadores - minería, industria y energía.csv"
PIB = "Nuevos datos/PIB.csv"
ContIndef = "Nuevos datos/Porcentaje contratos indefinidos registrados.csv"
ContTemp = "Nuevos datos/Porcentaje contratos temporales registrados.csv"
renta = "Nuevos datos/Renta per cápita anual media.csv"

U1 = "Nuevos datos/Unidades productivas - agricultura, ganadería, caza, silvicultura y pesca.csv"
U2 = "Nuevos datos/Unidades productivas - alimentación y textil.csv"
U3 = "Nuevos datos/Unidades productivas - comercio y hostelería.csv"
U4 = "Nuevos datos/Unidades productivas - construcción.csv"
U5 = "Nuevos datos/Unidades productivas - finanzas y seguros.csv"
U6 = "Nuevos datos/Unidades productivas - metal.csv"
U7 = "Nuevos datos/Unidades productivas - minería, electricidad y agua.csv"
U8 = "Nuevos datos/Unidades productivas - transporte y almacenamiento.csv"

def normalize_municipio_nombre(name):
    return re.sub(r'(?i)^(.*),\s*(El|La|Los|Las)\.?$', r'\1 (\2)', name)

def clear_unknown_values(df):
    df.replace(['-', ''], np.nan, inplace=True)
    return df

def normalize_dataframe_municipio_nombre(df):
    df["municipio_nombre"] = df["municipio_nombre"].apply(normalize_municipio_nombre)
    return clear_unknown_values(df)


df_centros = pd.read_csv(CT, engine="python", delimiter=";")
df_centros_ats = pd.read_csv(ATS, delimiter=";", encoding="latin1", names=["municipio_nombre", "plazas_atencion_social"])
df_asoc = pd.read_csv(ASOC, delimiter=";", engine="python", encoding="latin1", names=["municipio_nombre", "asociaciones"])


# Datos "yearless"
for i in range(N_GRUPOS):
    # print("--------------",i,"--------------")
    GRUPOS["yearless"][i] = GRUPOS["yearless"][i].merge(normalize_dataframe_municipio_nombre(df_centros), on="municipio_nombre", how="left")
    GRUPOS["yearless"][i] = GRUPOS["yearless"][i].merge(normalize_dataframe_municipio_nombre(df_centros_ats), on="municipio_nombre", how="left")
    GRUPOS["yearless"][i] = GRUPOS["yearless"][i].merge(normalize_dataframe_municipio_nombre(df_asoc), on="municipio_nombre", how="left")
    GRUPOS["yearless"][i].to_excel(f"Nuevos datos/temp/temp_label_{i}.xlsx", index=False)

df_cssoc = pd.read_csv(CentSsoc, delimiter=";")
df_EP = pd.read_excel(EP)
df_ES1 = pd.read_excel(ES1)
df_ES2 = pd.read_excel(ES2)
df_ESup = pd.read_excel(ESup)

df_T1 = pd.read_csv(T1, delimiter=";")
df_T2 = pd.read_csv(T2, delimiter=";")
df_T3 = pd.read_csv(T3, delimiter=";")
df_T4 = pd.read_csv(T4, delimiter=";")
df_T5 = pd.read_csv(T5, delimiter=";")
df_T6 = pd.read_csv(T6, delimiter=";")

df_pib = pd.read_csv(PIB, delimiter=";")

df_indef = pd.read_csv(ContIndef, delimiter=";")
df_temp = pd.read_csv(ContTemp, delimiter=";")
df_renta = pd.read_csv(renta, delimiter=";")

df_U1 = pd.read_csv(U1, delimiter=";")
df_U2 = pd.read_csv(U2, delimiter=";")
df_U3 = pd.read_csv(U3, delimiter=";")
df_U4 = pd.read_csv(U4, delimiter=";")
df_U5 = pd.read_csv(U5, delimiter=";")
df_U6 = pd.read_csv(U6, delimiter=";")
df_U7 = pd.read_csv(U7, delimiter=";")
df_U8 = pd.read_csv(U8, delimiter=";")

def merge_yeared_data(df:pd.DataFrame, data_name:str):
    global MIN_YEAR, MAX_YEAR
    # cols.append(data_name)
    for j in df.columns[1:]:
        if 2014 <= int(j) < 2025:
            if int(j) < MIN_YEAR:
                MIN_YEAR = int(j)
            if int(j) > MAX_YEAR:
                MAX_YEAR = int(j)
            for i in range(N_GRUPOS):
                GRUPOS[int(j)][i] = GRUPOS[int(j)][i].merge(normalize_dataframe_municipio_nombre(df[["municipio_nombre", f'{j}']].rename(columns={f'{j}': data_name})), on="municipio_nombre", how="left")


merge_yeared_data(df_cssoc, "Centros de Servicios Sociales")
merge_yeared_data(df_EP, "Estudios Primaria e inferior")
merge_yeared_data(df_ES1, "Estudios Primera Secundaria")
merge_yeared_data(df_ES2, "Estudios Segunda Secundaria")
merge_yeared_data(df_ESup, "Estudios Superiores")

merge_yeared_data(df_T1, "Trabajadores Agricultura y ganaderia")
merge_yeared_data(df_T2, "Trabajadores Construcción")
merge_yeared_data(df_T3, "Trabajadores Distribución y Hosteleria")
merge_yeared_data(df_T4, "Trabajadores Empresas y Finanzas")
merge_yeared_data(df_T5, "Trabajadores Inmobiliarias")
merge_yeared_data(df_T6, "Trabajadores Mineria Industria y Energia")

merge_yeared_data(df_pib, "PIB")

merge_yeared_data(df_indef, "Porcentaje Contratos Indefinidos")
merge_yeared_data(df_temp, "Porcentaje Contratos Temporales")
merge_yeared_data(df_renta, "Renta per cápita media")

merge_yeared_data(df_U1, "Unidades productivas - Agricultura, ganaderia, caza, silvicultura y pesca")
merge_yeared_data(df_U2, "Unidades productivas - Alimentacion y textil")
merge_yeared_data(df_U3, "Unidades productivas - Comercio y hosteleria")
merge_yeared_data(df_U4, "Unidades productivas - Construccion")
merge_yeared_data(df_U5, "Unidades productivas - Finanzas y seguros")
merge_yeared_data(df_U6, "Unidades productivas - Metal")
merge_yeared_data(df_U7, "Unidades productivas - Mineria electricidad y agua")
merge_yeared_data(df_U8, "Unidades productivas - Transporte y almacenamiento")

for j in range(MIN_YEAR, MAX_YEAR+1):
    for i in range(N_GRUPOS):
        GRUPOS[int(j)][i].to_excel(f"Nuevos datos/temp/temp_label_{j}_{i}.xlsx", index=False)
