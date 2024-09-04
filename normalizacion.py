import pandas as pd
import re
import numpy as np
from sklearn.preprocessing import MinMaxScaler

years = range(2014, 2024)
n_grupos = 5

dont_normalice = ["PIB", "Renta per cápita media"]
# Censos
censo_total = "Nuevos datos/norm/normalizadores/censo_rest.xlsx"
censo_menores_16 = "Nuevos datos/norm/normalizadores/censo_menores_16_rest.xlsx"
censo_mayores_16_menores_64 = "Nuevos datos/norm/normalizadores/censo_menores_16_mayores_64_rest.xlsx"
censo_mayores_64 = "Nuevos datos/norm/normalizadores/censo_mayores_64_rest.xlsx"
afiliados = "Nuevos datos/norm/normalizadores/afiliados.xlsx"

df_censo_total = pd.read_excel(censo_total) 
df_censo_menores_16 = pd.read_excel(censo_menores_16)
df_censo_mayores_16_menores_64 = pd.read_excel(censo_mayores_16_menores_64)
df_censo_mayores_64 = pd.read_excel(censo_mayores_64)
df_afiliados = pd.read_excel(afiliados)

def normalice_df(df:pd.DataFrame, year):
    dont_normalice_current = ["municipio_nombre"] + [col for col in dont_normalice if col in df.columns]
    out_df = df[dont_normalice_current]
    # out_df = df[["municipio_nombre"]]

    # Normalización de variables temporales
    # print("AÑO ", year, ":")

    # Centros de Servicios Sociales / 1000 habitantes
    if(year in df_censo_total.columns):
        out_df = out_df.merge(df_censo_total[["municipio_nombre", year]], on=["municipio_nombre"], how="left")
        out_df = out_df.merge(df[["municipio_nombre", "Centros de Servicios Sociales"]], on=["municipio_nombre"], how="left")
        out_df["Centros de Servicios Sociales por 1000 Habitantes"] = (out_df["Centros de Servicios Sociales"] / out_df[year]) * 1000

        out_df.drop(columns=["Centros de Servicios Sociales", year], inplace=True)

    # % Trabajdores por tipo de servicio
    if (year in df_afiliados.columns):
        scaler = MinMaxScaler(feature_range=(0.5, 1))
        out_df = out_df.merge(df_afiliados[["municipio_nombre", year]], on=["municipio_nombre"], how="left")
        out_df = out_df.merge(df[["municipio_nombre", "Trabajadores Agricultura y ganaderia", "Trabajadores Construcción", "Trabajadores Distribución y Hosteleria", "Trabajadores Empresas y Finanzas", "Trabajadores Inmobiliarias", "Trabajadores Mineria Industria y Energia"]], on=["municipio_nombre"])

        out_df["Porcentaje de Trabajadores en Agricultura y Gandería"] = out_df["Trabajadores Agricultura y ganaderia"] / out_df[year] * 100
        out_df["Porcentaje de Trabajadores en Construcción"] = out_df["Trabajadores Construcción"] / out_df[year] * 100
        out_df["Porcentaje de Trabajadores en Distribución y Hostelería"] = out_df["Trabajadores Distribución y Hosteleria"] / out_df[year] * 100
        out_df["Porcentaje de Trabajadores en Empresas y Finanzas"] = out_df["Trabajadores Empresas y Finanzas"] / out_df[year] * 100
        out_df["Porcentaje de Trabajadores en Inmobiliaria"] = out_df["Trabajadores Inmobiliarias"] / out_df[year] * 100
        out_df["Porcentaje de Trabajadores en Minería Industria y Energía"] = out_df["Trabajadores Mineria Industria y Energia"] / out_df[year] * 100

        out_df["Porcentaje de Trabajadores en Agricultura y Gandería"] = scaler.fit_transform(out_df[["Porcentaje de Trabajadores en Agricultura y Gandería"]])
        out_df["Porcentaje de Trabajadores en Construcción"] = scaler.fit_transform(out_df[["Porcentaje de Trabajadores en Construcción"]])
        out_df["Porcentaje de Trabajadores en Distribución y Hostelería"] = scaler.fit_transform(out_df[["Porcentaje de Trabajadores en Distribución y Hostelería"]])
        out_df["Porcentaje de Trabajadores en Empresas y Finanzas"] = scaler.fit_transform(out_df[["Porcentaje de Trabajadores en Empresas y Finanzas"]])
        out_df["Porcentaje de Trabajadores en Inmobiliaria"] = scaler.fit_transform(out_df[["Porcentaje de Trabajadores en Inmobiliaria"]])
        out_df["Porcentaje de Trabajadores en Minería Industria y Energía"] = scaler.fit_transform(out_df[["Porcentaje de Trabajadores en Minería Industria y Energía"]])
        
        out_df.drop(columns=["Trabajadores Agricultura y ganaderia", "Trabajadores Construcción", "Trabajadores Distribución y Hosteleria",	"Trabajadores Empresas y Finanzas",	"Trabajadores Inmobiliarias", "Trabajadores Mineria Industria y Energia", year], inplace=True)
    # Unidades productivas por 1000 habitantes > 16 años

    if (year in df_censo_mayores_16_menores_64.columns and "Unidades productivas - Agricultura, ganaderia, caza, silvicultura y pesca" in df.columns):
        out_df = out_df.merge(df_censo_mayores_16_menores_64[["municipio_nombre", year]], on=["municipio_nombre"], how="left")
        out_df = out_df.merge(df[["municipio_nombre", "Unidades productivas - Agricultura, ganaderia, caza, silvicultura y pesca", "Unidades productivas - Alimentacion y textil", "Unidades productivas - Comercio y hosteleria", "Unidades productivas - Construccion", "Unidades productivas - Finanzas y seguros", "Unidades productivas - Metal", "Unidades productivas - Mineria electricidad y agua", "Unidades productivas - Transporte y almacenamiento"]], on=["municipio_nombre"])
        out_df["Unidades productivas - Agricultura, ganaderia, caza, silvicultura y pesca por 1000 habitantes"] = (out_df["Unidades productivas - Agricultura, ganaderia, caza, silvicultura y pesca"] / out_df[year]) * 1000
        out_df["Unidades productivas - Alimentacion y textil por 1000 habitantes"] = (out_df["Unidades productivas - Alimentacion y textil"] / out_df[year]) * 1000
        out_df["Unidades productivas - Comercio y hosteleria por 1000 habitantes"] = (out_df["Unidades productivas - Comercio y hosteleria"] / out_df[year]) * 1000
        out_df["Unidades productivas - Construccion por 1000 habitantes"] = (out_df["Unidades productivas - Construccion"] / out_df[year]) * 1000
        out_df["Unidades productivas - Finanzas y seguros por 1000 habitantes"] = (out_df["Unidades productivas - Finanzas y seguros"] / out_df[year]) * 1000
        out_df["Unidades productivas - Metal por 1000 habitantes"] = (out_df["Unidades productivas - Metal"] / out_df[year]) * 1000
        out_df["Unidades productivas - Mineria electricidad y agua por 1000 habitantes"] = (out_df["Unidades productivas - Mineria electricidad y agua"] / out_df[year]) * 1000
        out_df["Unidades productivas - Transporte y almacenamiento por 1000 habitantes"] = (out_df["Unidades productivas - Transporte y almacenamiento"] / out_df[year]) * 1000

        out_df.drop(columns=["Unidades productivas - Agricultura, ganaderia, caza, silvicultura y pesca", "Unidades productivas - Alimentacion y textil", "Unidades productivas - Comercio y hosteleria", "Unidades productivas - Construccion", "Unidades productivas - Finanzas y seguros", "Unidades productivas - Metal", "Unidades productivas - Mineria electricidad y agua", "Unidades productivas - Transporte y almacenamiento", year], inplace=True)

    if ("Estudios Primaria e inferior" in df.columns):
        scaler = MinMaxScaler(feature_range=(0.5, 1))
        out_df["Estudios Primaria e inferior"] = scaler.fit_transform(df[["Estudios Primaria e inferior"]])
        out_df["Estudios Primera Secundaria"] = scaler.fit_transform(df[["Estudios Primera Secundaria"]])
        out_df["Estudios Segunda Secundaria"] = scaler.fit_transform(df[["Estudios Segunda Secundaria"]])
        out_df["Estudios Superiores"] = scaler.fit_transform(df[["Estudios Superiores"]])

    if (year in df_afiliados.columns and year in df_censo_mayores_16_menores_64.columns):
        scaler = MinMaxScaler(feature_range=(0.5, 1))
        out_df["Tasa de Empleo"] = (df_afiliados[year] / df_censo_mayores_16_menores_64[year])
        out_df["Tasa de Empleo"] = scaler.fit_transform(out_df[["Tasa de Empleo"]])

    if ("Porcentaje Contratos Indefinidos" in df.columns):
        scaler = MinMaxScaler(feature_range=(0.5, 1))
        out_df["Porcentaje Contratos Indefinidos"] = scaler.fit_transform(df[["Porcentaje Contratos Indefinidos"]])
        out_df["Porcentaje Contratos Temporales"] = scaler.fit_transform(df[["Porcentaje Contratos Temporales"]])
        
    return out_df

def normalize_df_yearless(df):
    dont_normalice_current = ["municipio_nombre"] + [col for col in dont_normalice if col in df.columns]
    out_df = df[dont_normalice_current]
    # out_df = df[["municipio_nombre"]]

    # Centros educativos por / 1000 ó 100000 habitantes (Por grupos de edades)
    out_df = out_df.merge(df_censo_menores_16[["municipio_nombre", max(df_censo_menores_16.columns[1:])]].rename(columns={max(df_censo_menores_16.columns[1:]):"menores16"}), on=["municipio_nombre"])
    out_df = out_df.merge(df_censo_mayores_16_menores_64[["municipio_nombre", max(df_censo_mayores_16_menores_64.columns[1:])]].rename(columns={max(df_censo_mayores_16_menores_64.columns[1:]):"mayores16_menores64"}), on=["municipio_nombre"])
    out_df = out_df.merge(df_censo_mayores_64[["municipio_nombre", max(df_censo_mayores_64.columns[1:])]].rename(columns={max(df_censo_mayores_64.columns[1:]):"mayores64"}), on=["municipio_nombre"])
    data_cols = ["plazas_atencion_social", "Centro de educacion artistica", "Centro de educacion especial","Centro de educacion obligatoria","Centro de educacion postobligatoria", "asociaciones"]
    out_df = out_df.merge(df[data_cols + ["municipio_nombre"]], on=["municipio_nombre"], how="left")

    out_df["Centros de Educacion Obligatoria por 1000 Habitantes menores de 16 años"] = (out_df["Centro de educacion obligatoria"] / out_df["menores16"]) * 1000
    out_df["Centros de Educacion Postobligatoria por 100000 Habitantes entre 16 y 64 años"] = (out_df["Centro de educacion postobligatoria"] / out_df["mayores16_menores64"]) * 100000
    out_df["Centros de Educacion Especializada por 100000 Habitantes entre 16 y 64 años"] = (out_df["Centro de educacion especial"] / out_df["mayores16_menores64"]) * 100000
    out_df["Centros de Educacion Artistica por 100000 Habitantes"] = (out_df["Centro de educacion artistica"] / (out_df["menores16"] + out_df["mayores16_menores64"] + out_df["mayores64"])) * 100000

    # Asociaciones por 1000 habitantes
    out_df["Asociaciones por 1000 Habitantes"] = (out_df["asociaciones"] / (out_df["menores16"] + out_df["mayores16_menores64"] + out_df["mayores64"])) * 1000

    # Plazas en Centros de Atención Social por 100 habitantes
    out_df["Plazas en Centros de Atención Social por 100 habitantes"] = (out_df["plazas_atencion_social"] / (out_df["menores16"] + out_df["mayores16_menores64"] + out_df["mayores64"])) * 100


    out_df = out_df.drop(columns=["menores16", "mayores16_menores64", "mayores64"] + data_cols)

    return out_df

for i in years:
    for j in range(n_grupos):
        file = f"Nuevos datos/temp/temp_label_{i}_{j}.xlsx"
        df = pd.read_excel(file)

        df_out = normalice_df(df, i)
        df_out.to_excel(f"Nuevos datos/norm/normalizado_{i}_{j}.xlsx", index=False)

for i in range(n_grupos):
    file = f"Nuevos datos/temp/temp_label_{i}.xlsx"
    df = pd.read_excel(file)
    # df_out = normalize_df_yearless(df)
    # df_out.to_excel(f"Nuevos datos/norm/normalizado_{i}.xlsx", index=False)