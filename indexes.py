"""Module generator of the indexes and subindexes"""
import warnings
import pandas as pd
from sklearn.preprocessing import MinMaxScaler
import numpy as np

warnings.filterwarnings("ignore", category=RuntimeWarning)

years = range(2015, 2024)
N_GRUPOS = 5
INDEX_CODES = {
    "PIB": "2.300",
    "Índice de PIB": "2.301",
    "Índice de Capital Humano": "2.302",
    "Subíndice de Nivel de Estudios Completados": "2.303",
    "Subíndice de Centros Escolares por tipo de enseñanza": "2.304",
    "Número de Asociaciones": "2.305",
    "Índice de Calidad del Empleo": "2.306",
    "Porcentaje de Contratos Indefinidos": "2.307",
    "Tasa de Empleo": "2.308",
    "Renta Media per Cápita": "2.309",
    "Índice de Diversidad Económica": "2.310",
    "Subíndice de Contratos por Servicio Ofrecido": "2.311",
    "Subíndice de Unidades Productivas por Servicio Ofrecido": "2.312",
    "Índice de Igualdad Económica": "2.313",
    "Plazas en centros de Atención Social": "2.314",
    "Centros de Servicios Sociales": "2.315"
}

def print_exception(exc):
    """Print an exception with some lines"""
    print("EXCEPTION FOR: ----------------")
    print(exc)
    print("-------------------------------")

def indice_pib(df_out, year, scaler):
    """PIB"""

    df_pib = pd.read_csv("Nuevos datos/PIB.csv", delimiter=";", engine="python")
    pib_col = str(year)
    try:
        pib_1 = df_pib[["municipio_nombre", pib_col]]

        df_out = df_out.merge(pib_1, on="municipio_nombre", how="left")
        df_out.rename(columns={pib_col: "PIB"}, inplace=True)

    except KeyError:
        df_out["PIB"] = np.nan

    pib = df_out[["PIB"]]

    df_out["Índice de PIB"] = scaler.fit_transform(pib).round(2)

    return df_out

def indice_capital_humano(df_out, df_yl, df_y, scaler):
    """Índice de Capital Humano"""

    try:
        ces_1 = df_yl["Centros de Educacion Obligatoria por 1000 Habitantes menores de 16 años"]
        ces_2 = df_yl["Centros de Educacion Postobligatoria por \
100000 Habitantes entre 16 y 64 años"]
        ces_3 = df_yl["Centros de Educacion Especializada por 100000 Habitantes entre 16 y 64 años"]
        ces_4 = df_yl["Centros de Educacion Artistica por 100000 Habitantes"]

        ces_res = ces_1 * 0.25 + ces_2 * 0.25 + ces_3 * 0.25 + ces_4 * 0.25

        df_out["Subíndice de Centros Escolares por tipo de enseñanza"] = ces_res
    except KeyError:
        
        df_out["Subíndice de Centros Escolares por tipo de enseñanza"] = np.nan
    try:
        nes_1 = df_y["Estudios Primaria e inferior"]
        nes_2 = df_y["Estudios Primera Secundaria"]
        nes_3 = df_y["Estudios Segunda Secundaria"]
        nes_4 = df_y["Estudios Superiores"]

        nes_res = nes_1 * 0.15 + nes_2 * 0.20 + nes_3 * 0.25 + nes_4 * 0.30

        df_out["Subíndice de Nivel de Estudios Completados"] = nes_res
    except KeyError:
        
        df_out["Subíndice de Nivel de Estudios Completados"] = np.nan

    try:
        asoc_1 = df_yl["Asociaciones por 1000 Habitantes"]
        df_out["Número de Asociaciones"] = asoc_1

    except KeyError:
        
        df_out["Número de Asociaciones"] = np.nan

    try:
        ces = df_out["Subíndice de Centros Escolares por tipo de enseñanza"]
        nes = df_out["Subíndice de Nivel de Estudios Completados"]
        asoc = df_out["Número de Asociaciones"]

        df_out["Índice de Capital Humano"] = ces / 3 + nes / 3 + asoc / 3

        index = df_out[["Índice de Capital Humano"]]

        df_out["Índice de Capital Humano"] = scaler.fit_transform(index).round(2)
        df_out["Subíndice de Centros Escolares por tipo de enseñanza"] = ces.round(2)
        df_out["Subíndice de Nivel de Estudios Completados"] = nes.round(2)
        df_out["Número de Asociaciones"] = asoc.round(2)
    except (KeyError, ValueError):
        
        df_out["Índice de Capital Humano"] = np.nan

    return df_out

def indice_calidad_empleo(df_out, df_y, scaler):
    """Índice de Calidad del Empleo"""
    try:
        ci_1 = df_y["Porcentaje Contratos Indefinidos"]
        df_out["Porcentaje de Contratos Indefinidos"] = ci_1
    except KeyError:
        
        df_out["Porcentaje de Contratos Indefinidos"] = np.nan

    try:
        te_1 = df_y["Tasa de Empleo"]
        df_out["Tasa de Empleo"] = te_1
    except KeyError:
        
        df_out["Tasa de Empleo"] = np.nan

    try:
        rpc_1 = df_y["Renta per cápita media"]
        df_out["Renta Media per Cápita"] = rpc_1
    except KeyError:
        
        df_out["Renta Media per Cápita"] = np.nan

    try:
        ci = df_out["Porcentaje de Contratos Indefinidos"]
        te = df_out["Tasa de Empleo"]
        rpc = df_out["Renta Media per Cápita"]

        df_out["Índice de Calidad del Empleo"] = ci / 3 + te / 3 + rpc / 3

        index = df_out[["Índice de Calidad del Empleo"]]

        df_out["Índice de Calidad del Empleo"] = scaler.fit_transform(index).round(2)
        df_out["Porcentaje de Contratos Indefinidos"] = ci.round(2)
        df_out["Tasa de Empleo"] = te.round(2)

    except (KeyError, ValueError):
        
        df_out["Índice de Calidad del Empleo"] = np.nan

    return df_out

def indice_diversidad_economica(df_out, df_y, scaler):
    """Índice de Diversidad Económica"""
    try:
        c_1 = df_y["Porcentaje de Trabajadores en Agricultura y Gandería"]
        c_2 = df_y["Porcentaje de Trabajadores en Construcción"]
        c_3 = df_y["Porcentaje de Trabajadores en Distribución y Hostelería"]
        c_4 = df_y["Porcentaje de Trabajadores en Empresas y Finanzas"]
        c_5 = df_y["Porcentaje de Trabajadores en Inmobiliaria"]
        c_6 = df_y["Porcentaje de Trabajadores en Minería Industria y Energía"]

        c_res = c_1 / 6 + c_2 / 6 + c_3 / 6 + c_4 / 6 + c_5 / 6 + c_6 / 6

        df_out["Subíndice de Contratos por Servicio Ofrecido"] = c_res

        c_cols = df_out[["Subíndice de Contratos por Servicio Ofrecido"]]

        df_out["Subíndice de Contratos por Servicio Ofrecido"] = scaler.fit_transform(c_cols)

    except KeyError:
        
        df_out["Subíndice de Contratos por Servicio Ofrecido"] = np.nan

    try:
        u_1 = df_y["Unidades productivas - \
Agricultura, ganaderia, caza, silvicultura y pesca por 1000 habitantes"]
        u_2 = df_y["Unidades productivas - Alimentacion y textil por 1000 habitantes"]
        u_3 = df_y["Unidades productivas - Comercio y hosteleria por 1000 habitantes"]
        u_4 = df_y["Unidades productivas - Construccion por 1000 habitantes"]
        u_5 = df_y["Unidades productivas - Finanzas y seguros por 1000 habitantes"]
        u_6 = df_y["Unidades productivas - Metal por 1000 habitantes"]
        u_7 = df_y["Unidades productivas - Mineria electricidad y agua por 1000 habitantes"]
        u_8 = df_y["Unidades productivas - Transporte y almacenamiento por 1000 habitantes"]

        u_res = u_1 / 8 + u_2 / 8 + u_3 / 8 + u_4 / 8 + u_5 / 8 + u_6 / 8 + u_7 / 8 + u_8 / 8

        df_out["Subíndice de Unidades Productivas por Servicio Ofrecido"] = u_res

        u_cols = df_out[["Subíndice de Unidades Productivas por Servicio Ofrecido"]]
        u_fit = scaler.fit_transform(u_cols)

        df_out["Subíndice de Unidades Productivas por Servicio Ofrecido"] = u_fit

    except KeyError:
        
        df_out["Subíndice de Unidades Productivas por Servicio Ofrecido"] = np.nan

    try:
        c = df_out["Subíndice de Contratos por Servicio Ofrecido"]
        u = df_out["Subíndice de Unidades Productivas por Servicio Ofrecido"]

        df_out["Índice de Diversidad Económica"] = c * 0.5 + u * 0.5

        index = df_out[["Índice de Diversidad Económica"]]

        df_out["Índice de Diversidad Económica"] = scaler.fit_transform(index).round(2)
        df_out["Subíndice de Contratos por Servicio Ofrecido"] = c.round(2)
        df_out["Subíndice de Unidades Productivas por Servicio Ofrecido"] = u.round(2)
    except (KeyError, ValueError):
        
        df_out["Índice de Diversidad Económica"] = np.nan

    return df_out

def indice_igualdad_economica(df_out, df_yl, df_y, scaler):
    "Índice de Igualdad Económica"
    try:
        ats_1 = df_yl["Plazas en Centros de Atención Social por 100 habitantes"]
        df_out["Plazas en centros de Atención Social"] = ats_1
    except KeyError:
        
        df_out["Plazas en centros de Atención Social"] = np.nan

    try:
        ss_1 = df_y["Centros de Servicios Sociales por 1000 Habitantes"]
        df_out["Centros de Servicios Sociales"] = ss_1
    except KeyError:
        
        df_out["Centros de Servicios Sociales"] = np.nan

    try:
        ats = df_out["Plazas en centros de Atención Social"]
        ss = df_out["Centros de Servicios Sociales"]

        df_out["Índice de Igualdad Económica"] = ats * 0.5 + ss * 0.5

        index = df_out[["Índice de Igualdad Económica"]]

        df_out["Índice de Igualdad Económica"] = scaler.fit_transform(index).round(2)
        df_out["Plazas en centros de Atención Social"] = ats.round(2)
        df_out["Centros de Servicios Sociales"] = ss.round(2)

    except (KeyError, ValueError):
        
        df_out["Índice de Igualdad Económica"] = np.nan

    return df_out

def rep_data(df_out, indicators):
    """Proccess all the additional subindexes and variables used to calculate the main indexes"""
    for column, code in INDEX_CODES.items():
        if column in df_out.columns:
            out_cols = df_out[["municipio_nombre", column]].rename(columns={column: "valor", \
                                                                "municipio_nombre": "Nombre"})
            indicators.append(out_cols.assign(Indicador=code))
        else:
            print(column)
    return df_out, indicators

def indexate(group, year):
    """Main indexing function"""
    file_yl = f"Nuevos datos/norm/normalizado_{group}.xlsx"
    file_y = f"Nuevos datos/norm/normalizado_{year}_{group}.xlsx"

    df_yl = pd.read_excel(file_yl)
    df_y = pd.read_excel(file_y)

    df_out = pd.DataFrame()
    df_out["municipio_nombre"] = df_yl["municipio_nombre"]

    indicators = []

    scaler = MinMaxScaler(feature_range=(0.5, 1))

    df_out = indice_pib(df_out, year, scaler)

    df_out = indice_capital_humano(df_out, df_yl, df_y, scaler)

    df_out = indice_calidad_empleo(df_out, df_y, scaler)

    df_out = indice_diversidad_economica(df_out, df_y, scaler)

    df_out = indice_igualdad_economica(df_out, df_yl, df_y, scaler)

    df_out, indicators = rep_data(df_out, indicators)

    final_rows = []
    for indicator_df in indicators:
        for idx, row in indicator_df.iterrows():
            final_rows.append({
                "codigo_valor": f"{group}_{idx}",
                "Nombre": row["Nombre"],
                "Valor": row["valor"],
                "Year": year,
                "Indicador": row["Indicador"]
            })

    df_final = pd.DataFrame(final_rows)

    df_mun_code = pd.read_csv("MadridLAUGeometries.csv", dtype={"lau_id":str})

    codes = df_mun_code[["name", "lau_id"]]

    df_final = df_final.merge(codes, left_on="Nombre", right_on="name", how="left")

    df_final["codigo_valor"] = df_final.apply(
        lambda row: f"{row["lau_id"]}_{row["Indicador"]}_{row["Year"]}", axis=1
    )

    df_final = df_final.drop(columns=['name'])

    return df_final

all_data = []

for i in years:
    for j in range(N_GRUPOS):
        df_exported = indexate(j, i)
        all_data.append(df_exported)

final_data = pd.concat(all_data, ignore_index=True)

final_data.to_csv("Nuevos datos/new_indexes/index_subindex_data.csv", index=False)
