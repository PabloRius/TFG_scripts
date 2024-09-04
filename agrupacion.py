"""Script that GROUPS a series of regions based on the selected attributes of them"""
import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans

GROUPS = 4

# Read the individual datasets with the attributes
DENSIDAD = "Nuevos datos/Densidad de población.csv"         # Densidad de población
densidad_df = pd.read_csv(DENSIDAD, delimiter=";")[["municipio_nombre", "2023"]]
densidad_df.columns = ["municipio_nombre", "densidad"]

DISTANCIA = "Nuevos datos/Distancia a la Capital.csv"       # Distancia a la capital
ddf_cols = ["municipio_nombre", "distancia"]
distancia_df = pd.read_csv(DISTANCIA, delimiter=";", names=ddf_cols , skiprows=1)

# Generate a merged DataFrame with both attributes
group_vars_df = densidad_df.merge(distancia_df, on=["municipio_nombre"])

# The name of the attributes to be used for the aggroupation
variables = group_vars_df[["distancia", "densidad"]]

# The scaler makes an initial normalization of the variables
scaler = StandardScaler()
norm_vars = scaler.fit_transform(variables)

# K-means algorithm uses the normalized data to generate the desired number of groups
kmeans = KMeans(n_clusters=GROUPS, random_state=42)
kmeans.fit(norm_vars)

# Now we can add a new column with the created labels
group_vars_df["group_label"] = kmeans.labels_

# A new file is created with names of the municipalities belonging to that group
for label in range(GROUPS):
    municipios_df = group_vars_df[group_vars_df['group_label'] == label][['municipio_nombre']]

    output_path = f"Nuevos datos/grupos/Municipios_agrupados_label_{label}.xlsx"
    municipios_df.to_excel(output_path, index=False)
