"""
Ingestión de datos - Reporte de clusteres
-----------------------------------------------------------------------------------------

Construya un dataframe de Pandas a partir del archivo 'clusters_report.txt', teniendo en
cuenta que los nombres de las columnas deben ser en minusculas, reemplazando los espacios
por guiones bajos; y que las palabras clave deben estar separadas por coma y con un solo 
espacio entre palabra y palabra.


"""
from io import StringIO
import re
import pandas as pd


def ingest_data():
    
    #
    # Inserte su código aquí
    #
    with open("clusters_report.txt") as fp:
        col1 = re.sub(r"\s{2,}", "\t", fp.readline().strip()).split("\t")
        col2 = re.sub(r"\s{2,}", "\t", fp.readline()).split("\t")
        fp.readline()
        fp.readline()

    columns = [str(col1[i] + " " + col2[i]).strip() if len(col2) > i else col1[i] for i in range(len(col1))]
    cluster, cantidad, porcentaje, principales = [col.lower().replace(" ", "_") for col in columns]

    df = pd.read_fwf("clusters_report.txt", skiprows=4, header=None)
    df.columns = [cluster, cantidad, porcentaje, principales]

    df = df.ffill()

    df[porcentaje] = df[porcentaje].str.replace(",", ".").str.replace(" %", "").astype(float)

    df = df.groupby([cluster, cantidad, porcentaje])[principales].agg(lambda x: x).reset_index(name=principales)

    df[principales] = df[principales].str.join(" ").str.replace(r"\s{2,}", " ", regex=True).str.replace(".", "").str.strip()

    return df

print(ingest_data())

