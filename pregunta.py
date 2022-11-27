"""
Ingestión de datos - Reporte de clusteres
-----------------------------------------------------------------------------------------

Construya un dataframe de Pandas a partir del archivo 'clusters_report.txt', teniendo en
cuenta que los nombres de las columnas deben ser en minusculas, reemplazando los espacios
por guiones bajos; y que las palabras clave deben estar separadas por coma y con un solo 
espacio entre palabra y palabra.


"""
import pandas as pd


def ingest_data():
    df = pd.read_fwf(
        "clusters_report.txt",
        widths=[8, 10, 20, 100],
        header=None,
        names=[
            "cluster",
            "cantidad_de_palabras_clave",
            "porcentaje_de_palabras_clave",
            "principales_palabras_clave_raw",
        ],
        skip_blank_lines=False,
        converters={
            "porcentaje_de_palabras_clave": lambda x: x.rstrip(" %").replace(",", ".")
        },
    ).drop([0, 1, 2, 3], axis=0)

    principales_palabras_clave_raw = df["principales_palabras_clave_raw"]
    df = df[df["cluster"].notna()].drop("principales_palabras_clave_raw", axis=1)
    df = df.astype(
        {
            "cluster": int,
            "cantidad_de_palabras_clave": int,
            "porcentaje_de_palabras_clave": float,
        }
    )

    principales_palabras_clave_list = []
    total_palabras = ""
    for linea in principales_palabras_clave_raw:
        if isinstance(linea, str):
            total_palabras += linea + " "
        else:
            palabras_individuales = [
                " ".join(x.split()) for x in total_palabras.split(",")
            ]
            palabras_conjuntas = ", ".join(palabras_individuales)
            principales_palabras_clave_list.append(palabras_conjuntas.rstrip("."))
            total_palabras = ""

    df["principales_palabras_clave"] = principales_palabras_clave_list

    return df
