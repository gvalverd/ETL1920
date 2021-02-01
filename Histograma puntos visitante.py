# -*- coding: utf-8 -*-
"""Partidos liga NBA.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/1BEwejS5lDkkWMyX9jd0bKJ4PEUqVdbs-

#Histograma Puntos Visitante

Ignacio Ruiz de Zuazu Echevarría

Máster en Data Science para Finanzas - ETL

9-12-2020

## **Instalar Spark**
"""

!apt-get install openjdk-8-jdk-headless -qq > /dev/null
!wget -q https://www-us.apache.org/dist/spark/spark-2.4.7/spark-2.4.7-bin-hadoop2.7.tgz
!tar xf spark-2.4.7-bin-hadoop2.7.tgz
!pip install -q findspark

import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["SPARK_HOME"] = "/content/spark-2.4.7-bin-hadoop2.7"

import findspark
findspark.init()
from pyspark import SparkContext
sc = SparkContext.getOrCreate()
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[*]").getOrCreate()

"""## **Creamos un RDD**
Estructura básica que usa spark
"""

nba_data = sc.textFile('partidosLigaNBA.csv')

nba_data.count() # es un 10% del fichero original

cabecera_nba_data = nba_data.take(1)[0] # hay que encontrar el elemento, no la lista
cabecera_nba_data

"""Hay que quitar la cabecera. En los datos vemos que primero tenemos la fecha junto con la hora del partido, después tenemos el equipo que juega de local junto con los puntos que metio en ese partido y por otro lado tenemos los equipos que juegaron fuera de casa con los puntos que metieron."""

sin_cabecera_nba_data = nba_data.filter(lambda x: x != cabecera_nba_data) # filter es de transformacion. Hasta que no haga una accion no se ejecuta
sin_cabecera_nba_data.take(5)

"""Vemos que como separador usa ' : '"""

sin_cabecera_nba_data = nba_data.filter(lambda x: x != cabecera_nba_data).map(lambda x: x.split(':')) 
sin_cabecera_nba_data.take(5)

"""Separo cada elemento en una lista. Tenemos que coger el elemento que hace referencia a los puntos de los visitantes."""

sin_cabecera_nba_data = nba_data.filter(lambda x: x != cabecera_nba_data).map(lambda x: x.split(':')[-1]) 
sin_cabecera_nba_data.take(5)

"""Son de tipo string y tenemos que pasarlo a numérico"""

sin_cabecera_nba_data = nba_data.filter(lambda x: x != cabecera_nba_data).map(lambda x: x.split(':')[-1]).map(lambda x: int(x)) 
sin_cabecera_nba_data.take(5)

"""Hay filas que contienen texto como 'Playoffs'. Habrá que hacerles un filtro."""

sin_cabecera_nba_data = nba_data.filter(lambda x: x != cabecera_nba_data).map(lambda x: x.split(':')[-1]).map(lambda x: int(x))

sin_cabecera_nba_data = nba_data.filter(lambda s: s != cabecera_nba_data).map(lambda s: s.split(':')[-1]).filter(lambda s_pvisit: s_pvisit.isdigit()).map(lambda i_pvisit: int(i_pvisit)) 
sin_cabecera_nba_data.take(1500)

lista = sin_cabecera_nba_data.collect() # Se convierte en lista

lista.sort()

"""* s = string
* s_pvisit = string de los puntos de visitante
* i_pvisit = número íntegro de los puntos de visitante

Miramos los cuartiles
"""

print(lista[int(len(lista)*.25)])
print(lista[int(len(lista)*.50)])
print(lista[int(len(lista)*.75)])

def intervalos(x):
    if x <= 101:
        if x <= 93:
            return ('intervalo 1')
        else:
            return ('intervalo 2')
    else:
        if x <= 110:
            return ('intervalo 3')
        else:
            return ('intervalo 4')

x1 = sin_cabecera_nba_data.map(lambda x: (intervalos(x),1)).reduceByKey(lambda y,z: y + z).collect()

"""Cambio a dataframe"""

import pandas as pd

intervalo_puntos_visitante = pd.DataFrame(x1)

"""Se cambia el nombre de las columnas"""

intervalo_puntos_visitante = intervalo_puntos_visitante.rename(columns={0:'Intervalos',
                                   1:'Puntos'})

"""Se ordenan los intervalos"""

intervalo_puntos_visitante = intervalo_puntos_visitante.sort_values(['Intervalos'])

"""Graficamos el resultado"""

import seaborn as sns

sns.barplot(x= 'Intervalos', y = 'Puntos', data = intervalo_puntos_visitante, color = 'skyblue' )

"""* Los del intervalo 1 van de 0 a 93 puntos
* Los del intervalo 2 van de 93 a 101 puntos
* Los del intervalo 3 van de 101 a 110 puntos
* Los del intervalo 4 van de 110 y más puntos
"""