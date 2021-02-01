#!/usr/bin/env python
# coding: utf-8

# # Práctica 1.-  Beatriz Cárdaba
# 
# ## Generación de ficheros de metadatos y lectura de formatos en python
# 
# ### Definición y Objetivo:
# Los metadatos son un conjunto de datos que describen el contenido informativo de un recurso, de archivos o de información de los mismos. Es decir, es información que describe otros datos. Los metadatos proporcionan información que caracteriza datos, describen el contenido, calidad, condiciones, historia, disponibilidad y otras características de los datos.
# Algunos de estos formatos son JSON y YALM.
# 
# El objetivo de este informe es la crecaión e implementación de diferetes funciones que permitan la lectura de datos en formato JSON y YALM en Python

# ## JSON:
# JSON es un formato que almacena información estructurada y se utiliza principalmente para transferir datos entre un servidor y un cliente.
# 
# Los datos en los archivos JSON son pares de propiedad valor separados por dos puntos. Estos pares se separan mediante comas y se encierran entre llaves. El valor de una propiedad puede ser otro objeto JSON, lo que ofrece una gran flexibilidad a la hora de estructurar información. Esta estructura de datos recuerda mucho a los diccionarios de Python.
# 
# JSON está constituído por dos estructuras:
#  - Una colección de pares de nombre/valor. En varios lenguajes esto es conocido como un objeto, registro, estructura, diccionario, tabla hash, lista de claves o un arreglo asociativo.
#  - Una lista ordenada de valores. En la mayoría de los lenguajes, esto se implementa como arrays, vectores, listas o sequencias.
#  
# En JSON, se presentan de estas formas:
# 
# Un objeto es un conjunto desordenado de pares nombre/valor. Un objeto comienza con *{* y termine con *}*. Cada nombre es seguido por *:* y los pares nombre/valor están separados por *,*.
# 
# ### Lectura de ficheros JSON en Python:
# 
# En este caso se van a leer los datos IRIS en el formato JSON. Estos datos han sido extraídos de Kaggle
# 

# In[20]:


# Primero importamos las librerías que vamos a utilizar.
import pandas as pd
# la librería JSON nos permite leer los archivos JSON en python
import json

# creamos una orden para que lea JSON el archivo que indiquemos en open(), se puede introducir la ruta si está en otra localización
with open('iris.json') as f:
  json_iris = json.load(f) # nombramos como data_iris los datos dentro del archivo JSON


# In[21]:


data_iris


# En este ejemplo cada objeto sería una flor. Todas las caraterísticas de cada flor está entre los mismas llaves. Cada carateristica está separada de su valor mediante **:**  Y las carateríasticas de la misma flor está separadas entre ellas mediate **,** .
# 
# ### Generación de fichero JSON en Python:
# 
# 

# In[18]:


import json

# cremaos un objeto que contenga diferentes clave:valor de las cartaterístcas de un estudiante
estudiante1 = '{"Nombres": "Pepe", "idiomas": ["Español", "Inglés"], "País": "España"}'
#pasamos a formato json los datos definidos
datos_estudiante = json.loads(estudiante1)

print( datos_estudiante)


# En este caso el objeto sería el estudiante, vemos sus cartaerísticas sepaedas entre comas entre sí, el nombre de la carteristica (clave) y su valor correspondiente están separados por dos puntos.
# 
# Exportamos los datos que hemos creado a un archivo JSON

# In[22]:


# creamos el formato JSON que se exportará a nuestra carpeta los datos creados en formato JSON
with open('datos_estudiante.json', 'w') as json_file:
  json.dump(datos_estudiante, json_file)


# ## YAML:
# 
# YAML es un formato para guardar objetos de datos con estructura de árbol. Normalmente se utiliza para definir archivos de configuración, aunque también es posible serializar objetos, es decir, escribir la estructura de un objeto en modo cadena de texto para posteriormente poderlo recuperar.
# 
# Las colecciones en YAML usan la indentación para delimitar el alcance y cada elemento de la colección inicia en su propia línea.
# 
# 
# ### Lectura de Ficheros YALM
# 
# En este caso leemos el achivo YAML College Scorecard Raw Data

# In[35]:


# importamos la librería YAML
import yaml
# leemos el archio .YALM para que lo lea python

with open('data.yaml') as file:
    # The FullLoader parameter handles the conversion from YAML
    # scalar values to Python the dictionary format
    data_yalm = yaml.load(file, Loader=yaml.FullLoader)

data_yalm


# ### Generación de ficheros YALM
# 
# Generamos un fichero en formato YALM desde Python y lo guradamos en la carpeta.

# In[2]:


import yaml

# Creamos los datos del fichero
dict_file = [{'sports' : ['soccer', 'football', 'basketball', 'cricket', 'hockey', 'table tennis']},
             {'countries' : ['Pakistan', 'USA', 'India', 'China', 'Germany', 'France', 'Spain']}]

# guardamos el fichero con .yaml en el directorio deonde está el notebook
with open('data_created.yaml', 'w') as file:
    documents = yaml.dump(dict_file, file)


# ## ¿Se pueden leer con spark los ficheros JSON y YALM?
# 
# - Archivos JSON: sí los archivos JSON pueden ser leídos por Spark Apache importando  "spark.implicits"
#  
#  https://spark.apache.org/docs/latest/sql-data-sources-json.html
#  
# 
# - Archivos YALM: sí los archivos  YALM pueden ser leídos por Spark. 
#      - https://www.geeksforgeeks.org/what-is-the-difference-between-yaml-and-json/
#      
#      
# ## ¿Qué tipo de bases de datos No SQL usa estructuras de datos similares?
# 
# La mayoría de las bases de datos NoSQL evitan utilizar este tipo de lenguaje o lo utilizan como un lenguaje de apoyo. Por poner algunos ejemplos, Cassandra utiliza el lenguaje CQL, MongoDB utiliza JSON o BigTable hace uso de GQL.Permiten hacer uso
# de otros tipos de modelos de almacenamiento de información como sistemas de clave–valor, objetos o grafos.
# 
# A ocntinucación definimos dostipos de bases de datos que siguen una estructura parecida:
# 
# - Bases de datos clave – valor: Son el modelo de base de datos NoSQL más popular, además de ser la más sencilla en cuanto a funcionalidad. En este tipo de sistema, cada elemento está identificado por una llave única, lo que permite la recuperación de la información de forma muy rápida, información que habitualmente está almacenada como un objeto binario (BLOB). Se caracterizan por ser muy eficientes tanto para las lecturas como para las escrituras.Algunos ejemplos de este tipo son Cassandra, BigTable o HBase.
# 
# - Bases de datos documentales: almacenan la información como un documento, generalmente utilizando para ello una estructura simple como JSON o XML y donde se utiliza una clave única para cada registro. Estos tipos de almacenamiento de datos permite, además de realizar búsquedas por clave–valor, realizar consultas más avanzadas sobre el contenido del documento. Son las bases de datos NoSQL más versátiles. Se pueden utilizar en gran cantidad de proyectos, incluyendo muchos que tradicionalmente funcionarían sobre bases de datos relacionales. Algunos ejemplos de este tipo son MongoDB o CouchDB.
# 
# - https://www.acens.com/wp-content/images/2014/02/bbdd-nosql-wp-acens.pdf
# 

# ## Referencias
#  - https://www.geeksforgeeks.org/what-is-the-difference-between-yaml-and-json/
#  - https://www.analyticslane.com/2018/07/16/archivos-json-con-python/
