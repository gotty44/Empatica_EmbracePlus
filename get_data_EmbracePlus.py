from avro.datafile import DataFileReader #ouverture + lecture fichier avro
from avro.io import DatumReader #interprétation des données avro (binaire -> objet python)
import json #manipulation de données JSON
import csv
import os #interaction avec le système de fichiers
import pandas as pd #manipulation de données

avro_file_path = r"C:\Users\Hugo\Desktop\01-3YK9K1J2CJ\raw_data\v6\1-1-01_1764754394.avro"
output_dir = r"C:\Users\Hugo\Desktop"

# for filename in os.listdir(input_dir):
#     if filename.endswith(".avro"):
#         avro_file_path = os.path.join(input_dir, filename)
#         reader = DataFileReader(open(avro_file_path, "rb"), DatumReader())
#         schema = json.loads(reader.meta['avro.schema'].decode('utf-8'))
#         data = next(reader)
        
#         avro_version = (data['schemaVersion']['major'],
#                         data['schemaVersion']['minor'],
#                         data['schemaVersion']['patch'])

#Inclure le reste dans une boucle pour traiter plusieurs fichiers avro dans un répertoire

## Read Avro File
reader = DataFileReader(open(avro_file_path, "rb"), DatumReader()) #ouverture fichier avro en mode binaire + lecture des données avro avec objet reader | DatumReader traduit les enregistrements binaires Avro en objets Python (dict, listes, etc.).

schema = json.loads(reader.meta['avro.schema'].decode('utf-8')) # récupération du schéma avro depuis les métadonnées du fichier avro | décodage en utf-8 | conversion en objet python (dictionnaire)
data=next(reader) # lecture uniquement du premier enregistrement (première ligne de données)

## Print the Avro Schema
print(schema)

## Export sensor data to CSV
avro_version = (data['schemaVersion']['major']), (data['schemaVersion']['minor']), (data['schemaVersion']['patch']) # récupération de la version du schéma avro e.g --> 1.4.2 

# Accelerometer 
acc = data["rawData"]["accelerometer"] # récupération des données de l'accéléromètre

timestamp = [round(acc["timestampStart"] + i * (1e6 / acc["samplingFrequency"])) #round --> arrondir les valeurs
             for i in range(len(acc["x"]))] #compte le nombre de mesure pour créer un nombre de timesptamp équivalent

#1e6 / acc["samplingFrequency"] --> intervalle de temps entre chaque mesure en microsecondes

#Convert ADC (analog-to-Digital Converter) values to g (gravitational force)
if avro_version < (6, 5, 0):
    delta_physical = acc["imuParams"]["physicalMax"] - acc["imuParams"]["physicalMin"]
    delta_adc = acc["imuParams"]["digitalMax"] - acc["imuParams"][" digitalMin"]
    x_g = [val * delta_physical / delta_digital for val in acc["x"]] #conversion des valeurs brutes en g (x,y,z)
    y_g = [val * delta_physical / delta_digital for val in acc["y"]]
    z_g = [val * delta_physical / delta_digital for val in acc["z"]]
else: #pour les versions 6.5.0 et supérieures du capteur 
    conversion_factor = acc["imuParams"]["conversionFactor"]
    x_g = [val * conversion_factor for val in acc["x"]]
    y_g = [val * conversion_factor for val in acc["y"]]
    z_g = [val * conversion_factor for val in acc["z"]]

with open(os.path.join(output_dir, 'accelerometer.csv'), 'w', newline='') as f: #création du fichier CSV dans le répertoire de sortie spécifié | os.path.join -> création chemin | w = write mode | newline='' pour éviter les lignes vides entre les lignes écrites
    writer = csv.writer(f) #création d'un objet writer pour écrire dans le fichier CSV
    writer.writerow(["unix_timestamp", "x", "y", "z"]) #écriture de l'en-tête du fichier CSV
    writer.writerows([[ts, x, y, z] for ts, x, y, z in zip(timestamp, x_g, y_g, z_g)]) #Créé une liste pour chaque ligne du csv e.g --> [ts, x, y, z] = [123456789, 0.01, -0.02, 0.98]]

    #zip(timestamp, x_g, y_g, z_g) --> combine les listes timestamp, x_g, y_g et z_g en une seule liste de tuples --> zip prends plusieurs listes et regroupe les éléments qui ont le même index dans un tuple unique. 

# Gyroscope
gyro = data["rawData"]["gyroscope"]
timestamp = [round(gyro["timestampStart"] + i * (1e6 / gyro["samplingFrequency"]))
            for i in range(len(gyro["x"]))]
# Convert ADC counts in DPS = degree per second
if avro_version < (6,5,0):
    x_dps = [val * 0.07 for val in gyro["x"]]
    y_dps = [val * 0.07 for val in gyro["y"]]
    z_dps = [val * 0.07 for val in gyro["z"]]
else:
    conversion_factor = gyro["imuParams"]["conversionFactor"]
    x_dps = [val * conversion_factor for val in gyro["x"]]
    y_dps = [val * conversion_factor for val in gyro["y"]]
    z_dps = [val * conversion_factor for val in gyro["z"]]
with open(os.path.join(output_dir, 'gyroscope.csv'), 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(["unix_timestamp", "x", "y", "z"])
    writer.writerows([[ts, x, y, z] for ts, x, y, z in zip(timestamp, x_dps, y_dps, z_dps)])

# Eda
eda = data["rawData"]["eda"]
timestamp = [round(eda["timestampStart"] + i * (1e6 / eda["samplingFrequency"]))
            for i in range(len(eda["values"]))] #values = clé contenant les valeurs EDA
with open(os.path.join(output_dir, 'eda.csv'), 'w', newline='') as f:
   writer = csv.writer(f)
   writer.writerow(["unix_timestamp", "eda"])
   writer.writerows([[ts, eda] for ts, eda in zip(timestamp, eda["values"])])

# Temperature
tmp = data["rawData"]["temperature"]
timestamp = [round(tmp["timestampStart"] + i * (1e6 / tmp["samplingFrequency"]))
            for i in range(len(tmp["values"]))]
with open(os.path.join(output_dir, 'temperature.csv'), 'w', newline='') as f:
   writer = csv.writer(f)
   writer.writerow(["unix_timestamp", "temperature"])
   writer.writerows([[ts, tmp] for ts, tmp in zip(timestamp, tmp["values"])])

# Tags
tags = data["rawData"]["tags"]
with open(os.path.join(output_dir, 'tags.csv'), 'w', newline='') as f:
   writer = csv.writer(f)
   writer.writerow(["tags_timestamp"])
   writer.writerows([[tag] for tag in tags["tagsTimeMicros"]])

 # BVP
bvp = data["rawData"]["bvp"]
timestamp = [round(bvp["timestampStart"] + i * (1e6 / bvp["samplingFrequency"]))
            for i in range(len(bvp["values"]))]
with open(os.path.join(output_dir, 'bvp.csv'), 'w', newline='') as f:
   writer = csv.writer(f)
   writer.writerow(["unix_timestamp", "bvp"])
   writer.writerows([[ts, bvp] for ts, bvp in zip(timestamp, bvp["values"])])

 # Systolic peaks
sps = data["rawData"]["systolicPeaks"]
with open(os.path.join(output_dir, 'systolic_peaks.csv'), 'w', newline='') as f:
   writer = csv.writer(f)
   writer.writerow(["systolic_peak_timestamp"])
   writer.writerows([[sp] for sp in sps["peaksTimeNanos"]])

# Steps
steps = data["rawData"]["steps"]
timestamp = [round(steps["timestampStart"] + i * (1e6 / steps["samplingFrequency"]))
            for i in range(len(steps["values"]))]
with open(os.path.join(output_dir, 'steps.csv'), 'w', newline='') as f:
   writer = csv.writer(f)
   writer.writerow(["unix_timestamp", "steps"])
   writer.writerows([[ts, step] for ts, step in zip(timestamp, steps["values"])])

# test git