from avro.datafile import DataFileReader #ouverture + lecture fichier avro
from avro.io import DatumReader #interprétation des données avro (binaire -> objet python)
import json #manipulation de données JSON
import csv
import os #interaction avec le système de fichiers
import pandas as pd #manipulation de données

avro_file_path = r"C:\Users\Hugo\Desktop\Empatica\DATA"
output_dir = r"C:\Users\Hugo\Desktop\Empatica\processed_data"
os.makedirs(output_dir, exist_ok=True) # création du répertoire de sortie s'il n'existe pas déjà

for filename in os.listdir(avro_file_path):
    if filename.endswith(".avro"):
        file_path = os.path.join(avro_file_path, filename) # création du chemin complet du fichier avro
        reader = DataFileReader(open(file_path, "rb"), DatumReader())
        schema = json.loads(reader.meta['avro.schema'].decode('utf-8'))
        data=next(reader) # un fichier peut contenir plusieurs enregistrements, on lit seulement le premier ici car chez Empatica il n'y a qu'un seul enregistrement par fichier
        reader.close()

        base_name = os.path.splitext(filename)[0] # extraction du nom de base du fichier (sans extension --> [0])
        file_timestamp = int(base_name.split("_")[-1]) # split("_") coupe le string à chaque underscore ["1-1-01", "1764754394"] | [-1] prend le dernier élément de la liste --> 1764754394 | int() convertit en entier
        
        ## Export sensor data to CSV
        avro_version = (data['schemaVersion']['major']), (data['schemaVersion']['minor']), (data['schemaVersion']['patch']) # récupération de la version du schéma avro e.g --> 1.4.2  | schemaVersion est une clé dans le dictionnaire data

        #----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
        # Accelerometer 
        acc = data["rawData"]["accelerometer"] # récupération des données de l'accéléromètre

        timestamp = [round(acc["timestampStart"] + i * (1e6 / acc["samplingFrequency"])) #round --> arrondir les valeurs | 1e6 / acc["samplingFrequency"] --> intervalle de temps entre chaque mesure en microsecondes
                     for i in range(len(acc["x"]))] #compte le nombre de mesure pour créer un nombre de timesptamp équivalent
        #Convert ADC (analog-to-Digital Converter) values to g (gravitational force)
        if avro_version < (6, 5, 0):
            delta_physical = acc["imuParams"]["physicalMax"] - acc["imuParams"]["physicalMin"]
            delta_adc = acc["imuParams"]["digitalMax"] - acc["imuParams"]["digitalMin"]
            x_g = [val * delta_physical / delta_adc for val in acc["x"]] #conversion des valeurs brutes en g (x,y,z)
            y_g = [val * delta_physical / delta_adc for val in acc["y"]]
            z_g = [val * delta_physical / delta_adc for val in acc["z"]]
        else: #pour les versions 6.5.0 et supérieures du capteur 
            conversion_factor = acc["imuParams"]["conversionFactor"]
            x_g = [val * conversion_factor for val in acc["x"]]
            y_g = [val * conversion_factor for val in acc["y"]]
            z_g = [val * conversion_factor for val in acc["z"]]
            
            #Création du .csv 
        with open(os.path.join(output_dir, f'accelerometer_{base_name}.csv'), 'w', newline='') as f: #création du fichier CSV dans le répertoire de sortie spécifié | os.path.join -> création chemin | w = write mode | newline='' pour éviter les lignes vides entre les lignes écrites
            writer = csv.writer(f) #création d'un objet writer pour écrire dans le fichier CSV
            writer.writerow(["file_timestamp", "unix_timestamp", "x", "y", "z"]) #écriture de l'en-tête du fichier CSV
            writer.writerows([[file_timestamp, ts, x, y, z] for ts, x, y, z in zip(timestamp, x_g, y_g, z_g)]) #Créé une liste pour chaque ligne du csv e.g --> [ts, x, y, z] = [123456789, 0.01, -0.02, 0.98]]

            #1. zip(timestamp, x_g, y_g, z_g) --> combine les listes timestamp, x_g, y_g et z_g en une seule liste de tuples --> zip prends plusieurs listes et regroupe les éléments qui ont le même index dans un tuple unique pour donner ensuite une ligne = un tuple 
            #2. [[ts, x, y, z] for ts, x, y, z... transforme les tuples générés par zip en listes pour l'écriture dans le CSV car writerows attend une liste de listes. | tuple (100, 0.1, 0.0, 1.0) → liste [100, 0.1, 0.0, 1.0]

        #----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

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
        
        with open(os.path.join(output_dir, f'gyroscope_{base_name}.csv'), 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(["file_timestamp", "unix_timestamp", "x", "y", "z"])
            writer.writerows([[file_timestamp, ts, x, y, z] for ts, x, y, z in zip(timestamp, x_dps, y_dps, z_dps)])

        # EDA
        eda = data["rawData"]["eda"]
        timestamp = [round(eda["timestampStart"] + i * (1e6 / eda["samplingFrequency"]))
                     for i in range(len(eda["values"]))] #values = clé contenant les valeurs EDA
        with open(os.path.join(output_dir, f'eda_{base_name}.csv'), 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(["file_timestamp", "unix_timestamp", "eda"])
            writer.writerows([[file_timestamp, ts, eda] for ts, eda in zip(timestamp, eda["values"])])

        # Temperature
        tmp = data["rawData"]["temperature"]
        timestamp = [round(tmp["timestampStart"] + i * (1e6 / tmp["samplingFrequency"]))
                     for i in range(len(tmp["values"]))]
        with open(os.path.join(output_dir, f'temperature_{base_name}.csv'), 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(["file_timestamp", "unix_timestamp", "temperature"])
            writer.writerows([[file_timestamp, ts, tmp] for ts, tmp in zip(timestamp, tmp["values"])])

        # Tags
        tags = data["rawData"]["tags"]
        with open(os.path.join(output_dir, f'tags_{base_name}.csv'), 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(["file_timestamp", "tags_timestamp"])
            writer.writerows([[file_timestamp, tag] for tag in tags["tagsTimeMicros"]])

        # BVP (Blood Volume Pulse = battement cardiaque)
        bvp = data["rawData"]["bvp"]
        timestamp = [round(bvp["timestampStart"] + i * (1e6 / bvp["samplingFrequency"]))
                     for i in range(len(bvp["values"]))]
        with open(os.path.join(output_dir, f'bvp_{base_name}.csv'), 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(["file_timestamp", "unix_timestamp", "bvp"])
            writer.writerows([[file_timestamp, ts, bvp] for ts, bvp in zip(timestamp, bvp["values"])])

        # Systolic peaks
        sps = data["rawData"]["systolicPeaks"]
        with open(os.path.join(output_dir, f'systolic_peaks_{base_name}.csv'), 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(["file_timestamp", "systolic_peak_timestamp"])
            writer.writerows([[file_timestamp, sp] for sp in sps["peaksTimeNanos"]])

        # Steps
        steps = data["rawData"]["steps"]
        timestamp = [round(steps["timestampStart"] + i * (1e6 / steps["samplingFrequency"])) 
                     for i in range(len(steps["values"]))]
        with open(os.path.join(output_dir, f'steps_{base_name}.csv'), 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(["file_timestamp", "unix_timestamp", "steps"])
            writer.writerows([[file_timestamp, ts, step] for ts, step in zip(timestamp, steps["values"])]) #zip uniquement les valeurs qui changent (timestamp et steps) | file_timestamp est constant pour tout le fichier