import os, csv
import concurrent.futures
from google.cloud import storage

def GetKey():
    wrs2thai = []
    current_path = os.path.abspath(os.getcwd())
    credential_path = current_path + "\\credential.json"
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path

    csvfile = current_path + "\\data\\LANSAT08_WRS-2-THAILAND-TEST.csv"
    with open(csvfile) as csvfile:
        csvReader = csv.DictReader(csvfile)
        for row in csvReader:
            wrs2thai.append([row['Path'],row['Row']])
    
    for i in wrs2thai:
        print(i)





if __name__ == "__main__":
    GetKey()