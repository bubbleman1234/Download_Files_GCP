import json
import os, csv
import concurrent.futures
from google.cloud import storage

def GetKey():
    wrs2thai = []
    current_path = os.path.abspath(os.getcwd())
    #credential_path = current_path + "/credential.json"
    #os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path

    csvfile = current_path + "/data/LANSAT08_WRS-2-THAILAND.csv"
    with open(csvfile) as csvfile:
        csvReader = csv.DictReader(csvfile)
        for row in csvReader:
            wrs2thai.append([str(row['Path']),str(row['Row'])])

    with concurrent.futures.ThreadPoolExecutor() as executor:
        for row in wrs2thai:
            #wrs = row[0] + "/" + row[1]
            executor.submit(GCPQuery, pathrow = row)

def GCPQuery(pathrow):
    result = {}
    wrs = pathrow[0] + "/" + pathrow[1]
    prefix = "LC08/01/" + wrs
    client = storage.Client()
    blobs = client.list_blobs("gcp-public-data-landsat", prefix=prefix, delimiter=None)
    for blob in blobs:
        url = blob.name
        filesize = blob.size
        tmp = (blob.name).split('/')
        #Component of tmp = ['LC08', '01', '001', '248', 'LC08_L1TP_001248_20160801_20170322_01_T1', 'LC08_L1TP_001248_20160801_20170322_01_T1_MTL.txt']
        if len(tmp) == 6:
            wrs = tmp[2] + "/" + tmp[3]
            if wrs in result:
                result[wrs].append([url, filesize])
            else:
                result[wrs] = [[url, filesize]]
        else:
            pass
    with open('result'+ pathrow[0] + pathrow[1] + '.json', 'w+') as fp:
        json.dump(result, fp)

if __name__ == "__main__":
    GetKey()