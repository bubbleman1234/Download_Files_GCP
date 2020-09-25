import json
import os, csv
import concurrent.futures
from google.cloud import storage

directory = []

def CheckDirectory(path):
	if path not in directory:
		directory.append(path)
	else:
		pass

def ReadCSV():
    wrs2thai = []
    current_path = os.path.abspath(os.getcwd())
    csvfile = current_path + "/data/LANSAT08_WRS-2-THAILAND.csv"
    with open(csvfile) as csvfile:
        csvReader = csv.DictReader(csvfile)
        for row in csvReader:
            wrs2thai.append([str(row['Path']),str(row['Row'])])
    return wrs2thai

def GetKey(wrs2thai):
    #credential_path = current_path + "/credential.json"
    #os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path

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
            path = (blob.name.split(tmp[5]))[0]
            CheckDirectory(path)
            wrs = tmp[2] + "/" + tmp[3]
            if wrs in result:
                result[wrs].append([url, filesize])
            else:
                result[wrs] = [[url, filesize]]
        else:
            pass
    current_path = os.path.abspath(os.getcwd())
    with open(current_path + '/key/key'+ pathrow[0] + pathrow[1] + '.json', 'w+') as fp:
        json.dump(result, fp)

def CreateDirectory(year):
    for path in directory:
        #EX. path = LC08/01/132/046/LC08_L1TP_132046_20191222_20200110_01_T1/
        datecreate = (path.split('/')[4]).split('_')[3]
        if year in datecreate:
            if not os.path.exists(path):
                try: 
                    os.makedirs(path, exist_ok = True) 
                    print("Directory '%s' created successfully" % path) 
                except: 
                    print("Directory '%s' can not be created" % path)
            else:
                print(path,"already exist!!")

def GetFile(wrs2thai):
    year = "202001"
    CreateDirectory(year)

if __name__ == "__main__":
    wrs2thai = ReadCSV()
    GetKey(wrs2thai)
    GetFile(wrs2thai)