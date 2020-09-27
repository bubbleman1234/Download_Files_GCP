import json
import os, csv
import logging
import concurrent.futures
from google.cloud import storage

directory = []
current_path = os.path.abspath(os.getcwd())
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')

def BeautySize(nbytes):
    i = 0
    suffixes = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']
    while nbytes >= 1024 and i < len(suffixes)-1:
        nbytes /= 1024.
        i += 1
    f = ('%.2f' % nbytes).rstrip('0').rstrip('.')
    
    return '%s %s' % (f, suffixes[i])

def setup_logger(name, log_file, level=logging.INFO):
    handler = logging.FileHandler(log_file)        
    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)

    return logger

def ReadCSV():
    wrs2thai = []
    csvfile = current_path + "/data/LANSAT08_WRS-2-THAILAND-TEST.csv"
    with open(csvfile) as csvfile:
        csvReader = csv.DictReader(csvfile)
        for row in csvReader:
            wrs2thai.append([str(row['Path']),str(row['Row'])])
    return wrs2thai

def GetKey(wrs2thai):
    credential_path = current_path + "/credential.json"
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path

    with concurrent.futures.ThreadPoolExecutor() as executor:
        for row in wrs2thai:
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
    #current_path = os.path.abspath(os.getcwd())
    with open(current_path + '/key/'+ pathrow[0] + pathrow[1] + '.json', 'w+') as fp:
        json.dump(result, fp)

def CheckDirectory(path):
	if path not in directory:
		directory.append(path)
	else:
		pass

def CreateDirectory(year):
    pathlog = current_path + '/logs/directory.log'
    logger = setup_logger('CreateDirectory', pathlog)
    for path in directory:
        #EX. path = LC08/01/132/046/LC08_L1TP_132046_20191222_20200110_01_T1/
        datecreate = (path.split('/')[4]).split('_')[3]
        if year in datecreate:
            if not os.path.exists(path):
                try: 
                    os.makedirs(path, exist_ok = True)
                    logger.info("Directory '%s' created successfully" % path)
                except: 
                    logger.error("Directory '%s' can not be created" % path)
            else:
                logger.warning("Directory '%s' already exist!!" % path)

def CheckListFile(filecheck, time):
    #EX: filecheck[0] = LC08/01/131/046/LC08_L1TP_131046_20200912_20200919_01_T1/LC08_L1TP_131046_20200912_20200919_01_T1_B6.TIF
    #EX: filename = LC08_L1TP_131046_20200912_20200919_01_T1_B6.TIF
    #EX: datecreatefile = 20200912
    result = {"file":[]}
    size = 0
    for check in filecheck:
        filename = check[0].split('/')[5]
        datecreatefile = filename.split('_')[3]
        if time in datecreatefile:
            result["file"].append(check[0])
            size += check[1]
    result["size"] = BeautySize(size)
    return result

def GetFile(wrs2thai):
    year = "201912"
    CreateDirectory(year)
    for i in wrs2thai:
        #EX. filename = /root/NECTEC/Download_Files_GCP/key/132046.json
        filename = current_path + "/key/" + i[0] + i[1] + ".json"
        key = i[0] + "/" + i[1]
        try:
            with open( filename, "r") as read_file:
                data = json.load(read_file)
            listfile = CheckListFile(data[key], year)
            print("Tile:",key,"Estimate Size:",listfile['size'])
        #print(listfile)
            with concurrent.futures.ThreadPoolExecutor() as executor:
                for eachfile in listfile['file']:
                    executor.submit(Download, filedownload = eachfile, year = year)
        except Exception as e:
            print(e)
            print("Cannot open file:",filename)

def Download(filedownload, year):
    print("Starting Download:",filedownload)
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket("gcp-public-data-landsat")
        blob = bucket.blob(filedownload)
        blob.download_to_filename(blob.name)
        print("Download Successful")
    except Exception as e:
        print(e)
        print("Download Failed")

if __name__ == "__main__":
    wrs2thai = ReadCSV()
    GetKey(wrs2thai)
    GetFile(wrs2thai)