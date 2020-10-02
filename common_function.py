import logging
import shutil
import os, csv
from google.cloud import storage


def BeautySize(nbytes):
    i = 0
    suffixes = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']
    while nbytes >= 1024 and i < len(suffixes)-1:
        nbytes /= 1024.
        i += 1
    f = ('%.2f' % nbytes).rstrip('0').rstrip('.')
    
    return '%s %s' % (f, suffixes[i])

def SetupLogger(formatter, name, log_file, level=logging.INFO):
    handler = logging.FileHandler(log_file)        
    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)

    return logger

def ReadCSV(current_path):
    wrs2thai = []
    csvfile = current_path + "/data/LANSAT08_WRS-2-THAILAND-TEST.csv"
    with open(csvfile) as csvfile:
        csvReader = csv.DictReader(csvfile)
        for row in csvReader:
            wrs2thai.append([str(row['Path']),str(row['Row'])])
    return wrs2thai

def GCPQuery(pathrow):
    wrs = pathrow[0] + "/" + pathrow[1]
    prefix = "LC08/01/" + wrs
    client = storage.Client()
    blobs = client.list_blobs("gcp-public-data-landsat", prefix=prefix, delimiter=None)
    return blobs

def CheckDiskUsage(filesize):
    disk = shutil.disk_usage("/")
    if disk.free > filesize:
        return True
    else:
        return False

def CheckDirectory(path, directory):
	if path not in directory:
		return True
	else:
		return False

def Download(filedownload, logger):
    print("Starting Download:",filedownload)
    logger.info("Starting Download File: %s",filedownload)
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket("gcp-public-data-landsat")
        blob = bucket.blob(filedownload)
        blob.download_to_filename(blob.name)
        print("Download",filedownload,"Successful")
        logger.info("Download File: %s Completed",filedownload)
    except Exception as e:
        print("Download File: %s Failed\nReason: %s" %(filedownload, e))
        logger.error("Download File: %s Failed\nReason: %s", filedownload, e)