import os, csv
#import timeit
import concurrent.futures
from google.cloud import storage

suffixes = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']
total_size = 0
key = []
directory = []
def BeautySize(nbytes):
	i = 0
	while nbytes >= 1024 and i < len(suffixes)-1:
		nbytes /= 1024.
		i += 1
	f = ('%.2f' % nbytes).rstrip('0').rstrip('.')
	return '%s %s' % (f, suffixes[i])

'''def ListFiles(fullpath,filesize):
    filedata = (fullpath.split('/'))[5]
    path = (fullpath.split(filedata))[0]
    #print(path)
    #print(filedata)
    #print(BeautySize(filesize))'''

def CheckDirectory(path):
	#print(path)
	#if not directory:
	#	directory.append(path) 
	if path not in directory:
		directory.append(path)
	else:
		pass

def GCPQuery(prefix):
	#size = 0
	global total_size
	client = storage.Client()
	blobs = client.list_blobs("gcp-public-data-landsat", prefix=prefix, delimiter=None)
	for blob in blobs:
		#check = blob.name.split("_")
		#if "2020" in check[3]:
		filedata = ((blob.name).split('/'))[5]
		path = (blob.name.split(filedata))[0]
		CheckDirectory(path)
		blobsize = blob.size
		key.append(blob.name)
		#print("File:",filedata,"Size:",blobsize)
		#data = ListFiles(blob.name,blobsize)
		total_size += blobsize

			#count += 1 

def GetKey():
    credential_path = "C:\\Users\\threevarapat.s\\Desktop\\NECTEC\\credential.json"
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path
    csvfile = "LANSAT08_WRS-2-THAILAND-TEST.csv"
	#csvfile = "LANSAT08_WRS-2-THAILAND.csv"
    wrs2thai = []
    with open(csvfile) as csvfile:
        csvReader = csv.DictReader(csvfile)
        for row in csvReader:
            wrs2thai.append([row['Path'],row['Row']])
    #print(wrs2thai)
    #for i in wrs2thai:
    #    print(i)
	#total_size = 0
    count = 0
    for tile in wrs2thai:
        path = tile[0]
        row = tile[1]
        year = "20200123"
        level = ['L1TP','L1GT','L1GS']
        for i in level:
            prefix = "LC08/01/" + path + "/" + row + "/" + "LC08_" + i + "_" + path + row + "_" + year
            GCPQuery(prefix)
			#total_size += GCPQuery(prefix)
        '''with concurrent.futures.ThreadPoolExecutor() as executor:
		futures = []	
		for tile in wrs2thai:
			path = tile[0]
			row = tile[1]
			prefix = "LC08/01/" + path + "/" + row + "/"
		#client = storage.Client()
		#blobs = client.list_blobs("gcp-public-data-landsat", prefix=prefix, delimiter=None)
		#executor.submit(GCPQuery, prefix=prefix)
		
		for blob in blobs:
			check = blob.name.split("_")
			if "2020" in check[3]:
				blobsize = blob.size
				data = ListFiles(blob.name,blobsize)
				total_size += blobsize
				count += 1
		'''
		#for future in concurrent.futures.as_completed(futures):
		#	print(future.result())
    print("Total Sizes:",BeautySize(total_size))
	#print("Files:",count,"\tTotal Sizes:",BeautySize(total_size))

def Download():
	global key
	'''f=open('f1.txt','w')
	for i in key:
		f.write	(i+'\n')
	f.close'''
	#print(key)
	#print(len(key))
	
	storage_client = storage.Client()
	bucket = storage_client.bucket("gcp-public-data-landsat")
	blob = bucket.blob(key[0])
	blob.download_to_filename(blob.name)

def CreateDirectory():
    for path in directory:
        try: 
            os.makedirs(path, exist_ok = True) 
            print("Directory '%s' created successfully" % path) 
        except OSError as error: 
            print("Directory '%s' can not be created" % path)

if __name__ == "__main__":
	#start = timeit.default_timer()
    GetKey()
    CreateDirectory()
    #Download()
    #for i in directory:
    #    print("Directory:",i)
	#stop = timeit.default_timer()
	#print('Time: ', stop - start)
