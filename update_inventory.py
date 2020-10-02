import os
import json
import datetime
from dateutil.relativedelta import relativedelta
import concurrent.futures
import common_function as common

directory_path = []
current_path = os.path.abspath(os.getcwd())
formatter = common.logging.Formatter('%(asctime)s %(levelname)s %(message)s')
datenow = (datetime.datetime.now()).strftime("%Y-%m-%d")

def ListFiles(tile, filelatest, file5year):
    today = (datetime.datetime.now()).strftime("%Y%m")
    #today = "202009"
    #pathlog = current_path + '/logs/checkcase-'+ today +'.log'
    #logger = common.setup_logger(formatter, 'Checkcase', pathlog)
    #today = (datetime.datetime.now()).strftime("%Y%m")
    indexfile = common.GCPQuery(tile)
    localcount = 0
    miscount = 0
    tilesize = 0
    notload = [] 
    for index in indexfile:
        name = index.name
        size = index.size
        #Find Folder to cut scope
        if name[-8:] != "$folder$":
            pathfile = current_path + "/" + index.name
            directory = name.split((name.split('/'))[5])[0]
            check = os.path.exists(pathfile)
            if check:
                filesize = os.path.getsize(pathfile)
                if filesize == size:
                    localcount += 1
                    #print("File: %s already download.\nSize: %s" %(name, common.BeautySize(size)))
                elif filesize != size:
                    print("File: %s already download but size different.\nSize local: %s, Size cloud: %s" %(name, common.BeautySize(size), common.BeautySize(filesize)))
            
            #Case File not in local
            elif not check:
                #EX. filecreate = 202009
                filecreate = ((name.split('/')[5]).split('_')[3])[0:6]
                yearnow = int(today[0:4])
                yearcreate = int(filecreate[0:4])
                monthnow = int(today[4:])
                monthcreate = int(filecreate[4:])
                #print("YearNow: %d MonthNow: %d | YearCreate: %d MonthCreate: %d" %(yearnow, monthnow, yearcreate, monthcreate))
                #logger.info("YearNow: %d MonthNow: %d | YearCreate: %d MonthCreate: %d" ,yearnow, monthnow, yearcreate, monthcreate)
                
                #For case lower 5 years
                if yearnow - yearcreate < 5:
                    miscount += 1
                    tilesize += size
                    if common.CheckDirectory(directory, directory_path):
                        directory_path.append(directory)
                    #For case update every week
                    if yearcreate == yearnow and monthnow - monthcreate <= 1 :
                        filelatest.append({'name': name, 'size': size})
                    else:
                        file5year.append({'name': name, 'size': size})
                    #result.append({'name': name, 'size': size})
                    #print("Case Lower 5 Years")
                    #logger.info("Case Lower 5 Years")

                #For case 5 years but month query less than month now
                elif yearnow - yearcreate == 5 and monthcreate >= monthnow:
                    miscount += 1
                    tilesize += size
                    if common.CheckDirectory(directory, directory_path):
                        directory_path.append(directory)
                    file5year.append({'name': name, 'size': size})
                    #result.append({'name': name, 'size': size})
                    #print("Case 5 Years")
                    #logger.info("Case 5 Years")
                #print(filecreate)
                else:
                    #print("Case Not download")
                    #logger.info("Case Not download")
                    notload.append(name.split('/')[4])
                        
    newlist = []
    for i in notload:
        if i not in newlist:
            newlist.append(i)

    print("Tile: %s/%s \t%d Files in local.\t%d Files need to download." %(tile[0], tile[1], localcount, miscount))
    print("Tile Size (5Y): %s\n" %common.BeautySize(tilesize))
    #print("We have %d files in local. %d need to download\n" %(localcount, miscount))
    #print("Folder that older than 5 year: %s" %len(newlist))
    #print("File do not need to download: %s" %len(notload))

def CheckFiles():
    tilelist = common.ReadCSV(current_path)
    filelatest = []
    file5year = []
    list5year = []

    for eachtile in tilelist:
        ListFiles(eachtile, filelatest, file5year)
    
    #Create File JSON for download data later
    jsondata = ListFiletoJSON(file5year, list5year)
    CreateListData5Y(jsondata, list5year)

    return filelatest

def ListFiletoJSON(file5year, listyear):
    forjson = {}

    for eachfile in file5year:
        date = ((eachfile["name"].split("/"))[5].split("_"))[3]
        yearoffile = date[0:4]
        monthoffile = date[4:6]
        filedetail = {"name": eachfile["name"], "size": eachfile["size"]}

        #for Create JSON file by reference from year
        if yearoffile not in listyear:
            listyear.append(yearoffile)

        #Case forjson have key year & month EX. {"2015": [{"10": [XXXX]}]}
        if yearoffile in forjson.keys() and monthoffile in forjson[yearoffile][0].keys():
            forjson[yearoffile][0][monthoffile].append(filedetail)

        #Case forjson have key year only (New Month) Ex. {"2015": []}
        elif yearoffile in forjson.keys():
            forjson[yearoffile][0][monthoffile] = [filedetail]

        #Case forjson don't have any key (New year)
        elif yearoffile not in forjson.keys():
            forjson[yearoffile] = [{monthoffile:[filedetail]}]
    
    return forjson

def CreateListData5Y(jsondata, listyear):
    for year in listyear:
        result = jsondata[year][0]
        with open(current_path + '/data/'+ year + '.json', 'w+') as fp:
            json.dump(result, fp)

def DownloadUpdate(filedownload):
    sumsize = 0
    pathlog = current_path + '/logs/download-'+ datenow +'.log'
    pathlogdirec = current_path + '/logs/directory-'+ datenow +'.log'

    logger = common.SetupLogger(formatter, 'DownloadFiles', pathlog)
    directorylogger = common.SetupLogger(formatter, 'CreateDirectory', pathlogdirec)
    #Check Total Size Before Download.
    for eachfile in filedownload:
        sumsize += eachfile["size"]
    print("Total Size of this month: %s" %common.BeautySize(sumsize))

    checkdisk = common.CheckDiskUsage(sumsize)

    #Create Directory
    for path in directory_path:
        try: 
            os.makedirs(path, exist_ok = True)
            directorylogger.info("Directory %s created successfully", path)

        except Exception as e: 
            directorylogger.error("Directory %s can not be created\nReason: %s" , path, e)

    if checkdisk == True:
        print("You're ready to download.")
        with concurrent.futures.ThreadPoolExecutor() as executor:
            for eachfile in filedownload:
                executor.submit(common.Download, filedownload = eachfile["name"], logger = logger)

    elif checkdisk == False:
        print("Disk not enough spaces.")

if __name__ == "__main__":
    filedownload = CheckFiles()
    DownloadUpdate(filedownload)