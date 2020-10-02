import os
import datetime
from dateutil.relativedelta import relativedelta
import concurrent.futures
import common_function as common

current_path = os.path.abspath(os.getcwd())
formatter = common.logging.Formatter('%(asctime)s %(levelname)s %(message)s')

def ListFiles(tile, filelatest, file5year):
    #today = (datetime.datetime.now()).strftime("%Y%m")
    today = "202009"
    #pathlog = current_path + '/logs/checkcase-'+ today +'.log'
    #logger = common.setup_logger(formatter, 'Checkcase', pathlog)
    #today = (datetime.datetime.now()).strftime("%Y%m")
    indexfile = common.GCPQuery(tile)
    localcount = 0
    miscount = 0
    notload = [] 
    for index in indexfile:
        name = index.name
        size = index.size
        #Find Folder to cut scope
        if name[-8:] != "$folder$":
            pathfile = current_path + "/" + index.name
            check = os.path.exists(pathfile)
            if check:
                filesize = os.path.getsize(pathfile)
                if filesize == size:
                    localcount += 1
                    print("File: %s already download.\nSize: %s" %(name, common.BeautySize(size)))
                elif filesize != size:
                    print("File: %s already download but size different.\nSize local: %s, Size cloud: %s" %(name, common.BeautySize(size), common.BeautySize(filesize)))
            
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
                    #For case update every week
                    if yearcreate == yearnow and monthcreate == monthnow:
                        filelatest.append({'name': name, 'size': size})
                    else:
                        file5year.append({'name': name, 'size': size})
                    #result.append({'name': name, 'size': size})
                    #print("Case Lower 5 Years")
                    #logger.info("Case Lower 5 Years")

                #For case 5 years but month query less than month now
                elif yearnow - yearcreate == 5 and monthcreate >= monthnow:
                    miscount += 1
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
    
    print("We have %d files in local. %d need to download" %(localcount, miscount))
    print("Folder that older than 5 year: %s" %len(newlist))
    print("File do not need to download: %s" %len(notload))

def CheckFiles():
    tilelist = common.ReadCSV(current_path)
    filelatest = []
    file5year = []
    for eachtile in tilelist:
        ListFiles(eachtile, filelatest, file5year)

    # for i in filelatest:
    #     print("File: %s Size: %s" %(i["name"], common.BeautySize(i["size"])))
    
    ListFiletoJSON(file5year)
    # print("#######################")
    # for i in file5year:
    #     print("File: %s Size: %d" %(i["name"], i["size"]))
    # today = datetime.datetime.today()
    # lastyear = today - relativedelta(years=3)
    # print("Today: %s LastYear: %s" %(today, lastyear))

def ListFiletoJSON(file5year):
    forjson = {}

    for eachfile in file5year:
        date = ((eachfile["name"].split("/"))[5].split("_"))[3]
        yearoffile = date[0:4]
        monthoffile = date[4:6]
        tmp = [yearoffile, monthoffile]
        tmp2 = {"name": eachfile["name"], "size": eachfile["size"]}

        #Case forjson have key year & month EX. {"2015": [{"10": [XXXX]}]}
        if yearoffile in forjson.keys() and monthoffile in forjson[yearoffile][0].keys():
            forjson[yearoffile][0][monthoffile].append(tmp2)

        #Case forjson have key year only (New Month) Ex. {"2015": []}
        elif yearoffile in forjson.keys():
            forjson[yearoffile][0][monthoffile] = [tmp2]

        #Case forjson don't have any key (New year)
        elif yearoffile not in forjson.keys():
            forjson[yearoffile] = [{monthoffile:[tmp2]}]
    
    print(listyear)
    print(forjson["2015"])

if __name__ == "__main__":
    CheckFiles()