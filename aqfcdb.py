#!/usr/bin/env python3
"""
    aqfcdb.py - Reads AQ forecast directory, using base path 'netapproot',
        run prefix 'runprefix' and simulation date 'rundate' all configured
        in the 'aqfcdb.json' input file; checks for all expected forecast
        files and generates missing file report saved in the aqfcdb.log file;
        loads 'aqfcdb' MongoDB database located on the API server

    conda activate aqfcdb
    python <path to script>/aqfcdb.py <path to script>/aqfcdb.json
"""
import os
import sys
import json
import re
import argparse
import datetime as dt
from shutil import chown, copy2, Error
from pymongo import MongoClient
import urllib

class runManager(object):
    def __init__(self):

        self.dtStamp = dt.datetime.now()
        self.dtStamp = self.dtStamp.replace(microsecond=0)

        self.dtString = self.dtStamp.isoformat('T')
        self.dtString = self.dtString.replace("-", "_")
        self.dtString = self.dtString.replace(":", "_")

        self.maxYear = self.dtStamp.strftime("%Y")
        self.maxMon  = self.dtStamp.strftime("%m")
        self.maxDay  = self.dtStamp.strftime("%d")

        self.setCmdLineArgs()

    def setLogFH(self):
        try:
            self.logfh = open(self.prg_cfgdata["RunInformation"]["logfile"], 'a+')
        except IOError:
            print("\t***ERROR: Could not open run report file ({})\n".format(fname))
            raise SystemExit

    def getLogFH(self):
        return (self.logfh)

    def setCmdLineArgs(self):
        parser = argparse.ArgumentParser()
        parser.add_argument("cfgfile", help="Your input configuration File (JSON format)", type=str)
        parser.add_argument("-u", "--uname", help="Remote database username",type=str)
        parser.add_argument("-p", "--pword", help="Remote database password",type=str)
        args = parser.parse_args()
        self.cfgFile = args.cfgfile
        self.dbuname = args.uname
        self.dbpword = args.pword
    
    def getDTstamp(self):
        return(self.dtStamp)
    
    def getDTstring(self):
        return(self.dtString)
    
    def getUseManFlag(self):
        return(self.prg_cfgdata["RunInformation"]["usemandate"])

    def getStartDate(self):
        # Returns a tuple of (year, month, day)
        return((self.prg_cfgdata["RunInformation"]["rundate"]["year"],
                self.prg_cfgdata["RunInformation"]["rundate"]["month"],
                self.prg_cfgdata["RunInformation"]["rundate"]["day"]))

    def getnetapproot(self):
        return(self.prg_cfgdata["RunInformation"]["netapproot"])

    def getwebdirroot(self):
        return(self.prg_cfgdata["RunInformation"]["webdirroot"])
               
    def getRunPrefix(self):
        return(self.prg_cfgdata["RunInformation"]["runprefix"])

    def getRunSuffix(self):
        return(self.prg_cfgdata["RunInformation"]["runsuffix"])
    
    def getNumRetro(self):
        return(self.prg_cfgdata["RunInformation"]["nretrodays"])
    
    def getMaxRetro(self):
        return(self.prg_cfgdata["RunInformation"]["maxretrodays"])

    def setProgramPath(self):
        self.programPath = os.getcwd()

    def getProgramPath(self):
        return(self.programPath)
    
    def getCfgData(self):
        return(self.prg_cfgdata)
    
    def getDBuname(self):
        return(self.dbuname)
    
    def getDBpword(self):
        return(self.dbpword)

    def readCfgFile(self):
        try:
            self.cfgfh = open(self.cfgFile, 'r')
        except IOError:
            print("\t***ERROR: Could not open JSON Configuration File ({})\n".format(self.cfgFile))
            raise SystemExit
        
        self.prg_cfgdata = json.load(self.cfgfh)

    def writeCfgData(self):
            
        self.logfh.write("aqfcdb.py run:\n")
        self.logfh.write("\tOn: {}\n".format(self.dtString))
        self.logfh.write("\tCfg File: {}\n".format(self.cfgFile))
        self.logfh.write("\t\tUse Manual Date?: {}\n".format(self.prg_cfgdata["RunInformation"]["usemandate"]))
        self.logfh.write("\t\tManual Simulation Date: {}/{}/{}\n".format( 
            self.prg_cfgdata["RunInformation"]["mandate"]["month"],
            self.prg_cfgdata["RunInformation"]["mandate"]["day"],
            self.prg_cfgdata["RunInformation"]["mandate"]["year"]))
        self.logfh.write("\t\tModel Directory: {}\n".format(self.prg_cfgdata["RunInformation"]["netapproot"]))
        self.logfh.write("\t\tRun Prefix: {}\n".format(self.prg_cfgdata["RunInformation"]["runprefix"]))
        self.logfh.write("\t\t# of Retro Days : {}\n".format(self.prg_cfgdata["RunInformation"]["nretrodays"]))
        self.logfh.write("\t\tMax # Retro Days: {}\n".format(self.prg_cfgdata["RunInformation"]["maxretrodays"]))

    def validateMandate(self):
        runlog.write("\t[INFO]: Checking manual date...\n")
        syr = self.prg_cfgdata["RunInformation"]["mandate"]["year"]
        smo = self.prg_cfgdata["RunInformation"]["mandate"]["month"]
        sdy = self.prg_cfgdata["RunInformation"]["mandate"]["day"]

        try:
            runDate = dt.datetime(year=syr, month=smo, day=sdy)
        except ValueError:
            print("\t***ERROR: Bad simulation date, check rundate in JSON config file.\n")
            raise SystemExit

        if (runDate > self.dtStamp):
            print("\t***ERROR: The simulation run date cannot be past the current date\n")
            raise SystemExit

        minYr = self.prg_cfgdata["RunInformation"]["minrunyear"]

        if (syr < minYr):
            print("\t***ERROR: Simulation year < {}, check project start\n".format(minYr))
            raise SystemExit
        
        runlog.write("\t[STAT]: Ok.\n")
    
    def validateRetro(self):
        runlog.write("\t[INFO]: Checking # retrospective days...\n")
        if (self.prg_cfgdata["RunInformation"]["nretrodays"] < 0 or
            self.prg_cfgdata["RunInformation"]["nretrodays"] > self.prg_cfgdata["RunInformation"]["maxretrodays"]):
            print("\t***ERROR: # retrospective simulation days < 0 or > maximum # allowed, check JSON config file.\n")
            raise SystemExit
        runlog.write("\t[STAT]: Ok.\n")

    def validatePyEnv(self):
        runlog.write("\t[INFO]: Checking Python Environment...\n")
        os.system('which python > ./python_env')
        try:
            efile = open('./python_env','r')
        except IOError:
            print("\t***ERROR: Couldn't open Python environment file ./python_env\n")
            raise SystemExit

        pyStr = efile.read()
        efile.close()
        m = re.search('aqfcdb',pyStr)
        if(not m):
            print("\t***ERROR: You are not in the correct conda environment (aqfcdb)\n")
            raise SystemExit
        runlog.write("\t[STAT]: Ok.\n")

class simManager(object):
    def __init__(self):

        self.simDir = runMgr.getnetapproot()
        self.simPre = runMgr.getRunPrefix()
        self.datesList = self.getSimDates()
        self.finalList = [] #contains dates for which simulation plot output directories exist

    def getSimDates(self):
        dList = []    # the list of model run dates
        numRetro = runMgr.getNumRetro()

        if runMgr.getUseManFlag():
            cfg = runMgr.getCfgData()
            myr = cfg["RunInformation"]["mandate"]["year"]
            mmo = cfg["RunInformation"]["mandate"]["month"]
            mdy = cfg["RunInformation"]["mandate"]["day"]

            manDate = dt.datetime(year=myr, month=mmo, day=mdy)
            manDateStr = manDate.strftime("%Y%m%d")
            dList.append(manDateStr)
            baseDate = manDate
        else:
            nowDate = runMgr.getDTstamp()
            nowDateStr = nowDate.strftime("%Y%m%d")
            dList.append(nowDateStr)
            baseDate = nowDate
        
        if numRetro > 0:
            for d in range(numRetro):
                dp1 = d+1
                retroDate = baseDate - dt.timedelta(days = dp1)
                retroDateStr = retroDate.strftime("%Y%m%d")
                dList.append(retroDateStr)

        return(dList)

    def checkSimEnv(self):
        runlog.write("\t[INFO]: Checking model simulation directory...\n")

        # First check to make sure the model simulation directory exists
        if(not os.path.exists(self.simDir)):
            print("\t\t***ERROR: Model simulation directory {} does not exist, check JSON config file\n".format(self.simDir))
            raise SystemExit
        
        runlog.write("\t[STAT]: Ok.\n")
        
        runlog.write("\t[INFO]: Checking simulation sub-directories...\n")
        # Next check to make sure all of the plot output sub-directories (dates stored in 'datesList')
        # exist.  If any are missing, we won't abort, but will report in output log file. Also build
        # the "final" list of simulation dates (those where directories exist, but note, may not have
        # any files, or may be incomplete # of files for each product)
        
        for d in range(len(self.datesList)):
            fullDirPath = self.getFullPath(self.datesList[d])
            if (not os.path.isdir(fullDirPath)):
                runlog.write("\t\t[WARN]: Simulation sub-directory {} does not exist, skipping!\n".format(fullDirPath))
            else:
                runlog.write("\t\t[STAT]: Simulation sub-directory {} exists, using!\n".format(fullDirPath))
                self.finalList.append(self.datesList[d])

        if len(self.finalList) == 0:
            print("\t\t***ERROR: All simulation dates have no corresponding model plot directories\n")
            raise SystemExit
        
        runlog.write("\t[STAT]: Done.\n")
    
    def getFullPath(self,dateArg):
        baseDir = runMgr.getnetapproot()
        prefix  = runMgr.getRunPrefix()
        suffix  = runMgr.getRunSuffix()
        return(baseDir + prefix + dateArg + suffix)

    def getDatesList(self):
        return(self.datesList)

    """
    'finalList' is a list of simulation dates for simulation DIRECTORIES that exist.  This does not mean
    there are files in each of the date directories, or that the correct # of files for each product are in
    the each directory.  That check will be done in processManager.
    """
    def getFinalList(self):
        return(self.finalList)

class productManager(object):
    def __init__(self):
        self.O31hr = {
            "prodDesc": "Hourly O3",
            "nFiles":55,
            "minHr": 0,
            "maxHr": 54,
            "imgTyp": 'png',
            "preFix": 'spa_O3_NYS_F',
            "dataCollection": 'o31hr'
        }
        
        self.O38hr = {
            "prodDesc": "8 Hourly O3",
            "nFiles":48,
            "minHr": 7,
            "maxHr": 54,
            "imgTyp": 'png',
            "preFix": 'spa_8hrO3_NYS_F',
            "dataCollection": 'o38hr'
        }

        self.PM251hr = {
            "prodDesc":  "Hourly PM2.5",
            "nFiles":55,
            "minHr": 0,
            "maxHr": 54,
            "imgTyp": 'png',
            "preFix": 'spa_PM25_NYS_F',
            "dataCollection": 'pm251hr'
        }

        self.PM2524hr = {
            "prodDesc": "24 Hourly PM2.5",
            "nFiles":32,
            "minHr": 23,
            "maxHr": 54,
            "imgTyp": 'png',
            "preFix": 'spa_24hrPM25_NYS_F',
            "dataCollection": 'pm2524hr'
        }
    
    def getO31hr(self):
        return(self.O31hr)
    def getO38hr(self):
        return(self.O38hr)
    def getPM251hr(self):
        return(self.PM251hr)
    def getPM2524hr(self):
        return(self.PM2524hr)

class processManager(object):

    def __init__(self):
        pass
    
    def collectProduct(self,productInfo, fList, dt):

        productList = []   # Array of product filenames

        runlog.write("\t[INFO]: Collecting {} files for {} simulation...\n".format(productInfo["prodDesc"], dt))
        
        for f in fileList:
            m = re.search(productInfo['preFix'],f)
            if(m):      # found a current product file
                productList.append(f)

        """
        If we DO NOT have the expected number of files for this product, return an empty list.
        This is a little heavy handed, but I think cleans up what the user sees via the web app.
        """
        if len(productList) != productInfo["nFiles"]:
            runlog.write("\t\t[WARN]: Got {} files, expected {} for {} on {}\n".format(len(productList), productInfo["nFiles"], productInfo["prodDesc"],dt))
            return([])  # return an empty list
        else:
            """
            W/O expliciting checking, we're assuming that we have all the correct files for a given product if
            the number of products we retrieved is equal to the expected number of files in the product info catalog.
            This may be a risky generalization but until I hear back from Sarah Lu et. al. we're going with it
            """
            productList.sort()
            runlog.write("\t\t[STAT]: OK\n")
            return(productList)
    
    #def checkProduct(self,pInfo,pList):
        #
        # Each product document now contains a run date, the full path of where the products
        # are located, and a list of the available files for that product (e.g. hourly O3)
        # for the run date.  Wade through each of the product documents checking the expected
        # files.
        #
        # At this point, only run dates for a product that actually had files found will exist
        # in the product list (pList)
        #

        #runlog.write("\t[INFO]: Checking {} products...\n".format(pInfo['prodDesc']))
        #if len(pList) == 0:
            #runlog.write("\t\t[WARN]: No {} products found\n".format(pInfo['prodDesc']))
        #else:
            #for pdoc in pList:
                #thisDate = pdoc['runDate']
                #products = pdoc['products']

                #thisHour = pInfo['minHr']
                #while thisHour <= pInfo['maxHr']:
                    #hrStr = f"{thisHour:02}"
                    #fileName = pInfo['preFix'] + hrStr + '.' + pInfo['imgTyp']

                    # check to see if the expected hour filename is in the product list
                    #if fileName not in products:
                        #runlog.write("\t\t[WARN]: File {} NOT FOUND for simulation date {}\n".format(fileName,thisDate))

                    #thisHour = thisHour + 1

class dbManager(object):

    def __init__(self):
        pass
    
    def mkConnection(self):
        runlog.write("\t[INFO]: Establishing PyMongo client connection to remote database...\n")
        un = runMgr.getDBuname()
        pw = runMgr.getDBpword()

        try:
            self.pmc = MongoClient('mongodb://%s:%s@api.asrc.albany.edu/aqfcst'%(un,pw))
        except ConnectionError:
            runlog.write("\t\t[STAT]: Couldn't establish client connection, aborting.\n")
            print("\t***ERROR: Could not make connection to remote MongoDB instance\n".format)
            raise SystemExit
    
    def testConnection(self):
        runlog.write("\t[INFO]: Checking PyMongo client connection to remote database...\n")
        db = self.pmc.aqfcst
        col = db.testcoll

        if col.count_documents({}) == 0:
            runlog.write("\t\t[STAT]: Couldn't retrieve documents from test collection.\n")
            print("\t***ERROR: Could not retrieve documents from test collection\n".format)
            raise SystemExit
        
        runlog.write("\t\t[STAT]: Ok.\n")

    def upsertDocuments(self, fcDocument):

        # If a product already exists in the database (runDate query), then update the 'fullPath'
        # and 'products' components in the existing document.  If it does not exist, insert into
        # the database
        db = self.pmc.aqfcst

        runlog.write("\t[INFO]: Upserting forecast document in database for {}...\n".format(fcDocument['runDate']))
        coll = db["aq_forecasts"]

        coll.update_one (
                { "runDate": fcDocument["runDate"] },
                { "$set":
                    { "runDate" : fcDocument["runDate"],
                      "netApp"  : fcDocument["netApp"],
                      "webDir"  : fcDocument["webDir"],
                      "o31hr"   : fcDocument["o31hr"],
                      "o38hr"   : fcDocument["o38hr"],
                      "pm251hr" : fcDocument["pm251hr"],
                      "pm2524hr": fcDocument["pm2524hr"]
                    }
                },
                upsert=True
            )
        

######################################################################################################################

if __name__ == '__main__':

    FC_Collection = []    # Array list of forecast objects
    
    runMgr = runManager()

    runMgr.setProgramPath()
    runMgr.readCfgFile()
    runMgr.setLogFH()
    runMgr.writeCfgData()
    
    if runMgr.getUseManFlag():
        runMgr.validateMandate()
    if runMgr.getNumRetro() != 0:
        runMgr.validateRetro()
    runMgr.validatePyEnv()
    
    simMgr = simManager()
    simMgr.checkSimEnv()
    
    prodMgr = productManager()
    procMgr = processManager()

    """
    Note: A forecast collection is a simulation date document.  We'll only build a forecast document
    object if we received ALL the product files for each product category (e.g. o31hr, o38hr, etc.).
    Again, this is a little heavy handed, but in the end it makes the user experience in the web app
    much cleaner.
    """
    # Loop over all the forecast dates
    dateList = simMgr.getFinalList()
    for d in range (len(dateList)):
        fileList = os.listdir(simMgr.getFullPath(dateList[d]))

        p_o31hr = []
        p_o31hr = procMgr.collectProduct(prodMgr.getO31hr(), fileList, dateList[d])
        #procMgr.checkProduct(prodMgr.getO31hr(), p_o31hr)

        p_o38hr = []
        p_o38hr = procMgr.collectProduct(prodMgr.getO38hr(), fileList, dateList[d])
        #procMgr.checkProduct(prodMgr.getO38hr(),p_o38hr)

        p_pm251hr = []
        p_pm251hr = procMgr.collectProduct(prodMgr.getPM251hr(), fileList, dateList[d])
        #procMgr.checkProduct(prodMgr.getPM251hr(),p_pm251hr)
        
        p_pm2524hr = []
        p_pm2524hr = procMgr.collectProduct(prodMgr.getPM2524hr(), fileList, dateList[d])
        #procMgr.checkProduct(prodMgr.getPM2524hr(),p_pm2524hr)

        if ( (len(p_o31hr) != 0) and (len(p_o38hr) != 0) and (len(p_pm251hr) != 0) and (len(p_pm2524hr) != 0) ):
            FC_Collection.append(
                { "runDate" : dateList[d],
                  "netApp"  : runMgr.getnetapproot(),
                  "webDir"  : runMgr.getwebdirroot(),
                  "o31hr"   : p_o31hr,
                  "o38hr"   : p_o38hr,
                  "pm251hr" : p_pm251hr,
                  "pm2524hr": p_pm2524hr
                })

    if (len(FC_Collection) > 0):  # must have at least 1 forecast document to commit to database
        #dbMgr = dbManager()
        #dbMgr.mkConnection()
        #dbMgr.testConnection()

        for f in range(len(FC_Collection)):
            #dbMgr.upsertDocuments(FC_Collection[f])
            print(FC_Collection[f])
                       
        #dbMgr.upsertDocuments(prodMgr.getO31hr(), o31hrProd)
        #dbMgr.upsertDocuments(prodMgr.getO38hr(), o38hrProd)
        #dbMgr.upsertDocuments(prodMgr.getPM251hr(), pm251hrProd)
        #dbMgr.upsertDocuments(prodMgr.getPM2524hr(), pm2524hrProd)

    runlog.write("\t[STAT]: Done.\n")
    runMgr.getLogFH().close()
