"""
    Program: aqfcdb.py
    Author: Mark Beauharnois
    Org: University at Albany ASRC

    conda activate aqfcdb
    (aqfcdb) python aqfcdy.py -u <db user> -p <db pa$$> aqfcdb.json
"""
import os
import sys
import shutil
import json
import re
import argparse
import datetime as dt
from pymongo import MongoClient

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

    def getMaxToStore(self):
        return(self.prg_cfgdata["RunInformation"]["maxdaystostore"])
    
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
    
    def collectProduct(self, productInfo, fList, dt):

        productList = []   # Array of product filenames

        runlog.write("\t[INFO]: Collecting {} files for {} simulation...\n".format(productInfo["prodDesc"], dt))
        
        for f in fileList:
            m = re.search(productInfo['preFix'],f)
            if(m):      # found a current product file
                productList.append(f)

        """
        If we DO NOT have the expected number of files for this product, log a warning message
        """
        if len(productList) != productInfo["nFiles"]:
            runlog.write("\t\t[WARN]: Got {} files, expected {} for {} on {}\n".format(len(productList), productInfo["nFiles"], productInfo["prodDesc"],dt))
        else:
            runlog.write("\t\t[STAT]: OK\n")
            
        productList.sort()
        return(productList)

class dbManager(object):

    def __init__(self):
        self.mkConnection()
        self.testConnection()
    
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

    """
      Get the current number of forecast day directories stored on local disk
    """
    def getNumLocalDays(self):
        db = self.pmc.aqfcst
        coll = db["local_disk_info"]
        document = coll.find_one({},{"_id":0})
        return(document["numDaysLocal"])
    
    """
      Update the # of forecast day directories stored on local disk using 'ndsVal'
    """
    def setNumLocalDays(self, ndsVal):
        db = self.pmc.aqfcst
        coll = db["local_disk_info"]
        coll.update_one(
            {},
            { "$set" :
                 { "numDaysLocal" : ndsVal }
            }
        )
        
class fileManager(object):

    def __init__(self):
        """
          maxDaysToStore : Maximum number of forecast days (directories of files) to save on local disk
          nDaysStored    : The current number of forecast days on disk (loaded from database on 
                           initialization, and updated upon end of file manager tasks
        """
        self.maxDaysToStore = runMgr.getMaxToStore()
        self.nDaysStored = dbMgr.getNumLocalDays()

    """
     ckBndryCondition : Check condition where user reduced the size of 'maxdaystostore' in the JSON
     config file.  We don't care if they increased it (disk storage is cheap right?) but we do care
     if they decreased it to the point that it is LOWER than the current number of days stored on
     local disk! If this condition is true, the number of directories we would need to purge would
     be 'nDaysStored' - 'maxDaysToStore', which would get us down to the maximum, but then we need
     space for the current number of new forecasts that were processed which is passed in as 'nfcsts'
    """
    def ckBndryCondition(self, nfcsts):
        if self.nDaysStored > self.maxDaysToStore:
            runlog.write("\t[IMPORTANT]: # of forecast days on local disk ({}) EXCEEDS maximum # allowed ({}), purging...\n".format(self.nDaysStored, self.maxDaysToStore))
            num_to_remove = self.nDaysStored - self.maxDaysToStore - nfcsts
            num_removed = self.purgeForecasts(num_to_remove)
            if num_removed < num_to_remove:
                # This is a critical condition because we needed to remove 'num_to_remove' directories
                # but removed LESS than what was required, which means we don't have enough room to store 
                # the new forecasts on local disk!
                runlog.write("\t\t[CRITICAL]: Critical Local Disk Management Issue!\n")
                runlog.write("\t\t[CRITICAL]: Couldn't purge minimum # of forecasts - {} out of {} purged.\n".format(num_removed, num_to_remove))
                runlog.write("\t\t[CRITICAL]: Not enough room to store new forecasts - Check config file and potential local disk issues!\n")
                if num_removed != 0: # some were removed, update database
                    self.nDaysStored = self.nDaysStored - num_removed
                    dbMgr.setNumLocalDays(self.nDaysStored)
                    raise SystemExit
            
            # Correct number of directories were purged
            self.nDaysStored = self.nDaysStored - num_removed
            dbMgr.setNumLocalDays(self.nDaysStored)

    """
     checkSpace : If we get here we passed the 'ckBndryCondition' test, where at runtime
     the number of forecast directories on local disk didn't exceed the maximum allowed. Or,
     it did exceed it, but we were able to successfully purge the requisite number of directories
     to make room for the incoming forecast directories. In this function, we check whether we
     need to clear directory space in order to store the new forecasts, and if so attempt to clear
     the needed space.  The function returns the number of forecast directories that can actually
     be stored (copied to local disk) by this run
    """
    def checkSpace(self, nfcsts):
        if self.nDaysStored == self.maxDaysToStore:
            # We've either been @ the maximum storage for awhile, or the user just reduced
            # it to new 'maxdaystostore' in JSON config file
            runlog.write("\t[IMPORTANT]: # of forecast days on local disk ({}) @ maximum allowed ({}), purging...\n".format(self.nDaysStored, self.maxDaysToStore))
            num_removed = self.purgeForecasts(nfcsts)
            runlog.write("\t\t[INFO]: Removed {} of {} forecast directories.\n".format(num_removed, nfcsts))
            if num_removed != nfcsts:
                return (self.maxDaysToStore - num_removed)
            else:
                return (nfcsts)
        elif self.nDaysStored + nfcsts > self.maxDaysToStore:
            # Example:
            #   nDaysStored    = 5
            #   maxDaysToStore = 10
            #   nfcsts         = 8
            #   5 + 8 == 13, which is > 10
            #   num_to_remove =  (nDaysStored + nfcsts) - maxDaysToStore
            #                 =  (5 + 8) - 10 == 3
            num_to_remove = (self.nDaysStored + nfcsts) - self.maxDaysToStore
            runlog.write("\t[IMPORTANT]: # of forecasts on local disk ({}) + current # of forecasts ({}) > maximum allowed ({}), purging {}...\n"
                         .format(self.nDaysStored, nfcsts, self.maxDaysToStore, num_to_remove))
            num_removed = self.purgeForecasts(num_to_remove)
            runlog.write("\t\t[INFO]: Removed {} of {} forecast directories.\n".format(num_removed, num_to_remove))
            if num_removed != num_to_remove:
                return (nfcsts - (num_to_remove - num_removed))
            else:
                return (nfcsts)
        else:
            # Appears to be plenty of room to store current # of forecast directories (nfcsts)
            return (nfcsts)
    
    """
      purgeForecasts : Removes 'ntr' forecast day directories from the local disk.  Note that
      we ALWAYS purge the 'ntr' OLDEST forecast directories, which is why we sort in ascending
      (oldest to newest) date (directory name) order.  Note that purgeForecasts should ONLY be
      executed if 'maxdaystostore' is REDUCED in the JSON config file, so that it creates a 
      state in which the current number of foreasts stored on local disk is > than the 
      maximum allowed.  The other case where purgeForecasts is executed is when we are at our
      maximum limit for the number of forecasts that can be retained on the local web directory
      which means the current number of days stored is equal to the maximum.
    """
    def purgeForecasts(self, ntr):
        # 'ntr' - # of forecast day directories to remove from disk
        numRemoved = 0  # this ulimately gets returned
        basePath = runMgr.getwebdirroot()
        dirList  = os.listdir(basePath)
        dirList.sort()  # ascending date order
        runlog.write("\t[INFO]: Purging {} forecast directories from local disk...\n".format(ntr))
        for d in range(ntr):
            dirName = dirList.pop(0)
            runlog.write("\t\t[INFO]: Removing forecast directory {} from local disk...\n".format(dirName))
            try:
                shutil.rmtree(basePath+dirName)
                runlog.write("\t\t[STAT]: Ok.\n")
                numRemoved = numRemoved + 1
            except OSError as e:
                runlog.write("\t\t[STAT]: Error: {} - {}\n".format(e.filename, e.strerror))
        
        runlog.write("\t[STAT]: Removed {} of {} forecast directories...\n".format(numRemoved, ntr))
        return(numRemoved)

    """
     copyForecasts : Given the number of forecast dates/directories that CAN be copied to local disk (mind you
     this could be LESS than the number of new forecasts we want to copy to the local space), attempt to copy
     the directories from NetAPP to local disk.  For each forecast along the way, update the 'onDisk' and 
     'offDiskReason' fields in the document.
    """
######################################################################################################################

if __name__ == '__main__':

    FC_Collection = []    # Array list of forecast objects
    
    runMgr = runManager()

    runMgr.setProgramPath()
    runMgr.readCfgFile()
    runMgr.setLogFH()
    runlog = runMgr.getLogFH()
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

    dbMgr   = dbManager()
    fileMgr = fileManager()

    """
    Note: A forecast collection is a simulation date document.  There could be partial product
    lists (files) for a given product, indicating a problem with the simulation of some sort.  We
    should keep track of this information in a "simStatus" document field for the front-end web
    application.  Therefore, we need to check each product.  Note that we're only doing a simple
    "expected number of files for each product" check.  May have to revisit this design if web
    application demands it.
    """
    # Loop over all the forecast dates
    dateList = simMgr.getFinalList()
    for d in range (len(dateList)):
        fileList = os.listdir(simMgr.getFullPath(dateList[d]))
        simStatus = "NORMAL"  # assume everything ok at first
        simMsg    = ""
        
        p_o31hr = []
        p_o31hr = procMgr.collectProduct(prodMgr.getO31hr(), fileList, dateList[d])
        if len(p_o31hr) != prodMgr.getO31hr()["nFiles"]:
            simStatus = "ALERT"
            simMsg = simMsg + "O31HR incomplete # of products\n"
            
        p_o38hr = []
        p_o38hr = procMgr.collectProduct(prodMgr.getO38hr(), fileList, dateList[d])
         if len(p_o38hr) != prodMgr.getO3hr()["nFiles"]:
            simStatus = "ALERT"
            simMsg = simMsg + "O38HR incomplete # of products\n"
        
        p_pm251hr = []
        p_pm251hr = procMgr.collectProduct(prodMgr.getPM251hr(), fileList, dateList[d])
        if len(p_pm251hr) != prodMgr.getPM251hr()["nFiles"]:
            simStatus = "ALERT"
            simMsg = simMsg + "PM25HR incomplete # of products\n"
        
        p_pm2524hr = []
        p_pm2524hr = procMgr.collectProduct(prodMgr.getPM2524hr(), fileList, dateList[d])
        if len(p_pm2524hr) != prodMgr.getPM2524hr()["nFiles"]:
            simStatus = "ALERT"
            simMsg = simMsg + "PM2524HR incomplete # of products\n"
        
        FC_Collection.append(
            { "runDate" : dateList[d],
              "simStat" : simStatus,
              "simMsg"  : simMsg,
              "onDisk"  : "",
              "offDiskReason" : "",
              "netApp"  : runMgr.getnetapproot(),
              "webDir"  : runMgr.getwebdirroot(),
              "o31hr"   : p_o31hr,
              "o38hr"   : p_o38hr,
              "pm251hr" : p_pm251hr,
              "pm2524hr": p_pm2524hr
            }
        )

    """
     Must have at least 1 forecast document to commit to database and store
     to local disk.  Note that the file management process must be completed BEFORE
     the database update because each forecast document needs to have it's 'onDisk'
     flag set and 'offDiskReason' (if it's onDisk flag is "false") determined by 
     the file manager, AND, we need to determine how many of the current forecasts
     can actually be copied to local disk in the event there is a problem purging
     the minimum # of existing directories
    """
    if (len(FC_Collection) > 0):

        # Handle file management tasks for local storage (for web application).
        fileMgr.ckBndryCondition(len(FC_Collection))          # special config file change case
        num_to_copy = fileMgr.checkSpace(len(FC_Collection))  # check remaining space cases
        
        # Update/Insert the current forecast documents into the database
        for f in range(len(FC_Collection)):
            dbMgr.upsertDocuments(FC_Collection[f])

    runlog.write("\t[STAT]: Done.\n")
    runMgr.getLogFH().close()
