#!/opt/python2_7/bin/python
import boto
import datetime
import sqlite3
import logging
from optparse import OptionParser
#
def printBackupError(errStr):
    """wrapper function to the logging function for errors"""
    # print "Backup Error : %s" %  errStr 
    msgStr = "Backup Error," + errStr
    logging.error(msgStr)
    return True
#
def getListBackupKeys(bucketname):
    """Takes a bucket name as an argument and returns a list of keys and sizes for 
    all of the objects stored in S3 that are larger then 0 bytes (ie not directory nodes)
    """
    s3 = boto.connect_s3()
    bucket = s3.lookup(bucketname)
    keylist = []
    for key in bucket:
        #   The Key name for the backup bucket contents should be either
        #   stage/dbmongo09/ a directory entry with zero size
        #   or stage/dbmongo09/12-Jul-12_14:17:01.tgz a backup file object
        #   size>0
        if key.size > 0 :
            keynameList = key.name.split('/')
            if len(keynameList) == 3:
                env =  keynameList[0]
                server =  keynameList[1]
                dateTimeList =  keynameList[2].split('_')
                if len(dateTimeList) == 2:
                    timePartsList = dateTimeList[1].split('.')
                    keylist.append([key.name, key.size, env, server, dateTimeList[0], timePartsList[0] ])
    #
    # keylist is a list of lists each element  having full_key, size_in_bytes, env, server, date , time 
    # for the backup file
    return keylist
#
def withinLastNdays(interval, yr, mon, day, hr, mi, sec):
    """ Determine whether the time spcified is within interval days in the past
        If it is it returns True otherwise False. Boolean type return. 
    """
    tAfter = datetime.datetime.now() - datetime.timedelta(days=interval)
    toTest = datetime.datetime(yr, mon, day, hr, mi, sec)
    return toTest > tAfter
#
#
def monthNumber(montstr):
    """Returns the number of the month from a 3 letter month string"""
    if montstr.upper() == 'JAN':
        return 1
    elif montstr.upper() == 'FEB':
        return 2
    elif montstr.upper() == 'MAR':
        return 3
    elif montstr.upper() == 'APR':
        return 4
    elif montstr.upper() == 'MAY':
        return 5
    elif montstr.upper() == 'JUN':
        return 6
    elif montstr.upper() == 'JUL':
        return 7
    elif montstr.upper() == 'AUG':
        return 8
    elif montstr.upper() == 'SEP':
        return 9
    elif montstr.upper() == 'OCT':
        return 10
    elif montstr.upper() == 'NOV':
        return 11
    elif montstr.upper() == 'DEC':
        return 12
    else :
        return 0
#
parser = OptionParser()
parser.add_option("-c", "--file_copies" , dest="fc", default=2, action="store", type="int",
    help="copies of each file")
parser.add_option("-d", "--copies_per_day" , dest="cpd", default=2, action="store", type="int",
    help="backup copies per day")
parser.add_option("-b", "--bucket" , action="store", type="string", 
    dest="bucket", help="bucket where mongodb backups are stored.")
#
(options, args) = parser.parse_args()
if (options.bucket == None):
    parser.print_usage()
    exit(1)
#
#
bucketToChk = options.bucket
# Number of copies of the backup files expected
minBckCpy = options.fc
# Number of backups per day expected
bcksPerDay = options.cpd
#
# User Modifiable Variables
# Logging format
logging.basicConfig(format='%(asctime)s,%(message)s', datefmt='%m/%d/%Y %H:%M:%S') 
#
# End of user modifiable variables 
#
backupKeyList = getListBackupKeys(bucketToChk)
recentBckupKeyList = []
for keyelement in backupKeyList:
    # get the backup objects for the last day
    dateLst = keyelement[4].split('-')
    year = int(dateLst[2]) + 2000
    month = monthNumber( dateLst[1]) 
    day = int(dateLst[0])
    timeLst = keyelement[5].split(':')
    hour = int(timeLst[0])
    minute = int(timeLst[1])
    sec = int(timeLst[1])
    if  withinLastNdays(1.1, year, month, day, hour, minute, sec):
        recentBckupKeyList.append([keyelement[0], keyelement[1], keyelement[2], keyelement[3], keyelement[4],\
         keyelement[5], datetime.datetime(year, month, day, hour, minute, sec) ] ) 
        # print keyelement[0], keyelement[1], keyelement[2], keyelement[3], keyelement[4], keyelement[5], \
        # datetime.datetime(year, month, day, hour, minute, sec) 

con = sqlite3.connect(':memory:')
with con:
    cur = con.cursor()
    cur.execute("CREATE TABLE BackupFile(Id INTEGER PRIMARY KEY, S3key TEXT, Fsize INTEGER, Env TEXT, Server TEXT, BckTime DATE);")
    idcnt = 1
    for recent_bckup_rec in  recentBckupKeyList:
        cur.execute("INSERT INTO BackupFile VALUES(?, ?, ?, ?, ?, ?)", (idcnt, recent_bckup_rec[0], recent_bckup_rec[1],\
         recent_bckup_rec[2], recent_bckup_rec[3], recent_bckup_rec[6]) )
        idcnt += 1
    con.commit()
    cur.execute("SELECT DISTINCT(Env) from  BackupFile")
    rows = cur.fetchall()
    # We need to check that there are 2 backup times and each backup has at least 1 file in it of greater than 0 size and
    # that the later file has a size greater or equal to that of the first file. ie we have a backup for the shard its >0
    # size and its not getting smaller
    for envt in rows:
        env = str(envt[0])  # remember a rowset is a tuple of tuples
        # print env

        cur.execute("SELECT BckTime, COUNT(*), sum(Fsize) FROM BackupFile WHERE SERVER IN ('dbmongo01', 'dbmongo02', 'dbmongo03')\
          AND Env = ?  GROUP BY  BckTime ORDER BY BckTime", envt)
        times =  cur.fetchall()
        if (len(times) < bcksPerDay) :
            printBackupError("There were only %s Backups found in %s in the last day for shard001 in %s" %  (len(times),bucketToChk, env) )
        for time in times:
            # print(time)
            if (time[1] < minBckCpy ):
                printBackupError("Only %s backup copies in %s for the backup of shard001 in %s taken at %s" % (time[1], bucketToChk, env, time[0]) )
            if (time[2] == 0):
                printBackupError("Total file size for  backup of shard001 in %s taken at %s is zero" % (env,  time[0]) )
        if (times[(len(times)-2)][2] - times[(len(times)-1)][2] ) > 4096 : # if the new backup is 1 block smaller 
            printBackupError("The backup files for shard001 in %s got smaller in S3bucket %s please investigate" % (env, bucketToChk) ) 

        cur.execute("SELECT BckTime, COUNT(*), sum(Fsize) FROM BackupFile WHERE SERVER IN ('dbmongo04', 'dbmongo05', 'dbmongo06')\
          AND Env = ?  GROUP BY  BckTime ORDER BY BckTime", envt)
        times =  cur.fetchall()
        if (len(times) < bcksPerDay) :
            printBackupError("There were only %s Backups found in %s in the last day for shard002 in %s" %  (len(times),bucketToChk, env)  )
        for time in times:
            # print(time)
            if (time[1] < minBckCpy ):
                printBackupError("Only %s backup copies in %s for the backup of shard002 in %s taken at %s" % (time[1], bucketToChk, env, time[0]) )
            if (time[2] == 0):
                printBackupError("Total file size for  backup of shard002 in %s taken at %s is zero" % (env,  time[0]) )
        if (times[(len(times)-2)][2] - times[(len(times)-1)][2] ) > 4096:
            printBackupError("The backup files for shard002 in %s got smaller in S3bucket %s please investigate" % (env, bucketToChk) ) 

        cur.execute("SELECT BckTime, COUNT(*), sum(Fsize) FROM BackupFile WHERE SERVER IN ('dbmongo07', 'dbmongo08', 'dbmongo09')\
          AND Env = ?  GROUP BY  BckTime ORDER BY BckTime", envt)
        times =  cur.fetchall()
        if (len(times) < bcksPerDay) :
            printBackupError("There were only %s Backups found in %s in the last day for shard003 in %s" % (len(times), bucketToChk, env) )
        for time in times:
            # print(time)
            if (time[1] < minBckCpy ):
                printBackupError("Only %s backup copies in %s for the backup of shard003 in %s taken at %s" % (time[1], bucketToChk, env, time[0]) )
            if (time[2] == 0):
                printBackupError("Total file size for  backup of shard003 in %s taken at %s is zero" % (env,  time[0]) )
        if (times[(len(times)-2)][2] - times[(len(times)-1)][2] ) > 4096:
            printBackupError("The backup files for shard003 in %s got smaller in S3 bucket %s please investigate" % (env, bucketToChk) )  
#
