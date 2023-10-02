#! /usr/bin/env python
""" check for DOIs without data in ESGF
Version: V0.1 2021-07-01: inital script based on registerDOI.py (stockhause@dkrz.de)"""

# Usage: ./checkDOI.py [<test|testdb|testdbtest>] 

import sys,os,re,urllib2,json,time

# set pathes
mydir=os.path.abspath(os.path.dirname(os.path.realpath(__file__)))
f=os.path.abspath(os.path.relpath(mydir+'/../config.json'))
# MS 2019-02-04
#config = json.loads(open(f,'r').read())
config = []
with open(f,'r') as f:
    config = json.loads(f.read())
for l in config["addpaths"]:
    sys.path.append(l)
sys.path.append(mydir) 

# import project specific modules
get_doimd_dir=mydir
import get_doimd
# AF/MS 2019-05-16: replace searchESGF by querySolr
#import searchESGF
import querySolr
#import datacite_mod
import time
#from xml.etree import ElementTree
import logging
from operator import itemgetter
try:
    import cx_Oracle
except:
    print "Cannot import module cx_Oracle"
    sys.exit()


# set environment for utf-8
os.environ['NLS_LANG']="AMERICAN_AMERICA.AL32UTF8"
os.environ['LANG']="en_US.UTF-8"
os.environ['LC_CTYPE']="en_US.UTF-8"
os.environ['LC_NUMERIC']="en_US.UTF-8"
os.environ['LC_TIME']="en_US.UTF-8"
os.environ['LC_COLLATE']="en_US.UTF-8"
os.environ['LC_MONETARY']="en_US.UTF-8"
os.environ['LC_MESSAGES']="en_US.UTF-8"
os.environ['LC_PAPER']="en_US.UTF-8"
os.environ['LC_NAME']="en_US.UTF-8"
os.environ['LC_ADDRESS']="en_US.UTF-8"
os.environ['LC_TELEPHONE']="en_US.UTF-8"
os.environ['LC_MEASUREMENT']="en_US.UTF-8"
os.environ['LC_IDENTIFICATION']="en_US.UTF-8"
os.environ['LC_ALL']="" 

def errorHandling(ecode,emessage,estring):
    """errorHandling: Error handling with insert in log_job table and add to log file; close db connection"""

    log2.error('%s: %s - %s ' % (estring,str(ecode),emessage.strip()))
    conn.rollback()

    # INSERT into log_job
    dumerr=re.sub('\'','\'\'',str(ecode)+' : '+emessage.strip())
    try:
        conn2 = cx_Oracle.connect(sdbfile)
        cur2 = conn2.cursor()
        sql='insert into log_jobs (ID,NAME,CALL,LOG_TYPE,LOG_MESS,ERR_CODE,ERR_MESS,TIMESTAMP) values (cmip6_cite_test.seq_log_jobs.nextVal,\'CHECK_DOI\',\'%s\',\'ERROR\',\'%s\',1,\'%s\',SYSTIMESTAMP)' % (' '.join(sys.argv),estring,dumerr)
        cur2.execute( sql )
        conn2.commit()
        cur2.close()
        conn2.close()
    except cx_Oracle.DatabaseError as e:
        pass


def logMessage(ltype,ecode,emess,lmess,lname,lshortmess):
    """logMessage: log insert messages in log_db table"""

    # insert in LOG_DB
    try:
        sql='insert into log_db (ID,NAME,SQL,ERR_CODE,ERR_MESS,TIMESTAMP) values (cmip6_cite_test.seq_log_db.nextVal,\'CHECK_DOI:%s\',\'%s\',%i,\'%s\',SYSTIMESTAMP)' % (lname,re.sub('\'','\'\'',lshortmess),ecode,re.sub('\'','\'\'',emess))
        cur.execute( sql )
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        log2.error("log_db insert failed: \'%s\' (reported error:\'%s: %s\')" % (sql,str(error.code),error.message))

    if len(testflag) == 0:
        conn.commit()
    if ecode == 0 :
        log2.info(lmess)
    else:
        log2.error((lmess+' - '+str(ecode)+': '+emess).strip())


def errExit():
    """errExit: Error exit with closing of db connection"""

    cur.close()
    conn.close()
    sys.exit()


# analyze options
tstart = time.time()
# MS 2019-02-04
#mydate = os.popen('date +%F').read().strip()
fdate = os.popen('date +%F')
mydate = fdate.read().strip()
fdate.close()

# MS 2019-12-03: pcera.dkrz.de -> pcera
#db='pcera.dkrz.de'
db='pcera'
fileflag=''
try:
    testflag = sys.argv[1]
    #print 'TEST: %s' % testflag
    if testflag == 'testdb' or testflag == 'testdbtest':
        # MS 2019-12-03: testdb -> tcera1
        # MS 2020-04-16: tcera1 -> testdb
        db='testdb'
        #db='tcera1'
        fileflag='test'
    if testflag == 'testdbtest':
        testflag='test'
    elif testflag=='testdb':
        testflag=''
except:
    testflag = ''

# set xml output dir
#outdir = config["xmloutdir"]
# MS 2019-12-03: testdb -> tcera1
# MS 2020-04-16: tcera1 -> testdb
#if db=='tcera1':
#if db=='testdb':
#    outdir=outdir+fileflag  # to be changed

# configure logfile and set log file name
formatter=logging.Formatter('%(asctime)s - %(levelname)s: %(message)s')
formatter2=logging.Formatter('%(levelname)s: %(message)s')

# configure second nodata log file
LOG_FILENAME2 = config["logdir"]+"/nodataESGF"+fileflag+".log"
log2 = logging.getLogger('log2')
console2=logging.FileHandler(LOG_FILENAME2)
console2.setFormatter(formatter)
log2.setLevel(logging.INFO)
log2.addHandler(console2)

# connect to db
#print 'Connect to CERA... %s' % db
cuser  = 'cmip6_cite_test'
# different passwords for cuser in tcera1 and pcera
# MS 2019-02-04
#fdb=os.path.abspath(os.path.relpath(mydir+'/../.cmip6_cite_test'+fileflag))
#cpw    = open(fdb,'r').read().strip()
fdb = open(os.path.abspath(os.path.relpath(mydir+'/../.cmip6_cite_test'+fileflag)),'r')
cpw = fdb.read().strip()
fdb.close()
try:
    # MS 2019-12-03: oda-scan.dkrz.de -> delphi7-scan.dkrz.de
    #sdbfile =  cuser+'/'+cpw+'@'+'( DESCRIPTION = ( ADDRESS_LIST = ( ADDRESS = ( PROTOCOL = TCP ) ( HOST = oda-scan.dkrz.de ) ( PORT = 1521 ) ) ) ( CONNECT_DATA = ( SERVER = DEDICATED ) ( SERVICE_NAME = '+db+' ) ))'
    sdbfile =  cuser+'/'+cpw+'@'+'( DESCRIPTION = ( ADDRESS_LIST = ( ADDRESS = ( PROTOCOL = TCP ) ( HOST = delphi7-scan.dkrz.de ) ( PORT = 1521 ) ) ) ( CONNECT_DATA = ( SERVER = DEDICATED ) ( SERVICE_NAME = '+db+' ) ))'
    conn = cx_Oracle.connect(sdbfile)
    cur = conn.cursor()
except cx_Oracle.DatabaseError as e:
    error, = e.args
    log2.error("Cannot connect to DB=\'%s\'. Check password (%i: %s)" % (':'.join(re.split(':',sdbfile)[:2]),error.code,error.message.strip()))
    #raise IOError, "\nCannot connect to DB=\'%s\'. Check password\n" % cuser
    sys.exit()


# 1. get list of drs_names in citation db for CMIP6 with completed metadata and registered DOIs
cur.prepare('select d.drs_map_esgf,e.external_pid_status from policy_drs d,citation c, externalid e, list_connect l where d.project_acronym in ( \'IPCC-AR6_CMIP6\',\'CMIP6_input4MIPs\' ) and d.policy_drs_id=c.policy_drs_id and c.externalid_id=e.external_id and l.citation_id=c.citation_id and l.contact_type_id=3000006 and e.external_pid_status=\'registered\' and (e.external_pid like \'10.22033/ESGF/%\' or e.external_pid like \'10.5072/ESGF/%\') order by d.drs_map_esgf')
#obs4mips test
#cur.prepare('select d.drs_map_esgf,e.external_pid_status from policy_drs d,citation c, externalid e, list_connect l where d.project_acronym in( \'IPCC-AR6_CMIP6\',\'CMIP6_input4MIPs\', \'Obs4MIPs\' ) and d.policy_drs_id=c.policy_drs_id and c.externalid_id=e.external_id and l.citation_id=c.citation_id and l.contact_type_id=3000006 order by d.drs_map_esgf')
try:
    cur.execute(None, )
except cx_Oracle.DatabaseError as e:
    error, = e.args
    errorHandling(error.code,error.message,'select from database failed -> exit')
    errExit()

try:
    res=cur.fetchall()
    cit_dict = { i[0]:i[1] for i in res }
except Exception as err:
    errorHandling(1, ' '.join(str(err.args)),'No DOIs found in externalid -> exit')  
    errExit() 

tdum = time.time()
tdbaccess = tdum-tstart
log2.info('data base access time:  %i s ( %.2f min )' % (tdbaccess,tdbaccess/60))

# 2. get list of ESGF published entities
# AF/MS 2019-05-16: replace searchESGF by querySolr functionality
#search = searchESGF.SearchESGF()
search = querySolr.QuerySolr()
existdict = {}
nonexistdict = {}
comment = ''

# CMIP6 and input4MIPs list
try:
    CMIP6_list = [m for m in cit_dict.keys()]
    CMIP6_list.sort()
    #print 'DB:CMIP6_list (',len(CMIP6_list),') - ',CMIP6_list
    #(existdict,nonexistdict,comment) = search.getList(CMIP6_list,'CMIP6','institution_id', [4,5], 0,config["nodes"+fileflag])
    (existdict,nonexistdict,comment) = search.get_list(CMIP6_list,'CMIP6','institution_id', [4,5], 0,config["nodes"+fileflag])
except Exception as err:
    errorHandling(1, ' '.join(str(err.args)),'CMIP6: searchESGF failed')  
    errExit() 

if len(comment) > 0: # error exit of search.getList
    errorHandling(1, comment,'CMIP6: searchESGF failed')  
    errExit() 

#print 'ESGF exist    (',len(existdict),'): ',existdict
#print 'ESGF nonexist (',len(nonexistdict),'): ',nonexistdict
#TEST in testdb without data in ESGF: 
#sys.exit()
###existdict = {'CMIP6.DCPP.DWD.MPI-ESM1-2-HR.dcppC-amv-ExTrop-pos':'20190101'}

tdum2 = time.time()
tesgf = tdum2-tdum
log2.info('ESGF index access time: %i s ( %.2f min )' % (tesgf,tesgf/60))

# 2a. log access error for nonexist with registered DOIs
if len(testflag) == 0:
    for n in nonexistdict:
        if n in cit_dict and cit_dict[n] == 'registered':
            log2.error('%s' % n)
            logMessage('ERROR',1,'No Data found in ESGF','checkDOI: No data found in ESGF for \'%s\'' % n,n,'checkDOI: No data found in eSGF for \'%s\'' % n)


tdum3 = time.time()
tdoi  = tdum3-tdum2
log2.info('DOI registration time:  %i s ( %.2f min )' % (tdoi,tdoi/60))

# INSERT into log_job (autocommit)
# check db connection...
if len(testflag) == 0 and len(nonexistdict) > 0:
    #log2.info('Total number of registered DOIs: %s' % str(num_dois))
    #print 'Total number of registered DOIs: %s' % str(num_dois)
    try:
        conn.ping()
    except:
        try:
            conn  = cx_Oracle.connect(sdbfile)
            cur = conn.cursor()
        except cx_Oracle.DatabaseError as f:
            error, = f.args
            log2.error("Cannot re-connect to DB=\'%s\'. Check password (%i: %s)" % (':'.join(re.split(':',sdbfile)[:2]),error.code,error.message.strip()))
            sys.exit()

    if len(nonexistdict) > 0:
        try:
            sql='insert into log_jobs (ID,NAME,CALL,LOG_TYPE,LOG_MESS,ERR_CODE,ERR_MESS,TIMESTAMP) values (cmip6_cite_test.seq_log_jobs.nextVal,\'CHECK_DOI\',\'%s\',\'INFO\',\'Total number DOIs without ESGF data: %s\',0,\'\',SYSTIMESTAMP)' % (' '.join(sys.argv),str(len(nonexistdict)))
            cur.execute( sql )
        except cx_Oracle.DatabaseError as f:
            error, = f.args
            log2.error('insert log_jobs failed: %s - %s ' % (str(error.code),error.message.strip()))
    conn.commit()

cur.close()
conn.close()

tend = time.time()
time = tend-tstart
log2.info('Total run time for script: %i s ( %.2f min )' % (time,time/60))
#log2.info(' - data base access time:  %i s ( %.2f min )' % (tdbaccess,tdbaccess/60))
#log2.info(' - ESGF index access time: %i s ( %.2f min )' % (tesgf,tesgf/60))
#log2.info(' - DOI registration time:  %i s ( %.2f min )' % (tdoi,tdoi/60))


