#! /usr/bin/env python
""" register DOI for entries with published datasets
Version: V0.9 2024-10-14: db change (stockhause@dkrz.de)
Version: V0.8 2021-07-01: checkDOI functionality excluded; only entries ready for registration and changed querySolr
Version: V0.7 2020-04-16: tcera1 -> testdb
Version: V0.6 2019-12-03: DB hardware/software exchange
Version: V0.5 2019-07-02, check of ESGF version format added -> exit prior to DataCite metadata creation
Version: V0.4 2019-05-16, replace ESGF Search API (searchESGF) by Solr API (querySolr)
Version: V0.3 2018-11-16, support for cx_Oracle 7
Version: V0.2 2018-03-02, first version with devel-operational
Version: V0.1 2017-01-17, stockhause@dkrz.de"""

# Usage: ./registerDOI.py [<test|testdb|testdbtest>] 

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
import querySolr2
import datacite_mod
import time
from xml.etree import ElementTree
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

    log.error('%s: %s - %s ' % (estring,str(ecode),emessage.strip()))
    conn.rollback()

    # INSERT into log_job
    dumerr=re.sub('\'','\'\'',str(ecode)+' : '+emessage.strip())
    try:
        conn2 = cx_Oracle.connect(sdbfile)
        cur2 = conn2.cursor()
        sql='insert into log_jobs (ID,NAME,CALL,LOG_TYPE,LOG_MESS,ERR_CODE,ERR_MESS,TIMESTAMP) values (cmip6_cite_test.seq_log_jobs.nextVal,\'REGISTER_DOI\',\'%s\',\'ERROR\',\'%s\',1,\'%s\',SYSTIMESTAMP)' % (' '.join(sys.argv),estring,dumerr)
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
        sql='insert into log_db (ID,NAME,SQL,ERR_CODE,ERR_MESS,TIMESTAMP) values (cmip6_cite_test.seq_log_db.nextVal,\'REGISTER_DOI:%s\',\'%s\',%i,\'%s\',SYSTIMESTAMP)' % (lname,re.sub('\'','\'\'',lshortmess),ecode,re.sub('\'','\'\'',emess))
        cur.execute( sql )
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        log.error("log_db insert failed: \'%s\' (reported error:\'%s: %s\')" % (sql,str(error.code),error.message))

    if len(testflag) == 0:
        conn.commit()
    if ecode == 0 :
        log.info(lmess)
    else:
        log.error((lmess+' - '+str(ecode)+': '+emess).strip())

def logXML(lmess,drs,doi,version,xmlfile):
    """logXML: store registered DataCite XML in log_xml table"""

    try:
        log.info(lmess)
        clob = cur.var(cx_Oracle.CLOB)
        # MS 2019-02-04
        #clob.setvalue(0, open(xmlfile,'r').read())
        with open(xmlfile,'r') as fxml:
            clob.setvalue(0,fxml.read())
        # insert in LOG_XML
        instemp = 'insert into log_xml (id,drs_name,doi,version,timestamp,xml) values (cmip6_cite_test.seq_log_xml.nextval,:drs,:doi,:version,SYSTIMESTAMP,:xml)'
        cur.execute(instemp,{'drs':drs,'doi':doi,'version':version,'xml':clob},)
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        log.error("log_xml insert failed (reported error:\'%s: %s\')" % (str(error.code),error.message))

    if len(testflag) == 0:
        conn.commit()


def resetStatus(pid):
    """resetStatus: reset status prior to db changes in case of error"""

    cur.prepare("update externalid set external_pid_status=\'initial\' where external_pid =:pid")
    try:
        cur.execute(None,pid=pid)
    except cx_Oracle.DatabaseError as f:
        error, = f.args
        logMessage('ERROR',error.code,error.message,'UPDATE externalid set external_pid_status=\'initial\' for %s' %  pid,'reset_status','UPDATE externalid set external_pid_status=initial for %s' %  pid)

    if len(testflag) == 0:
        conn.commit()


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
# MS 2024-10-14: testdb -> tcera; delphi7-scan.dkrz.de -> cera-db.dkrz.de/cera-testdb.dkrz.de
#db='pcera.dkrz.de'
db='pcera'
db2='cera-db.dkrz.de'
fileflag=''
try:
    testflag = sys.argv[1]
    #print 'TEST: %s' % testflag
    if testflag == 'testdb' or testflag == 'testdbtest':
        # MS 2020-04-16: tcera1 -> testdb
        # MS 2019-12-03: testdb -> tcera1
        db='tcera'
        db2='cera-testdb.dkrz.de'
        #db='testdb'
        #db='tcera1'
        fileflag='test'
    if testflag == 'testdbtest':
        testflag='test'
    elif testflag=='testdb':
        testflag=''
except:
    testflag = ''

# set xml output dir
outdir = config["xmloutdir"]
# MS 2019-12-03: testdb -> tcera1
# MS 2020-04-16: tcera1 -> testdb
#if db=='tcera1':
if db=='testdb':
    outdir=outdir+fileflag  # to be changed

# configure logfile and set log file name
formatter=logging.Formatter('%(asctime)s - %(levelname)s: %(message)s')
formatter2=logging.Formatter('%(levelname)s: %(message)s')

LOG_FILENAME = config["logdir"]+"/registerDOI"+fileflag+".log"
log = logging.getLogger('log')
log.setLevel(logging.INFO)
console1=logging.FileHandler(LOG_FILENAME)
console1.setLevel(logging.INFO)
sconsole1=logging.StreamHandler()
sconsole1.setLevel(logging.WARN)
console1.setFormatter(formatter)
sconsole1.setFormatter(formatter2)
log.addHandler(console1)
log.addHandler(sconsole1)
log.propagate = False

# configure second nodata log file
#LOG_FILENAME2 = config["logdir"]+"/nodataESGF"+fileflag+".log"
#log2 = logging.getLogger('log2')
#console2=logging.FileHandler(LOG_FILENAME2)
#console2.setFormatter(formatter)
#log2.setLevel(logging.INFO)
#log2.addHandler(console2)

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
    # MS 2024-10-14: delphi7-scan.dkrz.de -> cera-db.dkrz.de/cera-testdb.dkrz.de
    #sdbfile =  cuser+'/'+cpw+'@'+'( DESCRIPTION = ( ADDRESS_LIST = ( ADDRESS = ( PROTOCOL = TCP ) ( HOST = oda-scan.dkrz.de ) ( PORT = 1521 ) ) ) ( CONNECT_DATA = ( SERVER = DEDICATED ) ( SERVICE_NAME = '+db+' ) ))'
    #sdbfile =  cuser+'/'+cpw+'@'+'( DESCRIPTION = ( ADDRESS_LIST = ( ADDRESS = ( PROTOCOL = TCP ) ( HOST = delphi7-scan.dkrz.de ) ( PORT = 1521 ) ) ) ( CONNECT_DATA = ( SERVER = DEDICATED ) ( SERVICE_NAME = '+db+' ) ))'
    sdbfile =  cuser+'/'+cpw+'@'+'( DESCRIPTION = ( ADDRESS_LIST = ( ADDRESS = ( PROTOCOL = TCP ) ( HOST = '+db2+' ) ( PORT = 1521 ) ) ) ( CONNECT_DATA = ( SERVER = DEDICATED ) ( SERVICE_NAME = '+db+' ) ))'
    conn = cx_Oracle.connect(sdbfile)
    cur = conn.cursor()
except cx_Oracle.DatabaseError as e:
    error, = e.args
    log.error("Cannot connect to DB=\'%s\'. Check password (%i: %s)" % (':'.join(re.split(':',sdbfile)[:2]),error.code,error.message.strip()))
    #raise IOError, "\nCannot connect to DB=\'%s\'. Check password\n" % cuser
    sys.exit()


# 1. get list of drs_names in citation db for CMIP6 with completed metadata
# MS 2021-06-29: get only completed entries with status='initial' ready for registration 
cur.prepare('select d.drs_map_esgf,e.external_pid_status from policy_drs d,citation c, externalid e, list_connect l where d.project_acronym in ( \'IPCC-AR6_CMIP6\',\'CMIP6_input4MIPs\' ) and d.policy_drs_id=c.policy_drs_id and c.externalid_id=e.external_id and l.citation_id=c.citation_id and l.contact_type_id=3000006 and e.external_pid_status=\'initial\' and (e.external_pid like \'10.22033/ESGF/%\' or e.external_pid like \'10.5072/ESGF/%\') order by d.drs_map_esgf')
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
log.info('data base access time:  %i s ( %.2f min )' % (tdbaccess,tdbaccess/60))

# 2. get list of ESGF published entities
# AF/MS 2019-05-16: replace searchESGF by querySolr functionality
#search = searchESGF.SearchESGF()
# MS 2021-06-29: check ESGF by list of unregistered DOIs - cit_dict.keys()
search = querySolr2.QuerySolr()
existdict = {}
#nonexistdict = {}
comment = ''
mip_list = []

# CMIP6 and input4MIPs list
try:
    CMIP6_list = [m for m in cit_dict.keys()]
    CMIP6_list.sort()
    #print len(CMIP6_list)
    for m in CMIP6_list:
        #print len(re.findall('\.',m))
        mip='.'.join(re.split('\.',m)[0:4])
        if mip not in mip_list:
            mip_list.append(mip)

    #print len(mip_list),len(CMIP6_list)
    #print 'DB:CMIP6_list (',len(CMIP6_list),') - ',CMIP6_list
    #(existdict,nonexistdict,comment) = search.getList(CMIP6_list,'CMIP6','institution_id', [4,5], 0,config["nodes"+fileflag])
    (existdict,comment) = search.get_list(mip_list,CMIP6_list,'institution_id',  0,config["nodes"+fileflag])
except Exception as err:
    errorHandling(1, ' '.join(str(err.args)),'CMIP6: searchESGF failed')  
    errExit() 

if len(comment) > 0: # error exit of search.getList
    errorHandling(1, comment,'CMIP6: searchESGF failed')  
    errExit() 

#print 'ESGF exist    (',len(existdict),'): ',existdict
#print 'Comment:',comment
#print 'ESGF nonexist (',len(nonexistdict),'): ',nonexistdict
#TEST in testdb without data in ESGF: 

###existdict = {'CMIP6.DCPP.DWD.MPI-ESM1-2-HR.dcppC-amv-ExTrop-pos':'20190101'}

tdum2 = time.time()
tesgf = tdum2-tdum
#print tesgf/60
#sys.exit()
log.info('ESGF index access time: %i s ( %.2f min )' % (tesgf,tesgf/60))

# 2a. log access error for nonexist with registered DOIs
#if len(testflag) == 0:
#    for n in nonexistdict:
#        if n in cit_dict and cit_dict[n] == 'registered':
#            log2.error('%s' % n)

# print for test case if registration required
if len(testflag) > 0:
    for e in existdict:
        #if e in cit_dict and cit_dict[e] == 'initial':
        print 'REGISTER: %s' % e
    sys.exit()


# 3. register doi for exist without registered DOIs
try:
    dc = datacite_mod.DataCite(mydir)
except Exception as err:
    errorHandling(1, ' '.join(str(err.args)),'datacite_mod: datacite_mod instantiation failed')
    errExit()


if len(testflag) == 0:
    dctest = 0
else:
    dctest = 1


# loop over potential new DOI registrations
num_dois = 0
for e in existdict:
    xmlfile = ''
    # MS 2019-07-02: check ESGF version format
    if len(existdict[e])!=8:
        print 'ERROR: malformed version: %s for %s' % (existdict[e],e)
        continue

    # check for not yet registered
    #if e in cit_dict and cit_dict[e] != 'registered':
    # check db connection...
    try:
        conn.ping()
    except:
        try:
            conn  = cx_Oracle.connect(sdbfile)
            cur = conn.cursor()
        except cx_Oracle.DatabaseError as f:
            error, = f.args
            log.error("Cannot re-connect to DB=\'%s\'. Check password (%i: %s)" % (':'.join(re.split(':',sdbfile)[:2]),error.code,error.message.strip()))
            sys.exit()

    # 3a. set metadata externalid,citation-> get doi
    pid=cur.var(cx_Oracle.STRING)
    cur.prepare("update externalid set external_pid_status=\'registered\',timestamp=SYSTIMESTAMP where external_id = (select e.external_id from externalid e,citation c,policy_drs d  where d.drs_map_esgf=:drs and c.policy_drs_id=d.policy_drs_id and c.externalid_id=e.external_id) returning external_pid into :x")
    try:
        cur.execute(None,drs=e, x=pid)
    except cx_Oracle.DatabaseError as f:
        error, = f.args
        logMessage('ERROR',error.code,error.message,'UPDATE externalid set external_pid_status=\'registered\' for %s' %  e,'register_doi','UPDATE externalid set external_pid_status=registered for %s' %  e)
        continue

    if type(pid.getvalue()) is list:
        mypid = pid.getvalue()[0]
    else:
        mypid = pid.getvalue()

    cur.prepare("update citation set publication_year=:year, dateversion=:vers, modification_date=SYSTIMESTAMP where externalid_id = (select e.external_id from externalid e where e.external_pid=:mypid)")
    try:
        cur.execute(None,year=int(existdict[e][:4]),vers=existdict[e][:4]+'-'+existdict[e][4:6]+'-'+existdict[e][-2:],mypid=mypid)
    except cx_Oracle.DatabaseError as f:
        error, = f.args
        logMessage('ERROR',error.code,error.message,'UPDATE citation set publication_year=%s, dateversion=%s for %s - %s' % ( existdict[e][:4],existdict[e][:4]+'-'+existdict[e][4:6]+'-'+existdict[e][-2:],mypid,e),'register_doi','UPDATE citation set publication_year=%s, dateversion=%s for %s - %s' % ( existdict[e][:4],existdict[e][:4]+'-'+existdict[e][4:6]+'-'+existdict[e][-2:],mypid,e))
        resetStatus(mypid)
        continue

    # commit db metadata changes in externalid and citation: required for XML creation
    if len(testflag) == 0:
        conn.commit()

    print 'register doi %s for %s with version %s' % ( mypid,e,existdict[e])

    # 3b. create DataCite metadata
    try:
        mydoi=get_doimd.GetDoi(e,outdir,get_doimd_dir,existdict[e],db)
        (f,message,url) = mydoi.getDoi()  # f. (error) return code
    except Exception as err:
        resetStatus(mypid)
        logMessage('ERROR',1, ' '.join(str(err.args)),'getDoi: Metadata creation failed for %s' % e,'get_doimd','getDoi: Metadata creation failed for %s' % e)
        continue

    # no error in metadata creation
    if f == 0:
        # find xml in outdir
        for ff in os.listdir(outdir):
            if re.search(re.sub('\.','_',e)+'.xml',ff):
                xmlfile = os.path.abspath(outdir+'/'+ff)
                break

        # 3c. register metadata,DOI
            
        # register metadata
        try:
            (emess,ecode)=dc.callDataCite('metadata','POST',dctest,xmlfile,mypid,'')
            print 'Metadata: %s:\n%s\n' % (str(ecode),emess) # 201
        except Exception as err:
            resetStatus(mypid)
            logMessage('ERROR',1, ' '.join(str(err.args)),'callDataCite: DataCite metadata registration failed for %s' % e,'datacite_mod','callDataCite: DataCite metadata registration failed for %s' % e)
            continue

        # DataCite API http response code handling
        if ecode > 299: # http response code
            resetStatus(mypid)
            logMessage('ERROR',ecode,emess,'callDataCite: DataCite metadata registration failed for %s : %s - %s' % (mypid,str(ecode),str(emess)),'datacite_mod','callDataCite: DataCite metadata registration failed for %s : %s - %s' % (mypid,str(ecode),str(emess)))
            continue

        # register DOI
        try:
            (emess,ecode)=dc.callDataCite('doi','POST',dctest,'',mypid,url)
            print 'DOI: %i:\n%s\n' % (ecode,emess) # 201
        except Exception as err:
            resetStatus(mypid)
            logMessage('ERROR',1, ' '.join(str(err.args)),'callDataCite: DataCite DOI registration failed for %s' % e,'datacite_mod','callDataCite: DataCite DOI registration failed for %s' % e)
            continue

        # DataCite API http response code handling
        if ecode > 299: # http response code
            resetStatus(mypid)
            logMessage('ERROR',ecode,emess,'callDataCite: DataCite DOI registration failed for %s : %s - %s' % (mypid,ecode,emess),'datacite_mod','callDataCite: DataCite DOI registration failed for %s : %s - %s' % (mypid,ecode,emess))
            continue


        # store registered metadata XML in db
        logXML('DOI registered: %s - doi=%s with url=%s in version=%s' % (e,mypid,url,existdict[e]),e, mypid,existdict[e],xmlfile)

    # error handling for metadata creation
    elif f == 1:
        logMessage('ERROR',f,message,'getDoi: xml generation failed: %s - %s' % (e,message),'get_doimd','getDoi: xml generation failed: %s - %s' % (e,message))
        resetStatus(mypid)
        continue
    else:
        logMessage('WARN',f,message,'getDoi: xml generation failed: %s - %s' % (e,message),'get_doimd','getDoi: xml generation failed: %s - %s' % (e,message))
        resetStatus(mypid)
        continue

    # 3d. insert log db messages and increase number of registered dois
    num_dois += 1
    logMessage('INFO',0,'','UPDATE externalid set external_pid_status=\'registered\' for %s - %s' % ( mypid,e),'externalid','UPDATE externalid set external_pid_status=registered for %s - %s' % ( mypid,e))
    logMessage('INFO',0,'','UPDATE citation set publication_year=%s, dateversion=%s for %s - %s' % ( existdict[e][:4],existdict[e][:4]+'-'+existdict[e][4:6]+'-'+existdict[e][-2:],mypid,e),'citation','UPDATE citation set publication_year=%s, dateversion=%s for %s - %s' % ( existdict[e][:4],existdict[e][:4]+'-'+existdict[e][4:6]+'-'+existdict[e][-2:],mypid,e))

    #else:
    #    #print 'No DOI needs to be registered for %s' % e
    #    pass

tdum3 = time.time()
tdoi  = tdum3-tdum2
log.info('DOI registration time:  %i s ( %.2f min )' % (tdoi,tdoi/60))

# INSERT into log_job (autocommit)
# check db connection...
if len(testflag) == 0 and num_dois>0:
    log.info('Total number of registered DOIs: %s' % str(num_dois))
    #print 'Total number of registered DOIs: %s' % str(num_dois)
    try:
        conn.ping()
    except:
        try:
            conn  = cx_Oracle.connect(sdbfile)
            cur = conn.cursor()
        except cx_Oracle.DatabaseError as f:
            error, = f.args
            log.error("Cannot re-connect to DB=\'%s\'. Check password (%i: %s)" % (':'.join(re.split(':',sdbfile)[:2]),error.code,error.message.strip()))
            sys.exit()

    if num_dois > 0:
        try:
            sql='insert into log_jobs (ID,NAME,CALL,LOG_TYPE,LOG_MESS,ERR_CODE,ERR_MESS,TIMESTAMP) values (cmip6_cite_test.seq_log_jobs.nextVal,\'REGISTER_DOI\',\'%s\',\'INFO\',\'Total number of registered DOIs: %s\',0,\'\',SYSTIMESTAMP)' % (' '.join(sys.argv),str(num_dois))
            cur.execute( sql )
        except cx_Oracle.DatabaseError as f:
            error, = f.args
            log.error('insert log_jobs failed: %s - %s ' % (str(error.code),error.message.strip()))
    conn.commit()

cur.close()
conn.close()

tend = time.time()
time = tend-tstart
log.info('Total run time for script: %i s ( %.2f min )' % (time,time/60))
#log.info(' - data base access time:  %i s ( %.2f min )' % (tdbaccess,tdbaccess/60))
#log.info(' - ESGF index access time: %i s ( %.2f min )' % (tesgf,tesgf/60))
#log.info(' - DOI registration time:  %i s ( %.2f min )' % (tdoi,tdoi/60))
