#! /usr/bin/env python
""" create updated DataCite metadata files and transfer to oai server
Version: V0.8 2021-02-15: rsync changed
         V0.7 2020-04-16: tcera1 -> testdb
         V0.6 2020-01-06: rsync k204082 -> citeuser (stockhause@dkrz.de),
         V0.5 2019-12-03: DB hardware/software exchange (stockhause@dkrz.de),
         V0.4 2018-03-02, devel-operational set-up (stockhause@dkrz.de),
         V0.3 2017-05-08, stockhause@dkrz.de"""

# Usage: ./create_xml.py [<test|testdb|testdbtest>] 

# ATTENTION:
# 1. Cron job max. 1 times a day because of 'date' usage to extract changed metadata instead of 'timestamp' 
# 2. After error exit or test run -> delete lines in log file because of date

# CREATE DataCite MD when create_flag==True, i.e.: 
# 1.  dateid > last_date (external_id DOI registered or externalid MD updated after last create_XML run) OR
# 2. version > last_date (citation MD change after last create_XML run)
# 3. reftime > last_date: citation reference changed or added since last run of create_xml

import sys,os,re,urllib2,json
from subprocess import Popen, PIPE

# set pathes
mydir=os.path.abspath(os.path.dirname(sys.argv[0]))
get_doimd_dir=mydir
#print mydir
f=os.path.abspath(os.path.relpath(mydir+'/../config.json'))
#print f,open(f,'r').read()
# MS 2019-02-04
#config = json.loads(open(f,'r').read())
config = []
with open(f,'r') as f:
  config = json.loads(f.read())

for l in config["addpaths"]:
    sys.path.append(l)
sys.path.append(mydir)

# import project-specific modules
import get_doimd
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
#os.environ['NLS_LANG']="German_Germany.AL32UTF8"
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


# check ssh connection to dm-oai
rargs=['ssh','citeuser@dm-oai.dkrz.de','echo','\'Hallo\'']
p = Popen( ' '.join(rargs), shell=True, stdout=PIPE, stderr=PIPE)
if (len(p.stderr.readlines())>0 or p.stdout.read().strip() != 'Hallo'): 
    print 'dm-oai unreachable'
    sys.exit()


def errorHandling(ecode,emessage,estring):
    """errorHandling: Error handling with insert in log_job table and add to log file; close db connection"""

    dumerr=str(ecode)+' : '+emessage.strip()
    log.error('%s: %s - %s ' % (estring,ecode,emessage.strip()))
    # MS 2018-10-24: make sure that the last line in output starts with a date
    log.error('error exit')
    conn.rollback()
    cur.close()
    conn.close()

    # INSERT into log_job
    try:
        conn2 = cx_Oracle.connect(sdbfile)
        cur2 = conn2.cursor()
        sql='insert into log_jobs (ID,NAME,CALL,LOG_TYPE,LOG_MESS,ERR_CODE,ERR_MESS,TIMESTAMP) values (cmip6_cite_test.seq_log_jobs.nextVal,\'CREATE_XML\',\'%s\',\'ERROR\',\'%s\',1,\'%s\',SYSTIMESTAMP)' % (' '.join(sys.argv),estring,dumerr)
        cur2.execute( sql )
        conn2.commit()
        cur2.close()
        conn2.close()
    except cx_Oracle.DatabaseError as e:
        pass



def logMessage(ltype,ecode,emess,lmess,lname,lshortmess):
    """logMessage: log insert messages in log_db table"""

    # insert in LOG_DB
    sql='insert into log_db (ID,NAME,SQL,ERR_CODE,ERR_MESS,TIMESTAMP) values (cmip6_cite_test.seq_log_db.nextVal,\'CREATE_XML:%s\',\'%s\',%i,\'%s\',SYSTIMESTAMP)' % (lname,re.sub('\'','\'\'',lshortmess),ecode,re.sub('\'','\'\'',emess))
    cur.execute( sql )
    if len(testflag) == 0:
        conn.commit()
    if ecode == 0 :
        log.info(lmess)
    else:
        log.error((lmess+' - '+str(ecode)+': '+emess).strip())


def logXML(lmess,drs,doi,version,xmlfile):
    """logXML: store registered DataCite XML in log_xml table"""

    log.info(lmess)
    #print lmess
    clob = cur.var(cx_Oracle.CLOB)
    # MS 2019-02-04
    #clob.setvalue(0, open(xmlfile,'r').read())
    with open(xmlfile,'r') as fxml:
        clob.setvalue(0,fxml.read())

    # insert in LOG_XML
    instemp = 'insert into log_xml (id,drs_name,doi,version,timestamp,xml) values (cmip6_cite_test.seq_log_xml.nextval,:drs,:doi,:version,SYSTIMESTAMP,:xml)'
    cur.execute(instemp,{'drs':drs,'doi':doi,'version':version,'xml':clob},)


def oaiTransfer():
    """oaiTransfer: transfer xmls with rsync to OAI server and delete xmls from disk"""

    #MS 2021-02-02/2021-02-15: bug fixed in rsync transfer
    #rargs = ["rsync","-av","--include","*.xml","-e","ssh","--remove-source-files"]
    rargs = ["rsync","-av","-e","ssh","--remove-source-files"]
    mypaths = [outdir+"/*.xml","citeuser@dm-oai.dkrz.de:"+targetdir+"/"]
    #mypaths = [outdir+"/","citeuser@dm-oai.dkrz.de:"+targetdir+"/"]
    rargs.extend(mypaths)
    p = Popen( ' '.join(rargs), shell=True, stdout=PIPE, stderr=PIPE)
    err=[]
    for ll in p.stderr.readlines():
        err.append(ll.strip())
    if len(err)>0:
        return (1,'; '.join(err),'rsync of XMLs failed')

    return (0,'','XML file transfer completed')


# read and analyze options
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
    print 'TEST: %s' % testflag
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

# set dirs
outdir = config["xmloutdir"]+fileflag
targetdir = config["oaitargetdir"+fileflag]

# configure and assign log file
LOG_FILENAME = config["logdir"]+"/create_XML"+fileflag+".log"
log = logging.getLogger()
console=logging.FileHandler(LOG_FILENAME)
formatter=logging.Formatter('%(asctime)s - %(levelname)s: %(message)s')
simple_formatter=logging.Formatter('%(message)s')
console.setFormatter(formatter)
log.setLevel(logging.INFO)
log.addHandler(console)

# get last date without error exit for XML creation for comparison
#last_date=re.split(' ',os.popen('tail -1 '+LOG_FILENAME).read().strip())[0]
# MS 2019-02-04
flog = os.popen('tail -1000 '+LOG_FILENAME)
#for l in os.popen('tail -1000 '+LOG_FILENAME).readlines():
for l in flog.readlines():
    if not re.search('Total number of XMLs created',l.strip()):
        continue
    last_date = re.split(' ',l.strip())[0]
flog.close()

time.strptime(last_date,'%Y-%m-%d')
time.strptime(mydate,'%Y-%m-%d')

# connect to db
#print 'Connect to CERA... %s' % db
cuser  = 'cmip6_cite_test'
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
    log.error("Cannot connect to DB=\'%s\'. Check password (%i: %s)" % (':'.join(re.split(':',sdbfile)[:2]),error.code,error.message.strip()))
    # MS 2018-10-24: make sure that the last line in output starts with a date
    log.error('error exit')
    sys.exit()


# get registered DOIs
cur.prepare('select external_id,external_pid,external_pid_url,TO_CHAR(timestamp,\'YYYY-MM-DD\') from externalid where external_pid_status=\'registered\' and (external_pid like \'10.22033/ESGF/%\' or external_pid like \'10.5072/ESGF/%\') ')
try:
    cur.execute(None, )
except cx_Oracle.DatabaseError as e:
    error, = e.args
    errorHandling(error.code,error.message,'externalid: select from database failed')
    sys.exit()

try:
    res=cur.fetchall()
except:
    print 'nothing to fetch -> exit'
    logMessage('INFO',0,'','No new citation information -> exit','externalid','No new citation information -> exit')
    sys.exit()


# initialize datacite module
try:
    dc = datacite_mod.DataCite(mydir)
except Exception as err:
    errorHandling(1, ' '.join(err.args),'datacite_mod: datacite_mod instantiation failed')
    sys.exit()

# loop over registered DOI results out of database
nums = {'num_xmls':0,'num_dcs':0}
xml_transfer = []
for externalid, externalpid, externalurl, dateid in res:
    create_flag = False
    xmlfile = ''
    drs_name=re.split('/',externalurl)[-1]
    #print drs_name
    time.strptime(dateid,'%Y-%m-%d')

    # 1.  dateid > last_date (external_id DOI registered or externalid MD updated after last create_XML run)
    if dateid>last_date:
        create_flag = True

    # 2. version > last_date (citation MD change after last create_XML run)
    cur.prepare('with last_date as (select max(modification_date) as datel,citation_id as cid from list_connect l group by citation_id) select d.policy_drs_template,d.drs_map_esgf, case when l.datel>ci.modification_date then TO_CHAR(l.datel,\'YYYY-MM-DD\') when l.datel<=ci.modification_date then TO_CHAR(ci.modification_date,\'YYYY-MM-DD\') when ci.modification_date is NULL then TO_CHAR(l.datel,\'YYYY-MM-DD\') end as version from citation ci, policy_drs d, externalid e,last_date l where e.external_id= :id and e.external_id=ci.EXTERNALID_ID and ci.citation_id=l.cid and d.policy_drs_id=ci.policy_drs_id')

    try:
        cur.execute(None, {'id': externalid})
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        print error.code,error.message,'citation: select from database failed for %s' %  externalid

    try:
        (drstemp,drs,version)=cur.fetchone()
    except:
        print 'citation: select from database failed for %s' %  externalid
        continue
        
    #print drstemp,drs
    time.strptime(version,'%Y-%m-%d')
    # version: last md change in citation; last_date: last create_xml run
    # MS 2017-08-07: '>' => '>='
    if version>=last_date:
        create_flag = True


    # MS 2017-08-07: '>' => '>='
    # 3. reftime > last_date: citation reference changed or added since last run of create_xml
    # reftime >= last_date: citation reference changed or added since last run of create_xml
    cur.prepare('select max(TO_CHAR(c.upd_date,\'YYYY-MM-DD\')) as reftime from cera2_upd.upd_citation c, reference r,citation ci where c.citation_id=r.ref_citation_id and r.citation_id=ci.citation_id and ci.externalid_id= :id group by ci.externalid_id')
    try:
        cur.execute(None, {'id': externalid})
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        print error.code,error.message,'citation: select from database failed for %s' %  externalid

    try:
        (reftime)=cur.fetchone()
    except:
        print 'citation: select from database failed for %s' %  externalid
        continue
        
    #print 'reftime',reftime,last_date
    try:
        time.strptime(reftime,'%Y-%m-%d')
        if reftime>=last_date:
            create_flag = True
    except:
        pass


    # CREATE DataCite MD when create_flag==True, i.e.: 
    #print create_flag
    if create_flag == False:
        #print 'No metadata update necessary for \'%s\'' %  drs
        continue

    try:
        mydoi=get_doimd.GetDoi(drs,outdir,get_doimd_dir,re.sub('-','',version),db)
        (e,message,url) = mydoi.getDoi()
    except Exception as err:
        log.error('getDoi: Metadata creation failed for %s' % drs)
        e=1
        message='Unknown error occurred.'

    # no error in metadata creation
    if e == 0:
        log.info('get_doimd: %s - %s' % (drs,message))
    elif e == 1:
        log.error('get_doimd: %s - %s' % (drs,message))
        continue
    else:
        log.warning('get_doimd: %s - %s' % (drs,message))
    print 'get_doimd: %s - %s' % (drs,message)

    # find xml in outdir
    for f in os.listdir(outdir):
        if re.search(re.sub('\.','_',drs)+'.xml',f):
            xmlfile = outdir+'/'+f
            break
    
    nums['num_xmls'] += 1

    # REGISTER MD at DataCite for 
    # dateid < mydate: DOI not registered today (dateid != today)
    if dateid<mydate:
        try:
            if len(testflag) > 0: # test metadata registration '1'
                (emess,ecode)=dc.callDataCite('metadata','POST',1,xmlfile,externalpid,'')
                continue # no logging
            else:
                (emess,ecode)=dc.callDataCite('metadata','POST',0,xmlfile,externalpid,'')

            print 'Metadata: %i:\n%s\n' % (ecode,emess) # 201
        except Exception as err:
            logMessage('ERROR',1, ' '.join(err.args),'callDataCite: DataCite metadata registration failed for %s' % drs,'datacite_mod','callDataCite: DataCite metadata registration failed for %s' % drs)
            continue

        # DataCite API http response code handling
        if ecode > 299: # http response code
            logMessage('ERROR',ecode,emess,'callDataCite: DataCite metadata registration failed for %s : %s - %s' % (externalpid,ecode,emess),'datacite_mod','callDataCite: DataCite metadata registration failed for %s : %s - %s' % (externalpid,ecode,emess))
            continue

        # store registered XML in DB
        logXML('XML created for %s (version:%s doi:%s url:%s)' % (drs,re.sub('-','',version),externalpid,externalurl),drs,externalpid,re.sub('-','',version),xmlfile )
        nums['num_dcs'] += 1

    # add file to file list for transfer to OAI server
    xml_transfer.append(xmlfile)
            
        
if len(xml_transfer) == 0:
    cur.close()
    conn.close()
    sys.exit()

print len(xml_transfer),', '.join(xml_transfer)

if len(testflag) > 0:
    conn.rollback()
else:
    log.info('Total number of XMLs created: %s; total number of DataCite XMLs registered: %s' % (str(nums['num_xmls']),str(nums['num_dcs'])))
    print 'Total number of XMLs created: %s; total number of DataCite XMLs registered: %s' % (str(nums['num_xmls']),str(nums['num_dcs']))
    conn.commit()

    # transfer all created DataCite xmls with rsync to OAI server and delete xmls from disk
    (ecode,emess,estatus) = oaiTransfer()
    if ecode == 0:
        logMessage('INFO',ecode,emess,estatus,'oaiTransfer',estatus)
    else:
        logMessage('ERROR',ecode,emess,estatus,'oaiTransfer',estatus)

    try:
        sql='insert into log_jobs (ID,NAME,CALL,LOG_TYPE,LOG_MESS,ERR_CODE,ERR_MESS,TIMESTAMP) values (cmip6_cite_test.seq_log_jobs.nextVal,\'CREATE_XML\',\'%s\',\'INFO\',\'Total number of XMLs created: %s; total number of DataCite XMLs registered: %s\',0,\'\',SYSTIMESTAMP)' % (' '.join(sys.argv),str(nums['num_xmls']),str(nums['num_dcs']))
        cur.execute( sql )
    except cx_Oracle.DatabaseError as f:
        error, = f.args
        log.error('insert log_jobs failed: %s - %s ' % (str(error.code),error.message.strip()))

    conn.commit()


cur.close()
conn.close()
