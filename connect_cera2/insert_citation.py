#! /usr/bin/env python

""" Insert/Update evolving citation entries 
Version: V0.3 2024-10-14: db change (stockhause@dkrz.de)
         V0.2 2019-12-03: DB hardware/software exchange,
         V0.1 2018-06-05, stockhause@dkrz.de"""

# Usage: ./insert_citation.py [<test>]

import sys,os,re,urllib2
import logging
import json
from operator import itemgetter
try:
    import cx_Oracle
except:
    print "Cannot import module cx_Oracle"
    sys.exit()

# set environment
mydir=os.path.abspath(os.path.dirname(sys.argv[0]))
f=os.path.abspath(os.path.relpath(mydir+'/../config.json'))
config = json.loads(open(f,'r').read())
for l in config["addpaths"]:
    sys.path.append(l)
sys.path.append(mydir) 

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
    """errorHandling: Error exit"""

    log.error('%s: %s - %s ' % (estring,ecode,emessage.strip()))
    conn.rollback()
    cur.close()
    conn.close()
    sys.exit()

def mergeCera(db,db2,cuser,sqls,testflag):
    """connect as cuser and execute sql statements"""

    #print 'Connect to CERA as %s...' % cuser
    fdb=os.path.abspath(os.path.relpath(mydir+'/../.'+cuser+fileflag))
    cpw    = open(fdb,'r').read().strip()
    try:
        # MS 2019-12-03: oda-scan.dkrz.de -> delphi7-scan.dkrz.de
        # MS 2024-10-14: delphi7-scan.dkrz.de -> cera-db.dkrz.de or cera-testdb.dkrz.de
        #sdbfile =  cuser+'/'+cpw+'@'+'( DESCRIPTION = ( ADDRESS_LIST = ( ADDRESS = ( PROTOCOL = TCP ) ( HOST = oda-scan.dkrz.de ) ( PORT = 1521 ) ) ) ( CONNECT_DATA = ( SERVER = DEDICATED ) ( SERVICE_NAME = '+db+' ) ))'
        #sdbfile =  cuser+'/'+cpw+'@'+'( DESCRIPTION = ( ADDRESS_LIST = ( ADDRESS = ( PROTOCOL = TCP ) ( HOST = delphi7-scan.dkrz.de ) ( PORT = 1521 ) ) ) ( CONNECT_DATA = ( SERVER = DEDICATED ) ( SERVICE_NAME = '+db+' ) ))'
        sdbfile =  cuser+'/'+cpw+'@'+'( DESCRIPTION = ( ADDRESS_LIST = ( ADDRESS = ( PROTOCOL = TCP ) ( HOST = '+db2+' ) ( PORT = 1521 ) ) ) ( CONNECT_DATA = ( SERVER = DEDICATED ) ( SERVICE_NAME = '+db+' ) ))'
        conn = cx_Oracle.connect(sdbfile)
        cur = conn.cursor()
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        errorHandling(error.code,error.message,"Cannot connect to DB=\'%s\'. Check password" % (':'.join(re.split(':',sdbfile)[:2])))
        sys.exit()

    # execute sqls and commit
    if len(testflag) == 0:
        for k,v in sqls.iteritems():
            dumlist = []
            try:
                cur.execute(v)
            except cx_Oracle.DatabaseError as e:
                error, = e.args
                errorHandling(error.code,error.message,"Error in executing sql \'%s\'" % v)

        conn.commit()
        log.info('%s merge citation successful' % cuser )

    cur.close()
    conn.close()


# read options and analyze testflag
mydate = os.popen('date +%F').read().strip()

# MS 2019-12-03: pcera.dkrz.de -> pcera
# MS 2024-10-14: delphi7-scan.dkrz.de -> cera-db.dkrz.de
#db='pcera.dkrz.de'
db='pcera'
db2='cera-db.dkrz.de'
fileflag=''
try:
    testflag = sys.argv[1]
    print 'TEST: %s' % testflag
except:
    testflag = ''


# configure logfile and set log file name
LOG_FILENAME = config["logdir"]+"/insert_citation"+fileflag+".log"
log = logging.getLogger()
console=logging.FileHandler(LOG_FILENAME)
formatter=logging.Formatter('%(asctime)s - %(levelname)s: %(message)s')
simple_formatter=logging.Formatter('%(message)s')
console.setFormatter(formatter)
log.setLevel(logging.INFO)
log.addHandler(console)

# read sqls
cera = {}
cera_upd = {}
for l in open(mydir+'/insert_citation.sql','r').readlines():
    if re.match(r'^(\#)',l.strip()) or len(l.strip()) == 0:
        continue            
    key   = re.split(':',l.strip())[0]
    value = ':'.join(re.split(':',l.strip())[1:]).strip()
    if re.search('CERA2_UPD',key):
        cera_upd[key]=value
    else:
        cera[key]=value

# insert into cera2_upd
mergeCera(db,db2,'cera2_upd',cera_upd,testflag)

# update cera2
mergeCera(db,db2,'cera2',cera,testflag)

