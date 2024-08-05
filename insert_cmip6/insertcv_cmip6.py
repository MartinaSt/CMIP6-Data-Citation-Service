#! /usr/bin/env python
""" insert/update drs_entries from CMIP6_CV at https://github.com/WCRP-CMIP/CMIP6_CVs into cmip6_cite_test
Version: V0.7 2020-04-16: tcera1 -> testdb
Version: V0.6 2019-12-03: DB hardware/software exchange
Version: V0.5 2018-11-16: support for cx_Oracle 7
Version: V0.4 2018-03-16: integrate model_connect insert
Version: V0.3 2018-03-02: first version with devel-operational
Version: V0.2 2016-12-14: test option added
Version: V0.1 2016-11-02, stockhause@dkrz.de"""

# Usage: ./insertcv_cmip6.py [<TEST_FLAG>]
# TEST_FLAG: test|testdb|testdbtest
# modelflag: 0:only model entries; 1: model + exp entries for existing model
#
# example calls:
# ./insertcv_cmip6.py <CERA ACCOUNT> <upper INSTITUTE> <modelflag> <TEST_FLAG> 
# ./insertcv_cmip6.py 'MARTINA_STOCKHAUSE' '' 0 test  # model
# ./insertcv_cmip6.py 'MARTINA_STOCKHAUSE' 'CNRM-CERFACS' 0 test
# ./insertcv_cmip6.py 'MARTINA_STOCKHAUSE' 'CMIP.CNRM' 1 test
# ./insertcv_cmip6.py 'ANDREJ_TESTUSER' '' 1 test


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
#print mydir
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
    """errorHandling: Error handling with insert in log_job table and add to log file; close db connection"""

    dumerr=str(ecode)+' : '+emessage.strip()
    log.error('%s: %s - %s ' % (estring,ecode,emessage.strip()))
    conn.rollback()
    cur.close()
    conn.close()
    # INSERT into log_job
    try:
        conn2 = cx_Oracle.connect(sdbfile)
        cur2 = conn2.cursor()
        sql='insert into log_jobs (ID,NAME,CALL,LOG_TYPE,LOG_MESS,ERR_CODE,ERR_MESS,TIMESTAMP) values (cmip6_cite_test.seq_log_jobs.nextVal,\'INSERT_CV\',\'%s\',\'ERROR\',\'%s\',1,\'%s\',SYSTIMESTAMP)' % (' '.join(sys.argv),estring,dumerr)
        cur2.execute( sql )
        conn2.commit()
        cur2.close()
        conn2.close()
    except cx_Oracle.DatabaseError as e:
        pass
    sys.exit()


def logMessage(ltype,lmess,lname,lshortmess):
    """logMessage: log insert messages in log_db table"""

    log.info(lmess)
    # insert in LOG_DB
    sql='insert into log_db (ID,NAME,SQL,ERR_CODE,ERR_MESS,TIMESTAMP) values (cmip6_cite_test.seq_log_db.nextVal,\'INSERT_CV:%s\',\'%s\',0,\'\',SYSTIMESTAMP)' % (lname,lshortmess)
    cur.execute( sql )


def changeModDate(rel_models):
    """changeModDate: update modification date of related model/MIP citation"""

    for m in rel_models:
        cur.prepare("update citation set modification_date=SYSTIMESTAMP where policy_drs_id = (select policy_drs_id from policy_drs where drs_map_esgf=:mod )")
        try:
            cur.execute(None,mod=m)
            logMessage('INFO','UPDATE citation set modification_date=SYSDATE for \'%s\'' % m,m,'UPDATE citation set modification_date=SYSTIMESTAMP for %s' % m)
        except cx_Oracle.DatabaseError as f:
            error, = f.args
            errorHandling(error.code,error.message,'UPDATE citation set modification_date=SYSDATE failed for %s -> exit'  % m )


def getInt(v):
    """getInt: check getValue return value for list or variable type
       due to cx_Oracle change from version 5 returning a value to version 7 returning a list"""

    if type(v.getvalue()) is list:
        return int(v.getvalue()[0])
    else:
        return int(v.getvalue())
    

def insertDb(p,c,e,m):
    """insertDb: insert entry in citation db tables"""

    policydrsid=cur.var(cx_Oracle.NUMBER)
    externid =cur.var(cx_Oracle.NUMBER)
    citeid =cur.var(cx_Oracle.NUMBER)

    log.info('%s INSERTS' % p['drs_map_esgf'])

    # INSERT into policy_drs
    cur.prepare("insert into policy_drs(policy_drs_id,policy_drs_template,project_acronym,drs_map_esgf,esgf_access_link,show) values (cmip6_cite_test.seq_policy_drs.nextVal,:temp,:pro,:drs,:link,:show) returning policy_drs_id into :x")
    try:
        cur.execute(None,temp=p['policy_drs_template'],pro=p['project_acronym'],drs=p['drs_map_esgf'],link=p['esgf_access_link'],show=p['show'], x=policydrsid)
        ipolicydrsid = getInt(policydrsid)
        logMessage('INFO','INSERT %s into %s (id=%i; policy_drs_template=\'%s\'; project_acronym=\'%s\'; esgf_access_link=\'%s\'; show=\'%s\')' % (p['drs_map_esgf'],'policy_drs',ipolicydrsid,p['policy_drs_template'],p['project_acronym'],p['esgf_access_link'],p['show']), p['drs_map_esgf'], 'INSERT %s into %s: policy_drs_template=%s; project_acronym=%s; esgf_access_link; show=%s)' % (p['drs_map_esgf'],'policy_drs',p['policy_drs_template'],p['project_acronym'],p['show'])  )
        num_insert['policy_drs'] += 1
    except cx_Oracle.DatabaseError as f:
        error, = f.args
        errorHandling(error.code,error.message,'INSERT %s into %s failed -> exit'  % (p['drs_map_esgf'],'policy_drs'))

    #print int(policydrsid.getvalue())
    c['policy_drs_id'] = ipolicydrsid
    e['external_pid'] = e['external_pid'] % ( c['policy_drs_id'] )
    m['policy_drs_id'] = ipolicydrsid

    # check insert
    cur.prepare('select * from policy_drs where policy_drs_id = :id')
    try:
        cur.execute(None, {'id': ipolicydrsid})
    except cx_Oracle.DatabaseError as f:
        error, = f.args
        errorHandling(error.code,error.message,'SELECT %s from %s -> exit'  % (p['drs_map_esgf'],'policy_drs'))

    res=cur.fetchone()
    print res

    # INSERT into externalid
    cur.prepare("insert into externalid(external_id,external_pid,external_id_type_id,external_pid_status,external_pid_url,timestamp) values (cmip6_cite_test.seq_externalid.nextVal,:pid,:typeid,:status,:link,SYSTIMESTAMP) returning external_id into :x")
    try:
        cur.execute(None,pid=e['external_pid'],typeid=e['external_id_type_id'],status=e['external_pid_status'],link=e['external_pid_url'], x=externid)
        iexternid = getInt(externid)
        logMessage('INFO','INSERT %s into %s (id=%i; external_pid=\'%s\'; external_id_type_id=%i; external_pid_status=\'%s\'; external_pid_url=\'%s\')' % (p['drs_map_esgf'],'externalid', iexternid,e['external_pid'],e['external_id_type_id'],e['external_pid_status'],e['external_pid_url']), p['drs_map_esgf'], 'INSERT %s into %s (external_pid=%s; external_id_type_id=%i; external_pid_status=%s; external_pid_url)' % (p['drs_map_esgf'],'externalid',e['external_pid'],e['external_id_type_id'],e['external_pid_status']))
        num_insert['externalid'] += 1
    except cx_Oracle.DatabaseError as f:
        error, = f.args
        errorHandling(error.code,error.message,'INSERT %s into %s'  % (p['drs_map_esgf'],'externalid'))

    #print int(externid.getvalue())
    if externid < 1:
        sys.exit()
    c['externalid_id'] = iexternid
    # check insert
    cur.prepare('select * from externalid where external_id = :id')
    try:
        cur.execute(None, {'id': iexternid})
    except cx_Oracle.DatabaseError as f:
        error, = f.args
        errorHandling(error.code,error.message,'SELECT %s from %s'  % (p['drs_map_esgf'],'externalid'))

    res=cur.fetchone()
    print res

    # INSERT into citation
    cur.prepare("insert into citation(citation_id,dateversion,title,publisher,externalid_id,policy_drs_id,publication_year,upd_by,modification_date) values (cmip6_cite_test.seq_citation_id.nextVal,to_char(sysdate,'YYYY-MM-DD'),:tit,:pub,:eid,:drsid,to_char(sysdate,'YYYY'),:upd,SYSTIMESTAMP) returning citation_id into :x")
    try:
        cur.execute(None,tit=c['title'],pub=c['publisher'],eid=c['externalid_id'],drsid=c['policy_drs_id'],upd=c['upd_by'], x=citeid)
        iciteid = getInt(citeid)
        logMessage('INFO','INSERT %s into %s (id=%i; upd_by=\'%s\'; title=\'%s\'; publisher=\'%s\'; externalid_id=%i; policy_drs_id=%i)' % (p['drs_map_esgf'],'citation', iciteid,c['upd_by'],c['title'],c['publisher'],c['externalid_id'],c['policy_drs_id']),p['drs_map_esgf'], 'INSERT %s into %s (upd_by=%s; title; publisher=%s; externalid_id=%i; policy_drs_id=%i)' % (p['drs_map_esgf'],'citation', c['upd_by'],c['publisher'],c['externalid_id'],c['policy_drs_id']))
        num_insert['citation'] += 1
    except cx_Oracle.DatabaseError as f:
        error, = f.args
        errorHandling(error.code,error.message,'INSERT %s into %s'  % (p['drs_map_esgf'],'citation'))

    #print int(citeid.getvalue())
    if citeid < 1:
        sys.exit()
    # check insert
    cur.prepare('select * from citation where citation_id = :id')
    try:
        cur.execute(None, {'id': iciteid})
    except cx_Oracle.DatabaseError as f:
        error, = f.args
        errorHandling(error.code,error.message,'SELECT %s from %s'  % (p['drs_map_esgf'],'citation'))

    res=cur.fetchone()
    print res

    # model_connect
    # 1. get modelid
    cur.prepare('select model_id from models where model_acronym = :macr and institution_acronym = :iacr')
    try:
        cur.execute(None, {'macr':m['model_acronym'],'iacr':m['institution_acronym']})
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        errorHandling(error.code,error.message,'select from database failed (select * from models where model_acronym =  \'%s\' and institution_acronym =   \'%s\') -> exit' % (m['model_acronym'],m['institution_acronym']))

    try:
        (modelid)=cur.fetchone()
    except:
        # skip if not unique
        log.error('model_connect: no unique model found in db (number of model entries: %i) -> continue' % len(res))
        return

    if not modelid:
        print 'model_connect: no model_id found in database. Please run generateModel.py first or correct database entries'
        log.error('model_connect: no model_id found in database. Please run generateModel.py first or correct database entries')
        sys.exit()

    # 2. INSERT into model_connect
    cur.prepare("insert into model_connect (model_id,policy_drs_id) select :mid1, :pid1 from dual where not exists(select * from model_connect where model_id=:mid2 and policy_drs_id=:pid2)")
    try:
        cur.execute(None,mid1=int(modelid[0]),pid1=m['policy_drs_id'],mid2=int(modelid[0]),pid2=m['policy_drs_id'])
        logMessage('INFO','INSERT (model_id=%i,policy_drs_id=%i) into %s for \'%s\'' % (int(modelid[0]),m['policy_drs_id'],'model_connect',m['drs_map_esgf']),m['drs_map_esgf'],'INSERT (model_id=%i,policy_drs_id=%i) into %s for %s' % (int(modelid[0]),m['policy_drs_id'],'model_connect',m['drs_map_esgf']))
        num_insert['model_connect'] += 1
    except cx_Oracle.DatabaseError as f:
        error, = f.args
        errorHandling(error.code,error.message,'INSERT %s into %s (model_id=%i,policy_drs_id=%i)'  % (m['drs_map_esgf'],'model_connect',int(modelid[0]),m['policy_drs_id']))




# read options and analyze testflag
mydate = os.popen('date +%F').read().strip()

upd_by=sys.argv[1]
spec_inst=sys.argv[2]
modelflag=int(sys.argv[3])

# MS 2019-12-03: pcera.dkrz.de -> pcera
#db='pcera.dkrz.de'
db='pcera'
fileflag=''
try:
    testflag = sys.argv[4]
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

# configure logfile and set log file name
LOG_FILENAME = config["logdir"]+"/insertcv_cmip6"+fileflag+".log"
log = logging.getLogger()
console=logging.FileHandler(LOG_FILENAME)
formatter=logging.Formatter('%(asctime)s - %(levelname)s: %(message)s')
simple_formatter=logging.Formatter('%(message)s')
console.setFormatter(formatter)
log.setLevel(logging.INFO)
log.addHandler(console)

# update CMIP6_CVs content from git
try:
    print os.popen('cd '+mydir+'/CMIP6_CVs;git pull https://github.com/WCRP-CMIP/CMIP6_CVs; cd '+mydir).read()
except:
    log.error('git pull for update of CVs failed -> exit')
    raise

# walk through tables in 'CMIP6_CVs'
os.chdir(mydir+'/CMIP6_CVs')
for root, dirs, files in os.walk(".", topdown=False):
    for f in files:
        if not re.search('.json',f):
            continue
        print 'read ',f
        try:
            js2 = json.loads(open(f,'r').read())
        except:
            log.error('Error in reading %s' % f)
            continue
        
        if re.search('_activity_id.json',f):
            cv_mips = js2
        elif re.search('6_experiment_id.json',f):
            # MS 2016-09-02:error correction still required
            js3 = {}
            for k,v in js2["experiment_id"].iteritems():
                if type(v["activity_id"]) != list:
                    dumv = {}
                    dum = []
                    dum.append(v["activity_id"])
                    for k2,v2 in v.iteritems():
                        if k2 == "activity_id":
                            dumv[k2] = dum
                        else:
                            dumv[k2] = v2
                    js3[k] = dumv
                else:
                    js3[k] = v
                
            cv_experimentsx = js3
        elif re.search('_institution_id.json',f):
            dum_institutes = js2
        elif re.search('_source_id.json',f):
            dum_models = js2
        else:
            print 'SKIP %s' % f

# MS: Hacking PCMDI-test inserts
#dum_models['source_id']['PCMDI-test-1-0']['activity_participation']=[u'CMIP',u'C4MIP']
#print dum_models['source_id']['PCMDI-test-1-0']
#sys.exit()

# merge institutes/models
cv_modelsx = {}
for k,v in dum_models["source_id"].iteritems():
    for k2,v2 in dum_institutes["institution_id"].iteritems():
        if k2 in v['institution_id']:
            dum_dict = {'source':v['label'],'institution_id':k2,'institution':v2}
            dum_dict['mod_details']=v
            cv_modelsx[k+'$'+k2]=dum_dict


cv_models = sorted(cv_modelsx.items())
cv_experiments = sorted(cv_experimentsx.items())
# experiments belonging to multiple mips
if testflag=='test':
    print '\nExperiments belonging to multiple mips:'
    for (k,v) in cv_experiments:
        if len(v['activity_id'])>1:
            for vv in v['activity_id']:
                print k,vv
#sys.exit()

# init to write citation entry into db 
project='IPCC-AR6_CMIP6'
project_id='CMIP6'
publisher='Earth System Grid Federation'
modelentries2 = []
sql_lines = []
policy_drs = []
externalid = []
citation = []
modelconn = []
num_drs = {'exp':0,'model':0}
num_insert = {'policy_drs':0,'externalid':0,'citation':0,'model_connect':0}

# 1. prepare model/MIP granularity
drs_template = '%s %s %s %s'
drsid_template = '%s.%s.%s.%s'
title_template = '%s %s model output prepared for %s %s'

# ?activity_id -> activity_drs???
# MS 2019-12-03: pcera.dkrz.de -> pcera
#if db == 'pcera.dkrz.de':
if db == 'pcera':
    esgf_template = 'http://esgf-data.dkrz.de/search/cmip6-dkrz/?mip_era=%s&activity_id=%s&institution_id=%s&source_id=%s'
    url_template ='http://cera-www.dkrz.de/WDCC/meta/CMIP6/'+drsid_template
    pid_template ='10.22033/ESGF/CMIP6.%s'
else:
    esgf_template = 'http://esgf-fedtest.dkrz.de/search/testproject/?mip_era=%s&activity_id=%s&institution_id=%s&source_id=%s'
    url_template ='http://cera-www.dkrz.de/WDCC/testmeta/CMIP6/'+drsid_template
    pid_template ='10.5072/ESGF/CMIP6.%s'

# walk through cv content 
activities = []
for (k,v) in cv_experiments:
    for (ik,iv) in cv_models:
        if len(iv['mod_details']['activity_participation']) > 0 and iv['mod_details']['activity_participation'][0] != '' :   # initial list has none empty entries!
            activities =  iv['mod_details']['activity_participation']
            # add mandatory CMIP activity (DECK experiment) if required
            if 'CMIP' not in activities:
                activities.append('CMIP')
            flag_break = 0
            for vv in v['activity_id']:
                if vv not in iv['mod_details']['activity_participation']:
                    continue
                else:
                    flag_break = 1
            if flag_break == 0: # not found in activity list
                break
        else: # no activity list available: set at least CMIP
            activities=['CMIP']
 
        # build insert statements
        for a in activities:
            dumentry = {}
            dumextid = {}
            dumcite  = {}
            dummod   = {}

            # policy_drs
            dumentry['level'] = 'model'
            dumentry['policy_drs_id'] = 'cmip6_cite_test.seq_policy_drs.nextVal'
            dumentry['policy_drs_template'] = drs_template % (project_id,a,iv['institution_id'],re.split('\$',ik)[0])
            dumentry['project_acronym'] = project
            dumentry['drs_map_esgf'] = drsid_template % (project_id,a,iv['institution_id'],re.split('\$',ik)[0])
            dumentry['esgf_access_link'] = esgf_template % (project_id,a,iv['institution_id'],re.split('\$',ik)[0])
            dumentry['show'] = 'Y'

            # citation
            dumcite['citation_id'] = 'cmip6_cite_test.seq_citation_id.nextVal'
            dumcite['dateversion'] = 'to_char(sysdate,\'YYYY-MM-DD\')'
            dumcite['title'] = title_template % (iv['institution_id'],re.sub(' ','',iv['mod_details']['label']),project_id,a)
            dumcite['publisher'] = publisher
            dumcite['externalid_id'] = 0
            dumcite['policy_drs_id'] = 0
            dumcite['publication_year'] = 'to_char(sysdate,\'YYYY\')'
            dumcite['upd_by'] = upd_by

            # externalid
            dumextid['external_id'] = 'cmip6_cite_test.seq_externalid.nextVal'
            dumextid['external_pid'] = pid_template #% (dumentry['policy_drs_id'])
            dumextid['external_id_type_id'] = 2
            dumextid['external_pid_status'] = 'initial'
            dumextid['external_pid_url'] = url_template % (project_id,a,iv['institution_id'],re.split('\$',ik)[0])
            dumextid['timestamp'] = 'SYSTIMESTAMP'

            # model_connect details
            dummod['drs_map_esgf'] = drsid_template % (project_id,a,iv['institution_id'],re.split('\$',ik)[0])
            dummod['model_acronym'] = re.split('\$',ik)[0]
            dummod['institution_acronym'] = re.split('\$',ik)[1]

            drs = drs_template % (project_id,a,iv['institution_id'],re.split('\$',ik)[0])
            drsid = drsid_template % (project_id,a,iv['institution_id'],re.split('\$',ik)[0])
            esgf = esgf_template % (project_id,a,iv['institution_id'],re.split('\$',ik)[0])
            temp = 'insert into policy_drs values (cmip6_cite_test.seq_policy_drs.nextVal,\'%s\',\'%s\',\'%s\',\'%s\',\'%s\');' % (drs,project,drsid,esgf,dumentry['show'])
            if temp in sql_lines: # check if already in prepared insert statements
                continue
            num_drs['model'] += 1
            sql_lines.append('insert into policy_drs values (cmip6_cite_test.seq_policy_drs.nextVal,\'%s\',\'%s\',\'%s\',\'%s\',\'%s\');' % (drs,project,drsid,esgf,dumentry['show']))
            policy_drs.append(dumentry)
            externalid.append(dumextid)
            citation.append(dumcite)
            modelconn.append(dummod)

# 2. prepare experiment granularity
title_template = '%s %s model output prepared for %s %s %s'
drs_template = '%s %s %s %s %s'
drsid_template = '%s.%s.%s.%s.%s'

# ?activity_id -> activity_drs???
# MS 2019-12-03: pcera.dkrz.de -> pcera
#if db == 'pcera.dkrz.de':
if db == 'pcera':
    esgf_template = 'http://esgf-data.dkrz.de/search/cmip6-dkrz/?mip_era=%s&activity_id=%s&institution_id=%s&source_id=%s&experiment_id=%s'
    url_template ='http://cera-www.dkrz.de/WDCC/meta/CMIP6/'+drsid_template
    pid_template ='10.22033/ESGF/CMIP6.%s'
else:
    esgf_template = 'http://esgf-fedtest.dkrz.de/search/testproject/?mip_era=%s&activity_id=%s&institution_id=%s&source_id=%s&experiment_id=%s'
    url_template ='http://cera-www.dkrz.de/WDCC/testmeta/CMIP6/'+drsid_template
    pid_template ='10.5072/ESGF/CMIP6.%s'

# experiment case: modelflag:1
if modelflag >= 1:
    print 'EXPERIMENTS:'
    for (k,v) in cv_experiments:
        for a in v['activity_id']:
            # in case ONLY TIER1 required
            #if int(v['tier'].strip()) != 1:
            #    continue
            print 'CMIP6 %s %s (tier %s)' % (a,k,v['tier'].strip())

            for (ik,iv) in cv_models:
                # 2017-03-23: new list "activity_participation" added in CV for source_id
                if a not in iv['mod_details']['activity_participation'] and a != 'CMIP':   # initial list has one empty entry!
                    continue
                else:
                    pass
                
                flag_break = 0 # flag to control break 
                for vv in v['activity_id']:
                    if vv not in iv['mod_details']['activity_participation']:
                        continue
                    else: # continue for activity_id is found
                        flag_break = 1

                    if flag_break == 0: # break for activity_id is not found
                        break

                # prepare inserts
                dumentry = {}
                dumextid = {}
                dumcite  = {}
                dummod   = {}
                num_drs['exp'] += 1

                # policy_drs
                dumentry['level'] = 'exp'
                dumentry['policy_drs_id'] = 'cmip6_cite_test.seq_policy_drs.nextVal'
                dumentry['policy_drs_template'] = drs_template % (project_id,a,iv['institution_id'],re.split('\$',ik)[0],k)
                dumentry['project_acronym'] = project
                dumentry['drs_map_esgf'] = drsid_template % (project_id,a,iv['institution_id'],re.split('\$',ik)[0],k)
                dumentry['esgf_access_link'] = esgf_template % (project_id,a,iv['institution_id'],re.split('\$',ik)[0],k)
                dumentry['show'] = ''

                # citation
                dumcite['citation_id'] = 'cmip6_cite_test.seq_citation_id.nextVal'
                dumcite['dateversion'] = 'to_char(sysdate,\'YYYY-MM-DD\')'
                dumcite['title'] = title_template % (iv['institution_id'],re.sub(' ','',iv['mod_details']['label']),project_id,a,k)
                dumcite['publisher'] = publisher
                dumcite['externalid_id'] = 0
                dumcite['policy_drs_id'] = 0
                dumcite['publication_year'] = 'to_char(sysdate,\'YYYY\')'
                dumcite['upd_by'] = upd_by

                # externalid
                dumextid['external_id'] = 'cmip6_cite_test.seq_externalid.nextVal'
                dumextid['external_pid'] = pid_template #% (dumentry['policy_drs_id'])
                dumextid['external_id_type_id'] = 2
                dumextid['external_pid_status'] = 'initial'
                dumextid['external_pid_url'] = url_template % (project_id,a,iv['institution_id'],re.split('\$',ik)[0],k)
                dumextid['timestamp'] = 'SYSTIMESTAMP'

                # model_connect details
                dummod['drs_map_esgf'] = drsid_template % (project_id,a,iv['institution_id'],re.split('\$',ik)[0],k)
                dummod['model_acronym'] = re.split('\$',ik)[0]
                dummod['institution_acronym'] = re.split('\$',ik)[1]

                # add sql insert lines to lists
                drs = drs_template % (project_id,a,iv['institution_id'],re.split('\$',ik)[0],k)
                drsid = drsid_template % (project_id,a,iv['institution_id'],re.split('\$',ik)[0],k)
                esgf = esgf_template % (project_id,a,iv['institution_id'],re.split('\$',ik)[0],k)
                sql_lines.append('insert into policy_drs values (cmip6_cite_test.seq_policy_drs.nextVal,\'%s\',\'%s\',\'%s\',\'%s\',\'%s\');' % (drs,project,drsid,esgf,dumentry['show']))
                policy_drs.append(dumentry)
                externalid.append(dumextid)
                citation.append(dumcite)
                modelconn.append(dummod)

    sql_lines.append('')

print '\nNo of policy_drs entries (CMIP6_CV): '+str(len(sql_lines))+'\n'
#print modelconn
#sys.exit()

for k,v in num_drs.iteritems():
    print '    No of %s entries in policy_drs: %i' % (k,v)


# connect to database
print 'Connect to CERA...'
cuser  = 'cmip6_cite_test'
fdb=os.path.abspath(os.path.relpath(mydir+'/../.cmip6_cite_test'+fileflag))
cpw    = open(fdb,'r').read().strip()
try:
    # MS 2019-12-03: oda-scan.dkrz.de -> delphi7-scan.dkrz.de
    #sdbfile =  cuser+'/'+cpw+'@'+'( DESCRIPTION = ( ADDRESS_LIST = ( ADDRESS = ( PROTOCOL = TCP ) ( HOST = oda-scan.dkrz.de ) ( PORT = 1521 ) ) ) ( CONNECT_DATA = ( SERVER = DEDICATED ) ( SERVICE_NAME = '+db+' ) ))'
    sdbfile =  cuser+'/'+cpw+'@'+'( DESCRIPTION = ( ADDRESS_LIST = ( ADDRESS = ( PROTOCOL = TCP ) ( HOST = delphi7-scan.dkrz.de ) ( PORT = 1521 ) ) ) ( CONNECT_DATA = ( SERVER = DEDICATED ) ( SERVICE_NAME = '+db+' ) ))'
    conn = cx_Oracle.connect(sdbfile)
    cur = conn.cursor()
except cx_Oracle.DatabaseError as e:
    error, = e.args
    log.error("Cannot connect to DB=\'%s\'. Check password (%i: %s)" % (':'.join(re.split(':',sdbfile)[:2]),error.code,error.message.strip()))
    sys.exit()

# insert into database
rel_models = [] # for exp case
for p,e,c,m in zip(policy_drs,externalid,citation,modelconn):
    skipflag = 0 # skipflag in case entry already exists
    # 2020-10-21: Skip ScenarioMIP for HAMMOZ-Consortium
    if re.search('HAMMOZ-Consortium',p['drs_map_esgf']) and re.search('ScenarioMIP',p['drs_map_esgf']):
        print 'SKIP: ',p['drs_map_esgf']
        continue
    # MS: Hack inserts
    #if p['show']=='' and re.search('PCMDI',p['drs_map_esgf']):
    #    #print p,e,c,m
    #    pass
    #else:
    #    continue
    #sys.exit()

    cur.prepare('select * from policy_drs where drs_map_esgf = :drs')
    try:
        cur.execute(None, {'drs':p['drs_map_esgf']})
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        errorHandling(error.code,error.message,'select from database failed (select * from policy_drs where drs_map_esgf =  \'%s\') -> exit' % p['drs_map_esgf'])

    res=cur.fetchall()
    # skip if entry already exists
    if len(res) >= 1: # error skips??? or p['drs_map_esgf']=='CMIP6.CMIP.CSIRO-BOM.ACCESS-1-0' or  re.search('AeroChemMIP',p['drs_map_esgf']) or  re.search('GEOMIP',p['drs_map_esgf']):
        #print 'skip1: %s; modelflag=%i' % (p['drs_map_esgf'],modelflag)
        continue

    # skip if modelflag=0: only insert model/MIP granularity
    if not re.search(spec_inst,str.upper(p['drs_map_esgf'].encode('utf-8'))) and modelflag < 1:
        #print 'skip2: %s; modelflag=%i' % (p['drs_map_esgf'],modelflag)
        continue
    # print DRS entries to insert for test options and continue
    if len(testflag) > 0 and modelflag < 1: 
        print 'NEW %s' % p['drs_map_esgf']
        continue

    if p['level'] == 'exp': # model/MIP entry or even exp entries exists?
        try:
            cur.prepare('select * from policy_drs d,citation c where d.drs_map_esgf like :drs and c.policy_drs_id=d.policy_drs_id order by d.drs_map_esgf')
            cur.execute(None, {'drs':'.'.join(re.split('\.',p['drs_map_esgf'])[0:-1])+'%'})
        except cx_Oracle.DatabaseError as e:
            error, = e.args
            errorHandling(error.code,error.message,'select from database failed (select * from policy_drs d,citation c where d.drs_map_esgf like \'%s\' and c.policy_drs_id=d.policy_drs_id order by d.drs_map_esgf) -> exit' % '.'.join(re.split('\.',p['drs_map_esgf'])[0:-1])+'%')

        res=cur.fetchall()
        # skip experiment entry inserts 
        # for modelflag=0
        # for modelflag=1 in case model/MIP non existing (len(res)=0)
        # NOT IMPLEMENTED - for modelflag=2 in case model/MIP but no experiments exist (len(res)=1) or no model/MIP exists
        if len(res) < modelflag:
            #print 'SKIP exp %s (modelflag=%i)' % (p['drs_map_esgf'],modelflag)
            #skipflag=0
            continue

        col_names=[]
        for i in range(0, len(cur.description)):
            col_names.append(cur.description[i][0])

        # set model user for new exp entries
        for k,v in zip(col_names,res[0]):
            if k == 'UPD_BY' and upd_by==v:
                #print k,v
                c['upd_by']=v
                skipflag = 1 # do not skip
                if len(testflag) > 0:
                    print 'NEW %s (upd_by=%s)' % (p['drs_map_esgf'] , v)
                    continue
            #else:
            #    if len(testflag) > 0 :
            #        print '...not inserted %s' % p['drs_map_esgf']
            #    continue

        # continue for test or skip case
        if len(testflag) > 0 or skipflag == 0: 
            continue
        else:
            # get related model/MIP entry for exp entry and insert exp entries
            #print 'EXP',p['drs_map_esgf'],testflag,modelflag,skipflag
            if '.'.join(re.split('\.',p['drs_map_esgf'])[:-1]) not in rel_models:
                rel_models.append('.'.join(re.split('\.',p['drs_map_esgf'])[:-1]))
            insertDb(p,c,e,m)
            continue
        

    # continue for test or skip case
    if len(testflag) > 0 or modelflag > 0 : # nothing to do for EXP or TEST
        continue
    else:
        # insert model/MIP entry
        #print 'MODEL',p['drs_map_esgf'],testflag,modelflag #,skipflag 
        insertDb(p,c,e,m)
        continue

# change date in model citation for EXP 
if len(testflag) == 0 and modelflag > 0 :
    #print '\n'.join(rel_models)
    changeModDate(rel_models)

 
##### testing
####conn.rollback()
####sys.exit()

# write report in log file and in log_jobs table
if len(testflag) > 0:
    conn.rollback()
else:
    if num_insert['policy_drs'] == 0 or num_insert['externalid'] == 0 or num_insert['citation'] == 0:  
        cur.close()
        conn.close()
        sys.exit()

    log.info('Total number of inserts: policy_drs=%s externalid=%s citation=%s model_connect=%s' % (str(num_insert['policy_drs']),str(num_insert['externalid']),str(num_insert['citation']),str(num_insert['model_connect'])))
    # INSERT into log_job (autocommit)
    try:
        sql='insert into log_jobs (ID,NAME,CALL,LOG_TYPE,LOG_MESS,ERR_CODE,ERR_MESS,TIMESTAMP) values (cmip6_cite_test.seq_log_jobs.nextVal,\'INSERT_CV\',\'%s\',\'INFO\',\'Total number of inserts: policy_drs=%s externalid=%s citation=%s model_connect=%s\',0,\'\',SYSTIMESTAMP)' % (' '.join(sys.argv),str(num_insert['policy_drs']),str(num_insert['externalid']),str(num_insert['citation']),str(num_insert['model_connect']))
        cur.execute( sql )
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        log.error('%s' % str(error.code))
        log.error('%s' % error.message)
        raise

    conn.commit()
    ####testing conn.rollback()


cur.close()
conn.close()

 
