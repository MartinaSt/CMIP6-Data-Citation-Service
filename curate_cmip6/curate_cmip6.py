#! /usr/bin/env python
"""Check citation database content for required curation issues
Version: V0.10 2020-04-16: tcera1 -> testdb
Version: V0.9  2020-03-10: No authors for DOI data check added
Version: V0.8  2019-12-03: DB hardware/software exchange
Version: V0.7  2019-07-23: upd_listconnect - add check on deleted lists still used in list_connect and correction
Version: V0.6  2019-07-11: upd_exp - add contacts of model/MIP with registered DOI for exp without creators
Version: V0.5  2019-07-10: first author curation for authors are institutions
Version: V0.4  2018-11-16: support for cx_Oracle 7
Version: V0.3  2018-03-02: first version with devel-operational (stockhause@dkrz.de)"""

# ./curate_cmip6.py [<TEST_FLAG>]
# TEST_FLAG=testdb (default=pcera)

import sys,os,os.path,re,getopt,logging,smtplib
import json
from email.mime.text import MIMEText

# set pathes
mydir=os.path.abspath(os.path.dirname(sys.argv[0]))
#print mydir
f=os.path.abspath(os.path.relpath(mydir+'/../config.json'))
config = json.loads(open(f,'r').read())
for l in config["addpaths"]:
    sys.path.append(l)
sys.path.append(mydir) 

# cx_Oracle module available?
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

def logMessage(ltype,lmess,lname,lshortmess):
    """logMessage: log insert messages in log_db table"""

    log.info(lmess)
    # insert in LOG_DB
    sql='insert into log_db (ID,NAME,SQL,ERR_CODE,ERR_MESS,TIMESTAMP) values (cmip6_cite_test.seq_log_db.nextVal,\'CURATE_CMIP6:%s\',\'%s\',0,\'\',SYSTIMESTAMP)' % (lname,lshortmess)
    cur.execute( sql )

def callError(sql,errname,cur,conn,log):
    """callError: Log error message and close db connection"""
    log.error('Error in execution of SQL statement: \'%s\'\n%s -> EXIT' % (sql,errname))
    cur.close()
    conn.close()

def getColumns(gcur):
    """getColumns: Extract db table column names for cursor"""
    col_names=[]
    for i in range(0, len(gcur.description)):
        col_names.append(gcur.description[i][0])
    return col_names


def getInt(v):
    """getInt: check getValue return value for list or variable type
       due to cx_Oracle change from version 5 returning a value to version 7 returning a list"""

    if type(v.getvalue()) is list:
        return int(v.getvalue()[0])
    else:
        return int(v.getvalue())

def upd_exp(cur, out_curated):
    """add contacts for experiments with a registered DOI on the corresponding"""

    num_uexp = 0
    sql='insert into list_connect (citation_id,related_unit_name_id,contact_type_id,upd_by,modification_date) select ce.citation_id,lm.related_unit_name_id,lm.contact_type_id,lm.upd_by,sysdate from citation cm, list_connect lm,policy_drs pm, externalid em, policy_drs pe,citation ce, externalid ee where REGEXP_COUNT(pm.drs_map_esgf, \'\.\') = 3 and pm.policy_drs_id=cm.policy_drs_id and cm.externalid_id=em.external_id and em.external_pid_status=\'registered\' and lm.citation_id=cm.citation_id and REGEXP_COUNT(pe.drs_map_esgf, \'\.\') = 4 and pe.drs_map_esgf like pm.drs_map_esgf||\'.%\' and pe.policy_drs_id=ce.policy_drs_id and ce.externalid_id=ee.external_id and ce.citation_id not in (select citation_id from list_connect where contact_type_id=3000006)'

    #print "insert exp into list_connect"
    out_curated.append('UPD_EXP (insert contacts for exp into list_connect):')
    cur.prepare(sql)
    try:
        cur.execute(None, )
        num_uexp = cur.rowcount
        logMessage('INFO','INSERT into list_connect %i rows' % (num_uexp),'add exp contacts','INSERT into list_connect %i rows' % (num_uexp))
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        print error.message
        callError(sql,error.message,cur,conn,log)

    #print 'Total number of inserts into list_connect=%s' % (str(num_uexp))
    #sys.exit()
    if len(testflag) == 0 and num_uexp>0:
        # INSERT into log_job (autocommit)
        log.info('Total number of inserts into list_connect=%s' % (str(num_uexp)))
        #print 'Total number of inserts into list_connect=%s' % (str(num_uexp))
        out_curated.append('Total number of inserts into list_connect=%s' % (str(num_uexp)))
        try:
            sql='insert into log_jobs (ID,NAME,CALL,LOG_TYPE,LOG_MESS,ERR_CODE,ERR_MESS,TIMESTAMP) values (cmip6_cite_test.seq_log_jobs.nextVal,\'CURATE_CMIP6\',\'%s\',\'INFO\',\'Total number of inserts into list_connect=%s\',0,\'\',SYSTIMESTAMP)' % (' '.join(sys.argv),str(num_uexp))
            cur.execute( sql )
        except cx_Oracle.DatabaseError as f:
            error, = f.args
            log.error('insert log_jobs failed: %s - %s ' % (str(error.code),error.message.strip()))
        conn.commit()
    out_curated.append('\n')
    return out_curated 


def upd_hascontact(cur,out_curated):
    """add contactPerson in case of HAS_CONTACT error detected"""

    num_insert = {'related_unit_name':0,'related_unit':0,'list_connect':0}
    num_upd = {'citation':0}

    relunitnameid=cur.var(cx_Oracle.NUMBER)
    relunitid=cur.var(cx_Oracle.NUMBER)
    citationid=cur.var(cx_Oracle.NUMBER)

    # MS (2019-07-10)
    #sql='with x as (select c.citation_id,c.policy_drs_id,c.title,c.upd_by,case when ru.person_id>0 then ru.person_id when ru.institute_id>0 then ru.institute_id end as first_author,case when ru.person_id>0 then \'PERSON\' when ru.institute_id>0 then \'INSTITUTE\' end as ctype from list_connect l_cr, citation c, related_unit_name rn, related_unit ru where l_cr.contact_type_id=3000006 and l_cr.citation_id=c.citation_id and l_cr.related_unit_name_id=rn.related_unit_name_id and rn.related_unit_name_id=ru.related_unit_name_id and ru.sequence_no=1) select x.citation_id,x.title,x.upd_by,x.first_author,x.ctype,p.drs_map_esgf from x,policy_drs p where x.citation_id not in (select citation_id from list_connect where x.citation_id=citation_id and contact_type_id=3000005) and x.policy_drs_id=p.policy_drs_id order by x.title'
    sql='with x as (select c.citation_id,c.policy_drs_id,c.title,c.upd_by,case when ru.person_id>0 then ru.person_id when ru.institute_id>0 then ru.institute_id end as first_author,case when ru.person_id>0 then \'PERSON\' when ru.institute_id>0 then \'INSTITUTE\' end as ctype from list_connect l_cr, citation c, related_unit_name rn, related_unit ru where l_cr.contact_type_id=3000006 and l_cr.citation_id=c.citation_id and l_cr.related_unit_name_id=rn.related_unit_name_id and rn.related_unit_name_id=ru.related_unit_name_id and ru.sequence_no=1) select x.citation_id,x.title,x.upd_by,x.first_author,x.ctype,p.drs_map_esgf,up.person_id from x,policy_drs p, cera2_upd.upd_person up where x.citation_id not in (select citation_id from list_connect where x.citation_id=citation_id and contact_type_id=3000005) and x.policy_drs_id=p.policy_drs_id and ((upper(x.upd_by)=upper(up.email)) or (upper(SUBSTR(x.upd_by, 1 ,INSTR(x.upd_by, \'_\', 1, 1)-1))=upper(up.first_name) and upper(SUBSTR(x.upd_by, INSTR(x.upd_by,\'_\', -1, 1)+1))=upper(up.last_name))) order by x.title'
    try:
        cur.execute(sql)
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        #print error.message
        callError(sql,error.message,cur,conn,log)

    data = cur.fetchall()
    #print len(data)
    col_names=getColumns(cur)
    out_curated.append('UPD_HASCONTACT (insert missing ContactPersons):')

    todolist = []
    for d in data:
        #print d
        line = {}
        for k2,v2 in zip(col_names,d):
            line[str.upper(k2)]=str(v2)
            #print k2,v2
        # MS 2019-07-10
        #if line['CTYPE'] == 'INSTITUTE':
        #    continue
        #print line
        todolist.append(line)

    # 1. check if list with single person already exists. lists with first person has person_id
    for t in todolist:
        trunid=''
        truid=''
        #print t['DRS_MAP_ESGF']
        out_curated.append(t['DRS_MAP_ESGF'])
        try:
            cur.prepare("with x as (select related_unit_name_id,related_unit_name,upd_by from related_unit_name run where run.upd_by= :upd_by and run.related_unit_name_id in (select related_unit_name_id from related_unit where person_id= :pid and sequence_no = 1)) select x.related_unit_name_id,x.related_unit_name,x.upd_by,count(*) as numauthors from related_unit u,x where u.related_unit_name_id=x.related_unit_name_id group by x.related_unit_name_id,x.related_unit_name,x.upd_by")
            # MS 2019-07-10
            if t['CTYPE'] == 'INSTITUTE':
                cur.execute(None,upd_by=t['UPD_BY'],pid=int(t['PERSON_ID']))
            else: # 'CTYPE'='PERSON'
                cur.execute(None,upd_by=t['UPD_BY'],pid=int(t['FIRST_AUTHOR']))
        except cx_Oracle.DatabaseError as e:
            error, = e.args
            print error.message
            callError(sql,error.message,cur,conn,log)
    
        res = cur.fetchall()
        col_names=getColumns(cur)

        for r in res:
            dum = {}
            for k2,v2 in zip(col_names,r):
                dum[str.upper(k2)]=str(v2)
            if int(dum['NUMAUTHORS']) == 1:
                trunid = dum['RELATED_UNIT_NAME_ID']
                relunitnameid.setvalue(0, int(trunid))
                #t['RELATED_UNIT_NAME_ID'] = dum['RELATED_UNIT_NAME_ID']
                #relunitnameid.setvalue(0, int(t['RELATED_UNIT_NAME_ID']))
                #print t,int(relunitnameid.getvalue())
                break

        #print '1',trunid,truid,t
        #sys.exit()
        # 2. if required: insert related_unit_name  
        # 3. if required: insert related_unit
        #if not 'RELATED_UNIT_NAME_ID' in t:
        if len(trunid) == 0:   
            #print 'insert related_unit_name and related_unit required',t
            #print "insert into related_unit_name (related_unit_name_id,related_unit_name,modification_date,upd_by,related_unit_descr) values (seq_related_unit_name_id.nextVal,%s,sysdate,%s,%s ) returning related_unit_name_id into :runid" % (t['DRS_MAP_ESGF'][0:100]+'_contactPerson_'+re.sub(':','',re.sub('-','',os.popen('date +%F_%T').read().strip())),t['UPD_BY'],'nix')
            out_curated.append("insert into related_unit_name (related_unit_name_id,related_unit_name,modification_date,upd_by,related_unit_descr) values (seq_related_unit_name_id.nextVal,%s,sysdate,%s,%s ) returning related_unit_name_id into :runid" % (t['DRS_MAP_ESGF'][0:100]+'_contactPerson_'+re.sub(':','',re.sub('-','',os.popen('date +%F_%T').read().strip())),t['UPD_BY'],'nix'))
            cur.prepare("insert into related_unit_name (related_unit_name_id,related_unit_name,modification_date,upd_by,related_unit_descr) values (seq_related_unit_name_id.nextVal,:lname,sysdate,:lupd_by,:ldesc ) returning related_unit_name_id into :lrunid")
            try:
                cur.execute(None,lname=t['DRS_MAP_ESGF'][0:100]+'_contactPerson_'+re.sub(':','',re.sub('-','',os.popen('date +%F_%T').read().strip())),lupd_by=t['UPD_BY'],ldesc='',lrunid=relunitnameid)
                trunid = str(getInt(relunitnameid))
                num_insert['related_unit_name'] += 1
                logMessage('INFO','INSERT into related_unit_name (id=%i; name=\'%s\'; upd_by=\'%s\')' % (int(trunid),t['DRS_MAP_ESGF'][0:100]+'_contactPerson_'+re.sub(':','',re.sub('-','',os.popen('date +%F_%T').read().strip())),t['UPD_BY']),t['DRS_MAP_ESGF'],'INSERT into related_unit_name (id=%i; name=%s; upd_by=%s)' % (int(trunid),t['DRS_MAP_ESGF'][0:100]+'_contactPerson_'+re.sub(':','',re.sub('-','',os.popen('date +%F_%T').read().strip())),t['UPD_BY']))
            except cx_Oracle.DatabaseError as e:
                error, = e.args
                print error.message
                callError(sql,error.message,cur,conn,log)
            #print t,int(relunitnameid.getvalue())
            #print '2',trunid,truid,t
            # MS 2019-07-10
            if t['CTYPE'] == 'INSTITUTE':
                #print "insert into related_unit (related_unit_id,person_id,institute_id,sequence_no,related_unit_name_id) values (seq_related_unit_id.nextVal,%i,0,1,%i) returning related_unit_id into :rruid" % (int(t['PERSON_ID']),int(trunid))
                out_curated.append("insert into related_unit (related_unit_id,person_id,institute_id,sequence_no,related_unit_name_id) values (seq_related_unit_id.nextVal,%i,0,1,%i) returning related_unit_id into :rruid" % (int(t['PERSON_ID']),int(trunid)))
            else:
                #print "insert into related_unit (related_unit_id,person_id,institute_id,sequence_no,related_unit_name_id) values (seq_related_unit_id.nextVal,%i,0,1,%i) returning related_unit_id into :rruid" % (int(t['FIRST_AUTHOR']),int(trunid))
                out_curated.append("insert into related_unit (related_unit_id,person_id,institute_id,sequence_no,related_unit_name_id) values (seq_related_unit_id.nextVal,%i,0,1,%i) returning related_unit_id into :rruid" % (int(t['FIRST_AUTHOR']),int(trunid)))
            cur.prepare("insert into related_unit (related_unit_id,person_id,institute_id,sequence_no,related_unit_name_id) values (seq_related_unit_id.nextVal,:rpid,0,1,:rrunid ) returning related_unit_id into :rruid" )
            try:
                # MS 2019-07-10
                if t['CTYPE'] == 'INSTITUTE':
                    cur.execute(None,rpid=int(t['PERSON_ID']),rrunid=int(trunid),rruid=relunitid)
                    truid = str(getInt(relunitid))
                    logMessage('INFO','INSERT into related_unit (id=%i; person_id=%i; related_unit_name_id=%i)' % (int(truid),int(t['PERSON_ID']),int(trunid)),t['DRS_MAP_ESGF'],'INSERT into related_unit (id=%i; person_id=%i; related_unit_name_id=%i)' % (int(truid),int(t['PERSON_ID']),int(trunid)))
                else:
                    cur.execute(None,rpid=int(t['FIRST_AUTHOR']),rrunid=int(trunid),rruid=relunitid)
                    truid = str(getInt(relunitid))
                    logMessage('INFO','INSERT into related_unit (id=%i; person_id=%i; related_unit_name_id=%i)' % (int(truid),int(t['FIRST_AUTHOR']),int(trunid)),t['DRS_MAP_ESGF'],'INSERT into related_unit (id=%i; person_id=%i; related_unit_name_id=%i)' % (int(truid),int(t['FIRST_AUTHOR']),int(trunid)))
                num_insert['related_unit'] += 1
                irelunitid = getInt(relunitid)
            except cx_Oracle.DatabaseError as e:
                error, = e.args
                print error.message
                callError(sql,error.message,cur,conn,log)
            #print t,int(relunitnameid.getvalue()),int(relunitid.getvalue())
            #print '3',trunid,truid,t

        # 4. insert list_connect
        #print 'insert into list_connect (citation_id,related_unit_name_id,contact_type_id,upd_by,modification_date) values (%i,%i,3000005,\'%s\',sysdate)' % (int(t['CITATION_ID']),int(trunid),t['UPD_BY'])
        out_curated.append('insert into list_connect (citation_id,related_unit_name_id,contact_type_id,upd_by,modification_date) values (%i,%i,3000005,\'%s\',sysdate)' % (int(t['CITATION_ID']),int(trunid),t['UPD_BY']))
        cur.prepare("insert into list_connect (citation_id,related_unit_name_id,contact_type_id,upd_by,modification_date) values (:lccid,:lcrunid,3000005,:lcupd_by,sysdate)")
        try:
            cur.execute(None,lccid=int(t['CITATION_ID']),lcrunid=int(trunid),lcupd_by=t['UPD_BY'])
            num_insert['list_connect'] += 1
            logMessage('INFO','INSERT into list_connect (citation_id=%i; related_unit_name_id=%i; upd_by=\'%s\')' % (int(t['CITATION_ID']),int(trunid),t['UPD_BY']),t['DRS_MAP_ESGF'],'INSERT into list_connect (citation_id=%i; related_unit_name_id=%i; upd_by=%s)' % (int(t['CITATION_ID']),int(trunid),t['UPD_BY']))
        except cx_Oracle.DatabaseError as e:
            error, = e.args
            print error.message
            callError(sql,error.message,cur,conn,log)
        #print t,int(relunitnameid.getvalue())

        # 5. update citation
        #print "update citation set MODIFICATION_DATE=sysdate where CITATION_ID=%i" % (int(t['CITATION_ID']))
        out_curated.append('update citation set MODIFICATION_DATE=sysdate where CITATION_ID=%i' % (int(t['CITATION_ID'])))
        cur.prepare("update citation set MODIFICATION_DATE=sysdate where CITATION_ID= :ccid")
        try:
            cur.execute(None,ccid=int(t['CITATION_ID']))
            num_upd['citation'] += 1
            logMessage('INFO','UPDATE citation set modification_date=sysdate where citation_id=%i' % (int(t['CITATION_ID'])),t['DRS_MAP_ESGF'],'UPDATE citation set modification_date=sysdate where citation_id=%i' % (int(t['CITATION_ID'])))

        except cx_Oracle.DatabaseError as e:
            error, = e.args
            print error.message
            callError(sql,error.message,cur,conn,log)

    #sys.exit()
    #conn.rollback()
    #return

    if len(testflag) == 0 and num_insert['list_connect']>0:
        # INSERT into log_job (autocommit)
        log.info('Total number of inserts: related_unit_name=%s, related_unit=%s, list_connect=%s; number of updates: citation=%s' % (str(num_insert['related_unit_name']),str(num_insert['related_unit']),str(num_insert['list_connect']),str(num_upd['citation'])))
        #print 'Total number of inserts: related_unit_name=%s, related_unit=%s, list_connect=%s; number of updates: citation=%s' % (str(num_insert['related_unit_name']),str(num_insert['related_unit']),str(num_insert['list_connect']),str(num_upd['citation']))
        out_curated.append('Total number of inserts: related_unit_name=%s, related_unit=%s, list_connect=%s; number of updates: citation=%s' % (str(num_insert['related_unit_name']),str(num_insert['related_unit']),str(num_insert['list_connect']),str(num_upd['citation'])))
        try:
            sql='insert into log_jobs (ID,NAME,CALL,LOG_TYPE,LOG_MESS,ERR_CODE,ERR_MESS,TIMESTAMP) values (cmip6_cite_test.seq_log_jobs.nextVal,\'CURATE_CMIP6\',\'%s\',\'INFO\',\'Total number of inserts: related_unit_name=%s, related_unit=%s, list_connect=%s; number of updates: citation=%s\',0,\'\',SYSTIMESTAMP)' % (' '.join(sys.argv),str(num_insert['related_unit_name']),str(num_insert['related_unit']),str(num_insert['list_connect']),str(num_upd['citation']))
            cur.execute( sql )
        except cx_Oracle.DatabaseError as f:
            error, = f.args
            log.error('insert log_jobs failed: %s - %s ' % (str(error.code),error.message.strip()))
        conn.commit()
    out_curated.append('\n')
    return out_curated


def upd_listconnect(cur,runids,out_curated):
    """set related_unit_name entries flagged as deleted back to visible"""

    num_ulc = 0
    sql = ''
    for r in runids:
        #print "update related_unit_name set upd_by=replace(upd_by,' deleted','') where related_unit_name_id= %i" % r
        out_curated.append("update related_unit_name set upd_by=replace(upd_by,' deleted','') where related_unit_name_id= %i" % r)
        sql='update related_unit_name set upd_by=replace(upd_by,\' deleted\',\'\') where related_unit_name_id=%i' % r
        try:
            cur.execute(sql)
            num_ulc += 1
            logMessage('INFO','UPDATE related_unit_name set upd_by=replace(upd_by, deleted,) where related_unit_name_id= %i' % r,'LIST_CONNECT','UPDATE related_unit_name set upd_by=replace(upd_by, deleted,) where related_unit_name_id= %i' % r)
        except cx_Oracle.DatabaseError as e:
            error, = e.args
            print error.message
            callError(sql,error.message,cur,conn,log)

    #sys.exit()
    #conn.rollback()
    #return
    out_curated.append('UPD_LISTCONNECT (remove deleted_flag from related_unit_names):')
    if len(testflag) == 0 and num_ulc>0:
        # INSERT into log_job (autocommit)
        log.info('Total number of updates: related_unit_name=%s' % num_ulc)
        #print 'Total number of updates: related_unit_name=%s' % num_ulc
        out_curated.append('Total number of updates: related_unit_name=%s' % num_ulc)
        try:
            sql='insert into log_jobs (ID,NAME,CALL,LOG_TYPE,LOG_MESS,ERR_CODE,ERR_MESS,TIMESTAMP) values (cmip6_cite_test.seq_log_jobs.nextVal,\'CURATE_CMIP6\',\'%s\',\'INFO\',\'Total number of updates: related_unit_name=%s\',0,\'\',SYSTIMESTAMP)' % (' '.join(sys.argv),str(num_ulc))
            cur.execute( sql )
        except cx_Oracle.DatabaseError as f:
            error, = f.args
            log.error('insert log_jobs failed: %s - %s ' % (str(error.code),error.message.strip()))
        conn.commit()
        #conn.rollback()
    out_curated.append('\n')
    return out_curated


# set defaults and analyze testflag option
mydate = os.popen('date +%F').read().strip()

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
    #log.info('Call: %s' % ' '.join(sys.argv))
    testflag = ''

# configure logging and open logfile
LOG_FILENAME = config["logdir"]+"/curate_cmip6"+fileflag+".log"
log = logging.getLogger()
console=logging.FileHandler(LOG_FILENAME)
formatter=logging.Formatter('%(asctime)s - %(levelname)s: %(message)s')
simple_formatter=logging.Formatter('%(message)s')
console.setFormatter(formatter)
log.setLevel(logging.INFO)
log.addHandler(console)

# define check names, descriptions and categories
simples=['IS_PERSON','DOI_FORMAT','MULTI_LIST','MULTI_REF','MULTI_PERSON','MULTI_INST','TITLES','HAS_CONTACT','MODELS','VERSIONS','MULTI_ORCID','MULTI_INSTID','MISS_PERSON','MISS_INST','MISS_REF']
vis_checks=['CHECK_ORCID','CHECK_INSTID','CHECK_ADDINFOS','COMPLETED_CITATIONS','DOI_REF','DOI_ESGF','NODOI_ESGF','NEW_ORCIDS','NEW_INSTIDS','NEW_UPDCITATIONS','EXP_ADD_CREATORS','DEL_OR_ZEROLENGTH_LIST','DOI_NO_AUTHORS']
descr={'IS_PERSON':'contact_type not fitting related_unit: person/institute','DOI_FORMAT':'citation entries malformed: access_spec doi: or http','MULTI_LIST':'multiple lists connected as same contact type to same citation entry','MULTI_REF':'same reference related to citation entry multiple times','TITLES':'identical titles','MULTI_PERSON':'same person multiple times in same list','MULTI_INST':'same institute multiple times in same list','HAS_CONTACT':'missing contactPerson list for available creator list and citation entry','CHECK_ORCID':'visual check of new ORCIDS since last update','CHECK_INSTID':'visual check of new institute PIDs since last update','CHECK_ADDINFOS':'visual check of new addinfos since last update','MODELS':'models without citation entries','VERSIONS':'versions without citation entries','COMPLETED_CITATIONS':'visual check of completed citations with status initial','DOI_REF':'completed citations with DOI without cera2.citation entry','DOI_ESGF':'Data not accessible for completed citations with DOI using search API version of ESGF_ACCESS_LINK','NODOI_ESGF':'Data available in ESGF but no authors provided using search API version of ESGF_ACCESS_LINK','NEW_UPDCITATIONS':'visual check of new cera2_upd citations','NEW_ORCID':'visual check of new ORCID entry','NEW_INSTID':'visual check of new institute PID entry','MULTI_ORCID':'Same ORCID assigned to multiple persons','MULTI_INSTID':'Same INSTITUTE PID assigned to multiple institutes','MISS_PERSON':'Person used in related_unit is missing in upd_person','MISS_INST':'Institute used in related_unit is missing in upd_institute','MISS_REF':'Citation referenced in reference is missing in upd_citation','EXP_ADD_CREATORS':'Experiments without authors list but DOI registered for connected model/MIP','DEL_OR_ZERO_LENGTH_LIST':'Related_unit_name entries flagged as deleted or with list length=0 with use in list_connect','DOI_NO_AUTHORS':'DOI data without authors (list_connect entry deleted)'}

# connect to db
cuser  = 'cmip6_cite_test'
fdb=os.path.abspath(os.path.relpath(mydir+'/../.cmip6_cite_test'+fileflag))
cpw    = open(fdb,'r').read().strip()
try:
    # MS 2019-12-03: oda-scan.dkrz.de -> delphi7-scan.dkrz.de
    #sdbfile =  cuser+'/'+cpw+'@'+'( DESCRIPTION = ( ADDRESS_LIST = ( ADDRESS = ( PROTOCOL = TCP ) ( HOST = oda-scan.dkrz.de ) ( PORT = 1521 ) ) ) ( CONNECT_DATA = ( SERVER = DEDICATED ) ( SERVICE_NAME = '+db+' ) ))'
    sdbfile =  cuser+'/'+cpw+'@'+'( DESCRIPTION = ( ADDRESS_LIST = ( ADDRESS = ( PROTOCOL = TCP ) ( HOST = delphi7-scan.dkrz.de ) ( PORT = 1521 ) ) ) ( CONNECT_DATA = ( SERVER = DEDICATED ) ( SERVICE_NAME = '+db+' ) ))'
 
except:
    log.error("Cannot connect to DB=\'%s\'. Check password" % cuser)
    #raise IOError, "\nCannot connect to DB=\'%s\'. Check password\n" % cuser
    sys.exit()

try:
    conn = cx_Oracle.connect(sdbfile)
    cur = conn.cursor()
except IOError,ex:
    log.error("DB not found: %s :\n%s" % (':'.join(re.split(':',sdbfile)[:2]),ex))

# read sqls
cera = {}
sqls = {}
for l in open(mydir+'/curate_cmip6.conf','r').readlines():
    if re.match(r'^(\#)',l.strip()) or len(l.strip()) == 0:
        continue            
    key   = re.split(':',l.strip())[0]
    value = ':'.join(re.split(':',l.strip())[1:]).strip()
    #sqls[key]=Template(value).safe_substitute(templ)
    sqls[key]=value

# execute sqls and write results to dict cera
for k,v in sqls.iteritems():
    dumlist = []
    try:
        cur.execute(v)
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        callError(v,error.message,cur,conn,log)

    data = cur.fetchall()
    col_names=getColumns(cur)

    for d in data:
        line = {}
        for k2,v2 in zip(col_names,d):
            line[str.upper(k2)]=str(v2)

        dumlist.append(line)
                
    cera[k]=dumlist

out_curated = []
if len(cera['HAS_CONTACT'])>0 and len(testflag) == 0:
    out_curated = upd_hascontact(cur,out_curated)

if len(cera['EXP_ADD_CREATORS'])>0 and len(testflag) == 0:
    out_curated = upd_exp(cur,out_curated)


# analyze sql results (dict cera)
out_sum = []
out_details = []
runids = []

for k,v in cera.iteritems():
    counter = 0
    if len(v) == 0: # or v[0].values()[0]=='None':
        out_sum.append('%s: OK' % k)
        log.info('%s: OK' % k)
    else:
        #counter = len(v)
        for vv in v:
            if vv.values()[0] != 'None':
                counter += 1
        if counter == 0:
            out_sum.append('%s: OK' % k)
            log.info('%s: OK' % k)
            continue
            
        mylist = []
        if k == 'DEL_OR_ZEROLENGTH_LIST':
            for l in v:
                if int(l['RELATED_UNIT_NAME_ID']) not in runids:
                    runids.append(int(l['RELATED_UNIT_NAME_ID']))
            #print runids
            #sys.exit()

        if k!='DOI_ESGF' and k!='NODOI_ESGF':
            out_sum.append('%s: issues %i' % (k,counter) )
        if k in simples:
            for l in v:
                mylist.append(';'.join([l[key] for key in l if l[key]!='None']))
            log.error('%s: issues %i' % (k,counter))
        else:
            for l in v:
                mylist.append('$$'.join([key+'$'+l[key] for key in l if l[key]!='None']))
            if k!='DOI_ESGF' and k!='NODOI_ESGF':
                log.info('%s: issues %i' % (k,counter))
        out_details.append({k:mylist})


if len(cera['DEL_OR_ZEROLENGTH_LIST'])>0 and len(testflag) == 0:
    out_curated = upd_listconnect(cur,runids,out_curated)

out_analysed=[]    

# 1. prepare output in list out_analysed
# 2. check number of accessible datasets in ESGF for DOI data > 0
#    using search api version of esgf_access_link
num_completed = 0
for o in sorted(out_details):
    for k,v in o.iteritems():
        # prepare output for category simples
        if k in simples:
            out_analysed.append('\n%s (%s):\n%s' % (k,descr[k],'\n'.join(v)))
        else:
            if k=='COMPLETED_CITATIONS':
                num_completed += 1
                log.info('%s (%s):' % (k,descr[k]))
                for l in v:
                    #print re.split('\$',l)
                    xx = re.split('\$',l)
                    for x,index in zip(xx,range(len(xx))):
                        if x.strip()=='DRS_MAP_ESGF':
                            log.info('Citation completed for DRS: %s' % (xx[index+1]))

            # check esgf access links and number of datasets in response
            elif k=='DOI_ESGF':
                myv=[]
                mynum = {'OK':0, 'ERROR':0}
                for l in v:
                    myesgf = re.split('\$\$',re.split('ESGF_ACCESS_LINK\$',l)[1])[0]
                    #print myesgf
                    #print 'wget -q -O- \''+re.sub('search/cmip6-dkrz/','esg-search/search/',re.sub('search/testproject/','esg-search/search/',re.sub('search/esgf-dkrz/','esg-search/search/',myesgf)))+'&fields=master_id\''
                    #sys.exit()
                    ##for ll in os.popen('wget -q -O- \''+re.sub('search/testproject/','esg-search/search/',re.sub('search/esgf-dkrz/','esg-search/search/',myesgf))+'\'').readlines():
                    ##for ll in os.popen('wget -q -O- \''+re.sub('search/cmip6-dkrz/','esg-search/search/',re.sub('search/testproject/','esg-search/search/',re.sub('search/esgf-dkrz/','esg-search/search/',myesgf)))+'&fields=master_id\'').readlines():
                    for ll in os.popen('wget -q -O- \''+re.sub('search/input4mips-dkrz/','esg-search/search',re.sub('search/cmip6-dkrz/','esg-search/search/',re.sub('search/testproject/','esg-search/search/',re.sub('search/esgf-dkrz/','esg-search/search/',myesgf))))+'&fields=master_id\'').readlines():
                        #if re.search('result name',ll):
                        #if re.search(r'^(\s+)Total Number of Results:',ll):
                        if re.search(r'numFound',ll):
                            #mynum=re.split('numFound="',re.split('" start',ll)[0])[1]
                            #mnum=re.split(':',ll)[1].strip()
                            mnum=int(re.split('"',re.split('numFound="',ll.strip())[1])[0])
                            if int(mnum) == 0:
                                log.error('No data in ESGF - %s' % myesgf)
                                myv.append('No data in ESGF - %s' % myesgf)
                                mynum['ERROR'] += 1
                            else:
                                log.info('%s datasets in ESGF - %s' % (mnum,myesgf))
                                mynum['OK'] += 1
                            #sys.exit()
                            break
                   #sys.exit()

                # test result handling
                if len(myv) > 0:
                    out_analysed.append('\n%s (%s):\n%s\n' % (k,descr[k],'\n'.join(myv)))
                    out_sum.append('%s: issues %i' % (k,len(myv)) )
                    log.error('%s: %i - ERROR' % (k,mynum['ERROR']))
                else:
                    out_sum.append('%s: %i - OK' % (k,mynum['OK']))
                    log.info('%s: %i - OK' % (k,mynum['OK']))


            # check esgf access links and number of datasets in response for incomplete data references
            elif k=='NODOI_ESGF':
                myv2=[]
                mynum2 = {'OK':0, 'ERROR':0}
                for l in v:
                    myesgf2 = re.split('\$\$',re.split('ESGF_ACCESS_LINK\$',l)[1])[0]
                    for ll in os.popen('wget -q -O- \''+re.sub('search/cmip6-dkrz/','esg-search/search/',re.sub('search/testproject/','esg-search/search/',re.sub('search/input4mips-dkrz/','esg-search/search/',re.sub('search/esgf-dkrz/','esg-search/search/',myesgf2))))+'&fields=master_id\'').readlines():
                        #if re.search('result name',ll):
                        #if re.search(r'^(\s+)Total Number of Results:',ll):
                        if re.search(r'numFound',ll):
                            #mynum=re.split('numFound="',re.split('" start',ll)[0])[1]
                            #mnum=re.split(':',ll)[1].strip()
                            mnum2=int(re.split('"',re.split('numFound="',ll.strip())[1])[0])
                            if int(mnum2)>0:
                                #log.error('Data in ESGF without citation - %s' % myesgf2)
                                myv2.append('Data in ESGF without citation - %s' % myesgf2)
                                mynum2['ERROR'] += 1
                            else:
                                #log.info('%s datasets in ESGF - %s' % (mnum,myesgf))
                                mynum2['OK'] += 1
                            #sys.exit()
                            break
                    #sys.exit()

                if len(myv2) > 0:
                    out_analysed.append('\n%s (%s):\n%s\n' % (k,descr[k],'\n'.join(myv2)))
                    out_sum.append('%s: issues %i' % (k,len(myv2)) )
                    #log.error('%s: %i - ERROR' % (k,mynum['ERROR']))
                else:
                    out_sum.append('%s: %i - OK' % (k,mynum2['OK']))
                    #log.info('%s: %i - OK' % (k,mynum['OK']))
            else:
                out_analysed.append('\n%s (%s):\n%s\n' % (k,descr[k],re.sub('\$',':',re.sub('\$\$','\n   ','\n'.join(v)))))


#print '\n'.join(out_analysed)
#print '\n'.join(out_sum)
#sys.exit()

# send email with results and close db connection
if len(testflag) == 0:
    msg = MIMEText('CURATION:\n\n'+'\n'.join(out_curated)+'\n------------------------\nINFORMATION:\n\n'+'\n'.join(sorted(out_sum))+'\n\n------------------------\nDETAILS:\n'+'\n'.join(out_analysed))
else:
    msg = MIMEText('\n'.join(sorted(out_sum))+'\n\n------------------------\nDETAILS:\n'+'\n'.join(out_analysed))

msg['Subject'] = 'cmip6_curation (%s) on %s' % (db,os.popen('date +%Y-%m-%d').read().strip())
sender = 'k204082@dkrz.de'
##recipients = ['stockhause@dkrz.de','lammert@dkrz.de']
recipients = ['stockhause@dkrz.de','cmip-ipo@esa.int']
msg['From'] = sender
msg['To'] = ', '.join(recipients)
s = smtplib.SMTP('localhost')
#s = smtplib.SMTP('mailhost.dkrz.de')
s.sendmail(sender,recipients, msg.as_string())
s.quit()
cur.close()
conn.close()
