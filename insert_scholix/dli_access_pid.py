#! /usr/bin/env python

""" Insert/Update citation/reference from Scholix for WDCC and ESGF DOIs
Version: V0.5 2020-06-17: datacite api v1 -> v2
Version: V0.4 2020-04-16: tcera1 -> testdb
Version: V0.3 2019-12-03: DB hardware/software exchange
Version: V0.2 2018-09-06, WDCC Scholix merge changed after discussion with HHW, stockhause@dkrz.de
Version: V0.1 2018-08-15, stockhause@dkrz.de"""
# Usage: ./dli_access_pid.py <WDCC|ESGF> [<test|testdb|testdbtest>]

import sys,os,re,httplib2
import logging
import json
import datetime
from string import Template
from operator import itemgetter
try:
    import cx_Oracle
except:
    print "Cannot import module cx_Oracle"
    sys.exit()

# DOIs without type, which are papers
paper_except = [
    '10.1002','10.1007','10.1017','10.1016','10.1029','10.1038','10.1051','10.1073','10.1080','10.1088','10.1098','10.1101','10.1111','10.1126','10.1175','10.1289','10.1371',
    '10.20944','10.21203','10.2139','10.2151','10.22541','10.2307','10.26686',
    '10.31223','10.3389','10.3390',
    '10.4324',
    '10.5194'
]

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


def getColumns(gcur):
    """getColumns: get columns for cursor results"""

    col_names=[]
    for i in range(0, len(gcur.description)):
        col_names.append(gcur.description[i][0])
    return col_names



# read options and analyze testflag
mydate = os.popen('date +%F').read().strip()
citid = {u'dataset':2000017,u'publication':7,u'other':2}

endpoint='http://api.scholexplorer.openaire.eu/v1/linksFromPid?pid=%s'
crossref_api='https://api.crossref.org/works/doi/%s'
#MS 2020-06-17:api v1->api v2
#datacite_api='https://api.datacite.org/works/%s'
datacite_api='https://api.datacite.org/dois/%s'
h = httplib2.Http()
request = 'GET'
number = 0
counts = {'existing':0,'missing internal':0,'missing external':0,'utf-8 errors for missing':0} 
num_inserts = {'citation':0,'reference':0}
#sqlcite = 'MERGE INTO upd_citation clta USING ( select :doi as access_spec from dual ) ccit ON (clta.access_spec = ccit.access_spec and clta.entry_id is NULL) WHEN NOT MATCHED THEN INSERT (clta.citation_id,clta.title,clta.authors,clta.publisher,clta.publication_date,clta.access_spec,clta.presentation_id,clta.citation_type_id,clta.upd_by,clta.upd_date) VALUES (cera2_temp.seq_citation.nextVal,:title,:authors,:publisher,to_date(:pubyear,\'YYYY-MM-DD\'),ccit.access_spec,2000001,:citation_type,\'MARTINA_STOCKHAUSE - SCHOLIX\',sysdate)'
sqlcite = {'ESGF':'MERGE INTO upd_citation clta USING ( select :doi as access_spec from dual ) ccit ON (clta.access_spec = ccit.access_spec and clta.entry_id is NULL) WHEN NOT MATCHED THEN INSERT (clta.citation_id,clta.title,clta.authors,clta.publisher,clta.publication_date,clta.access_spec,clta.presentation_id,clta.citation_type_id,clta.upd_by,clta.upd_date) VALUES (cera2_temp.seq_citation.nextVal,:title,:authors,:publisher,to_date(:pubyear,\'YYYY-MM-DD\'),ccit.access_spec,2000001,:citation_type,\'MARTINA_STOCKHAUSE - SCHOLIX\',sysdate)',
           'WDCC':'MERGE INTO upd_citation clta USING ( select :doi as access_spec from dual ) ccit ON (clta.access_spec = ccit.access_spec and clta.entry_id is NULL) WHEN NOT MATCHED THEN INSERT (clta.citation_id,clta.title,clta.authors,clta.publisher,clta.publication_date,clta.access_spec,clta.presentation_id,clta.citation_type_id,clta.upd_by,clta.upd_date) VALUES (cera2_temp.seq_citation.nextVal,:title,:authors,:publisher,to_date(:pubyear,\'YYYY-MM-DD\'),ccit.access_spec,2000001,:citation_type,\'MARTINA_STOCKHAUSE - SCHOLIX\',sysdate)'
           #'WDCC':'MERGE INTO citation clta USING ( select :doi as access_spec from dual ) ccit ON (clta.access_spec = ccit.access_spec) WHEN NOT MATCHED THEN INSERT (clta.citation_id,clta.title,clta.authors,clta.publisher,clta.publication_date,clta.access_spec,clta.presentation_id,clta.citation_type_id) VALUES (cera2_temp.seq_citation.nextVal,:title,:authors,:publisher,to_date(:pubyear,\'YYYY-MM-DD\'),ccit.access_spec,2000001,:citation_type)' 
}
sqlref = { 'ESGF':'INSERT INTO CMIP6_CITE_TEST."REFERENCE" (citation_id,ref_type_id,ref_citation_id,upd_by,modification_date) SELECT :entry_id,(CASE WHEN (select ref_type_id from CERA2.REF_TYPE where upper(ref_type_acronym)=upper(:refacro)) IS NOT NULL THEN (select ref_type_id from CERA2.REF_TYPE where upper(ref_type_acronym)=upper(:refacro)) ELSE 2000005 END) ref_type_id,c.citation_id,:scholix,sysdate FROM CERA2_UPD.UPD_CITATION c where c.access_spec= :doi and c.entry_id is NULL and rownum=1', 
#'ESGF':'INSERT INTO CMIP6_CITE_TEST."REFERENCE" (citation_id,ref_type_id,ref_citation_id) SELECT :entry_id,(CASE WHEN (select ref_type_id from CERA2.REF_TYPE where upper(ref_type_acronym)=upper(:refacro)) IS NOT NULL THEN (select ref_type_id from CERA2.REF_TYPE where upper(ref_type_acronym)=upper(:refacro)) ELSE 2000005 END) ref_type_id,c.citation_id FROM CERA2_UPD.UPD_CITATION c where c.access_spec= :doi and c.entry_id is NULL and rownum=1', #insert into reference values (citation_id,ref_type_id,ref_citation_id);
           'WDCC':'INSERT INTO CERA2_UPD."REFERENCE" (entry_id,citation_id,ref_type_id) SELECT :entry_id,c.citation_id,(CASE WHEN (select ref_type_id from CERA2.REF_TYPE where upper(ref_type_acronym)=upper(:refacro)) IS NOT NULL THEN (select ref_type_id from CERA2.REF_TYPE where upper(ref_type_acronym)=upper(:refacro)) ELSE 2000005 END) ref_type_id FROM CERA2_UPD.UPD_CITATION c where c.access_spec= :doi and rownum=1' #'insert into cera2_upd.reference values (entry_id,citation_id,ref_type_id);
           #'WDCC':'INSERT INTO CERA2."REFERENCE" (entry_id,citation_id,ref_type_id) SELECT :entry_id,c.citation_id,(CASE WHEN (select ref_type_id from CERA2.REF_TYPE where upper(ref_type_acronym)=upper(:refacro)) IS NOT NULL THEN (select ref_type_id from CERA2.REF_TYPE where upper(ref_type_acronym)=upper(:refacro)) ELSE 2000005 END) ref_type_id FROM CERA2.CITATION c where c.access_spec= :doi and rownum=1' #'insert into cera2_upd.reference values (entry_id,citation_id,ref_type_id);
           #'WDCC':'INSERT INTO CERA2_UPD.UPD_CITATION (citation_id,title,authors,publisher,publication_date,access_spec,presentation_id,citation_type_id,upd_by,upd_date,entry_id,ref_type_id) SELECT cera2_temp.seq_citation.nextVal,c.title,c.authors,c.publisher,c.publication_date,c.access_spec,c.presentation_id,c.citation_type_id,\'MARTINA_STOCKHAUSE - SCHOLIX\',sysdate,:entry_id,(CASE WHEN (select ref_type_id from CERA2.REF_TYPE where upper(ref_type_acronym)=upper(:refacro)) IS NOT NULL THEN (select ref_type_id from CERA2.REF_TYPE where upper(ref_type_acronym)=upper(:refacro)) ELSE 2000005 END) ref_type_id FROM CERA2_UPD.UPD_CITATION c where c.access_spec= :doi and c.entry_id is NULL and rownum=1' 
           }

header = {'Content-Type':'text/plain;charset=UTF-8',
          'Accept':'application/json'}


testflag=''
pubflag = sys.argv[1]
# MS 2019-12-03: pcera.dkrz.de -> pcera
#db='pcera.dkrz.de'
db='pcera'
fileflag=''
try:
    testflag = sys.argv[2]
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

# configure logfile and set log file name
LOG_FILENAME = config["logdir"]+"/dli_access_pid_"+pubflag+fileflag+".log"
log = logging.getLogger()
console=logging.FileHandler(LOG_FILENAME)
formatter=logging.Formatter('%(asctime)s - %(levelname)s: %(message)s')
simple_formatter=logging.Formatter('%(message)s')
console.setFormatter(formatter)
log.setLevel(logging.INFO)
##log.setLevel(logging.DEBUG)
log.addHandler(console)
log.info('CALL: %s' % ' '.join(sys.argv))
print 'CALL: %s' % (' '.join(sys.argv))

# read sqls
sqls = {}
for l in open(mydir+'/dli_access_pid_sql.'+pubflag,'r').readlines():
    if re.match(r'^(\#)',l.strip()) or len(l.strip()) == 0:
        continue            
    key   = re.split(':',l.strip())[0]
    value = ':'.join(re.split(':',l.strip())[1:]).strip()
    sqls[key]=value

# connect to db
log.debug('Connect to CERA... %s' % db)
#if pubflag == 'WDCC':
#    cuser  = "cera2"
#elif pubflag == 'ESGF':
#    cuser  = "cera2_upd"
cuser  = "cera2_upd"
fdb=os.path.abspath(os.path.relpath(mydir+'/../.'+cuser))
#fdb=os.path.abspath(os.path.relpath(mydir+'/../.cera2_upd'))
cpw    = open(fdb,'r').read().strip()
try:
    # MS 2019-12-03: oda-scan.dkrz.de -> delphi7-scan.dkrz.de
    #sdbfile2 =  cuser+'/'+cpw+'@'+'( DESCRIPTION = ( ADDRESS_LIST = ( ADDRESS = ( PROTOCOL = TCP ) ( HOST = oda-scan.dkrz.de ) ( PORT = 1521 ) ) ) ( CONNECT_DATA = ( SERVER = DEDICATED ) ( SERVICE_NAME = '+db+' ) ))'
    sdbfile2 =  cuser+'/'+cpw+'@'+'( DESCRIPTION = ( ADDRESS_LIST = ( ADDRESS = ( PROTOCOL = TCP ) ( HOST = delphi7-scan.dkrz.de ) ( PORT = 1521 ) ) ) ( CONNECT_DATA = ( SERVER = DEDICATED ) ( SERVICE_NAME = '+db+' ) ))'
except:
    raise IOError, "\nCannot connect to DB=\'%s\'. Check password\n" % cuser

try:
    iconn = cx_Oracle.connect(sdbfile2)
    cur  = iconn.cursor()
    cur2 = iconn.cursor()
except IOError,ex:
    log.error("\nQC DB not found: %s :\n%s" % (':'.join(re.split(':',sdbfile2)[:2]),ex))

# get list of dois
try:
    cur.execute(sqls['DOI_ENTRY'])
except cx_Oracle.DatabaseError as e:
    error, = e.args
    log.error('%s: %s -> exit' % (sqls['DOI_ENTRY'],error.message))
    sys.exit()

data = cur.fetchall()

# error handling
if not data:
    log.error('No entries with DOIs found in database -> exit')
    sys.exit()

# get column names for cursor results
col_names=getColumns(cur)

# walk through result list
dumlist = []
dumsql = {}
dumtargets = {}
newrefs = []

#print data
#sys.exit()
for d in data:
    line = {}
    mysql = ''
    for k,v in zip(col_names,d):
        if str(v) == 'None':
            v=''
        line[str.upper(k)]=str(v)
    line['REFERENCES'] = []
    templ = dict(entryid=int(line['ENTRY_ID']))
    mysql=Template(sqls['DOI_REF']).safe_substitute(templ)

    # get list of references for doi
    try:
        cur2.execute(mysql)
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        log.error('%s: %s -> exit' % (mysql,error.message))
        sys.exit()

    data2 = cur2.fetchall()

    if len(data2) > 0:
        # get column names for cursor results
        #print 'len Ref>0',d,len(data2),data2
        #sys.exit()
        col_names2=getColumns(cur2)

        # walk through result list
        # dumlist = []
        for d2 in data2:
            #line2 = dict(line)
            line2 = {}
            for k2,v2 in zip(col_names2,d2):
                if str(v2) == 'None':
                    v2=''
                line2[str.upper(k2)]=str(v2)
            line['REFERENCES'].append(line2)
    #        dumlist.append(line2)

    dumlist.append(line)

##TEST
#print d,len(data2),data2
#print dumlist
#sys.exit()
#dumlist=[{'PID': '10.5072/esgf/cmip6.1402', 'REFERENCES': [], 'EXTERNAL_PID_URL': 'http://cera-www.dkrz.de/WDCC/testmeta/CMIP6/CMIP6.CMIP.NOAA-GFDL.GFDL-CM4', 'POLICY_DRS_ID': '1402', 'ENTRY_NAME': 'CMIP6.CMIP.NOAA-GFDL.GFDL-CM4', 'ENTRY_ID': '1864'}]

for d in dumlist:
    #TESTING 
    #break

    number += 1
    ep=endpoint % d['PID']
    ##TEST
    #ep = endpoint % '10.1594/wdcc/cmip5.mxel60'
    log.debug('%i. %s (%s)' % (number,d['PID'],ep))

    response, content = h.request(ep,
                                  request,
                                  headers=header)
    #print response
    #print content.strip(),len(content)
    if len(content) == 2:
        #log.info('...no results for api request (%s)' % d['PID'])
        continue
    try:
        js = json.loads(content.decode('utf-8'))
    except:
        log.error('Error in reading %s' % f)
        #print response.status
        continue

    #print js
    #sys.exit()
    for j in js:
        try:
            linkprovider = j['linkProvider'][0]['name']
        except:
            linkprovider = ''
        source = {'publisher':'','doi':'','title':'','type':''}
        try:
            source['publisher']=j['source']['publisher'][0]['name']
        except:
            pass
        try:
            source['doi']=j['source']['identifiers'][0]['identifier']
        except:
            source['doi']=''
        try:
            source['title']=j['source']['title']
        except:
            pass
        try:
            source['type']=j['source']['objectType']['type']
        except:
            pass

        relation = {'invers':'','type':'','name':'isReferencedBy'}
        try:
            relation = {'invers':j['relationship']['inverseRelationship'],'type':j['relationship']['schema'],'name':j['relationship']['name']}
        except:
            continue
        if relation['name'] in ['unknown','']:
            relation['name']='isReferencedBy'
        #print relation
        #sys.exit()
        target = {'publisher':'','doi':'','schema':'','title':'','type':'','creators':''}
        try:
            #print len(j['target']['creators']), 'first creator',j['target']['creators'][0]['name']
            authorlist = []
            for n in xrange(0,len(j['target']['creators'])):
                #print n,j['target']['creators'][n]['name']
                authorlist.append(j['target']['creators'][n]['name'].encode('UTF-8'))
            target['creators']='; '.join(authorlist)
        except:
            pass
        try:
            target['publisher']=j['target']['publisher'][0]['name']
        except:
            pass
        try:
            # MS: Do not add CMIP6 HandleIDs:21.14100;hdl:10876
            if re.search("^21.",j['target']['identifiers'][0]['identifier']) or re.search("^hdl:21.",j['target']['identifiers'][0]['identifier']):
                continue
            target['doi']=j['target']['identifiers'][0]['identifier']
        except:
            continue
        try:
            target['schema']=j['target']['identifiers'][0]['schema']
        except:
            pass
        try:
            target['title']=j['target']['title']
        except:
            pass
        try:
            target['type']=j['target']['objectType']['type']
        except:
            pass

        #if len(d['REFERENCES'])>0:
        #    print 'REFERENCES',d['REFERENCES']
        #    sys.exit()
        breakflag = 1
        for r in d['REFERENCES']:
            #print  'ref_pid',r['REF_PID'] 
            #print target['doi'].encode('UTF-8')
            #print target
            #sys.exit()
            if r['REF_PID'].lower() == target['doi'].encode('UTF-8').lower():
                log.debug('...existing %s (%s)' % (target['doi'],d['PID']))
                counts['existing'] += 1
                #print target
                #sys.exit()
                # TEST: comment next line out for testing
                breakflag = 0
                break
        if breakflag == 0:
            continue
        #sys.exit()
        #if re.search('wdcc',target['doi']):
        if re.search('wdcc',target['doi'].lower()) or re.search('esgf',target['doi'].lower()):
            log.debug('NEW INTERNAL: doi:%s (entry_id:%s) %s (%s) %s:%s (type: %s)' % (d['PID'],d['ENTRY_ID'],relation['name'],relation['type'],target['schema'],target['doi'],target['type']))
            counts['missing internal'] += 1
        else:
            if not re.search('10.',target['doi']):   
                log.debug('NEW-non-DOI: pid:%s (entry_id:%s) %s (%s) %s:%s (type: %s)' % (d['PID'],d['ENTRY_ID'],relation['name'],relation['type'],target['schema'],target['doi'],target['type']))
                #sys.exit()
                continue

            # MS 2019-07-22: check if already in newrefs
            breakflag2 = 1
            #print 'newref? ',d
            if len(newrefs)>0:
                for l in newrefs:
                    if (l['access_spec']==(target['schema']+':'+target['doi'])) and (l['entry_id']==d['ENTRY_ID']):
                        #print 'DUBLICATE:',l
                        #print 'DUBLICATE:',d
                        #sys.exit()
                        breakflag2 = 0
                        break
            if breakflag2 == 0:
                continue

            counts['missing external'] += 1
            log.info('NEW: doi:%s (entry_id:%s) %s (%s) %s:%s (type: %s)' % (d['PID'],d['ENTRY_ID'],relation['name'],relation['type'],target['schema'],target['doi'],target['type']))
            print 'NEW: doi:%s (entry_id:%s) %s (%s) %s:%s (type: %s)' % (d['PID'],d['ENTRY_ID'],relation['name'],relation['type'],target['schema'],target['doi'],target['type'])

            #newrefs.append('doi:%s entry_id:%s ref_type_acronym:%s relatedobject_schema:%s access_spec:%s:%s relatedobject_type:%s' % (d['PID'],d['ENTRY_ID'],relation['name'],relation['type'],target['schema'],target['doi'],target['type']))
            newrefs.append({'doi':d['PID'],'entry_id':d['ENTRY_ID'],'ref_type_acronym':relation['name'],'relatedobject_schema':relation['type'],'access_spec':target['schema']+':'+target['doi'],'relatedobject_type':target['type']})
            if target['doi'] in dumtargets.keys():
                continue
            dumtargets[target['doi']] = target

#for l in newrefs:
#    print l
#sys.exit() 

#open(mydir+'/upd_citation_'+pubflag+'_scholix_'+mydate+'.sql','w').write('\n'.join(dumsql.values()).encode('utf-8'))
#open(mydir+'/newrefs_'+pubflag+'_'+mydate+'.txt','w').write('\n'.join(newrefs))

sqllines = []
number=0

# enrich from crossref
# enrich from datacite
##print newrefs
##print dumtargets
#TESTING
#ESGF 
#newrefs = [{'doi': '10.1594/wdcc/hoaps3_daily', 'ref_type_acronym': u'isPreviousVersionOf', 'relatedobject_type': u'dataset', 'entry_id': '1864', 'access_spec': u'doi:10.5676/eum_saf_cm/hoaps/v001', 'relatedobject_schema': u'datacite'}, {'doi': '10.1594/wdcc/hoaps3_monthly', 'ref_type_acronym': u'isPreviousVersionOf', 'relatedobject_type': u'dataset', 'entry_id': '1864', 'access_spec': u'doi:10.5676/eum_saf_cm/hoaps/v001', 'relatedobject_schema': u'datacite'}]
#newrefs = [{'doi': '10.1594/wdcc/hoaps3_daily', 'ref_type_acronym': u'isPreviousVersionOf', 'relatedobject_type': u'dataset', 'entry_id': '2147311', 'access_spec': u'doi:10.5676/eum_saf_cm/hoaps/v001', 'relatedobject_schema': u'datacite'}, {'doi': '10.1594/wdcc/hoaps3_monthly', 'ref_type_acronym': u'isPreviousVersionOf', 'relatedobject_type': u'dataset', 'entry_id': '2139553', 'access_spec': u'doi:10.5676/eum_saf_cm/hoaps/v001', 'relatedobject_schema': u'datacite'}, {'doi': '10.1594/wdcc/wrf12_eraint_ctrl', 'ref_type_acronym': u'isReferencedBy', 'relatedobject_type': u'dataset', 'entry_id': '3519340', 'access_spec': u'doi:10.1594/pangaea.880512', 'relatedobject_schema': u'datacite'}, {'doi': '10.1594/wdcc/wrf12_gfdlesm_hist', 'ref_type_acronym': u'isReferencedBy', 'relatedobject_type': u'dataset', 'entry_id': '3519341', 'access_spec': u'doi:10.1594/pangaea.880512', 'relatedobject_schema': u'datacite'}, {'doi': '10.1594/wdcc/wrf12_gfdlesm_rcp45', 'ref_type_acronym': u'isReferencedBy', 'relatedobject_type': u'dataset', 'entry_id': '3519342', 'access_spec': u'doi:10.1594/pangaea.880512', 'relatedobject_schema': u'datacite'}, {'doi': '10.1594/wdcc/wrf12_hadgem2_rcp45', 'ref_type_acronym': u'isReferencedBy', 'relatedobject_type': u'dataset', 'entry_id': '3519344', 'access_spec': u'doi:10.1594/pangaea.880512', 'relatedobject_schema': u'datacite'}, {'doi': '10.1594/wdcc/wrf12_mpiesm_hist', 'ref_type_acronym': u'isReferencedBy', 'relatedobject_type': u'dataset', 'entry_id': '3519345', 'access_spec': u'doi:10.1594/pangaea.880512', 'relatedobject_schema': u'datacite'}, {'doi': '10.1594/wdcc/wrf12_mpiesm_rcp45', 'ref_type_acronym': u'isReferencedBy', 'relatedobject_type': u'dataset', 'entry_id': '3519346', 'access_spec': u'doi:10.1594/pangaea.880512', 'relatedobject_schema': u'datacite'}, {'doi': '10.1594/wdcc/wrf60_eraint_ctrl', 'ref_type_acronym': u'isReferencedBy', 'relatedobject_type': u'dataset', 'entry_id': '3519941', 'access_spec': u'doi:10.1594/pangaea.880512', 'relatedobject_schema': u'datacite'}, {'doi': '10.1594/wdcc/wrf60_gfdlesm_hist', 'ref_type_acronym': u'isReferencedBy', 'relatedobject_type': u'dataset', 'entry_id': '3519942', 'access_spec': u'doi:10.1594/pangaea.880512', 'relatedobject_schema': u'datacite'}, {'doi': '10.1594/wdcc/wrf60_gfdlesm_rcp45', 'ref_type_acronym': u'isReferencedBy', 'relatedobject_type': u'dataset', 'entry_id': '3519943', 'access_spec': u'doi:10.1594/pangaea.880512', 'relatedobject_schema': u'datacite'}, {'doi': '10.1594/wdcc/wrf60_hadgem2_hist', 'ref_type_acronym': u'isReferencedBy', 'relatedobject_type': u'dataset', 'entry_id': '3519944', 'access_spec': u'doi:10.1594/pangaea.880512', 'relatedobject_schema': u'datacite'}, {'doi': '10.1594/wdcc/wrf60_hadgem2_rcp45', 'ref_type_acronym': u'isReferencedBy', 'relatedobject_type': u'dataset', 'entry_id': '3519945', 'access_spec': u'doi:10.1594/pangaea.880512', 'relatedobject_schema': u'datacite'}, {'doi': '10.1594/wdcc/wrf60_mpiesm_hist', 'ref_type_acronym': u'isReferencedBy', 'relatedobject_type': u'dataset', 'entry_id': '3519946', 'access_spec': u'doi:10.1594/pangaea.880512', 'relatedobject_schema': u'datacite'}, {'doi': '10.1594/wdcc/wrf60_mpiesm_rcp45', 'ref_type_acronym': u'isReferencedBy', 'relatedobject_type': u'dataset', 'entry_id': '3519947', 'access_spec': u'doi:10.1594/pangaea.880512', 'relatedobject_schema': u'datacite'}]
#TEST
#dumtargets={u'10.1594/pangaea.880512': {'publisher': u'PANGAEA - Data Publisher for Earth & Environmental Science', 'doi': u'10.1594/pangaea.880512', 'title': u'West African Science Service Centre on Climate Change and Adapted Land Use (WASCAL) high-resolution climate simulation data, links to subset of variables at daily and monthly temporal resolution in NetCDF format, supplement to: Heinzeller, Dominikus; Dieng, Diarra; Smiatek, Gerhard; Olusegun, Christiana F; Klein, Cornelia; Hamann, Ilse; Salack, Seyni; Kunstmann, Harald (submitted): The WASCAL high-resolution regional climate simulation ensemble for West Africa. Earth System Science Data Discussions', 'creators': 'Smiatek, Gerhard; Kunstmann, Harald; Dieng, Diarra; Heinzeller, Dominikus; Klein, Cornelia; Olusegun, Christiana F; Hamann, Ilse', 'type': u'dataset', 'schema': u'doi'}, u'10.5676/eum_saf_cm/hoaps/v001': {'publisher': u'Satellite Application Facility on Climate Monitoring (CM SAF)', 'doi': u'10.5676/eum_saf_cm/hoaps/v001', 'title': u'Monthly Means / 6-Hourly Composites', 'creators': 'Schr\xc3\xb6der, Marc; Bakan, Stephan; Andersson, Axel; Klepp, Christian-Phillip; Fennig, Karsten', 'type': u'dataset', 'schema': u'doi'}}

for doi in dumtargets.keys():
    doi_prefix=re.split('/',doi)[0]
    # MS: check doi string validity
    if not re.search("^10.",doi):
        continue
    number += 1
    #ep = crossref_api % '10.1175/jcli-d-12-00623.1'
    if dumtargets[doi]['type'] == 'publication':
        ep = crossref_api % doi
    elif  dumtargets[doi]['type'] == 'dataset':
        ep = datacite_api % doi
    elif doi_prefix in paper_except: #re.search('10.5194',doi) or re.search('10.1175',doi) or re.search('10.26686',doi) or re.search('10.31223',doi) or re.search('10.1007',doi) or  re.search('10.2307',doi) or re.search('10.1038',doi) or re.search('10.1002',doi) or re.search('10.4324',doi):  # journal exemptions: GMD, BAMS, nature
        ep = crossref_api % doi
        dumtargets[doi]['type'] = 'publication'
        #print 'EXCEPTION: DOI=%s,type=%s' % (doi,dumtargets[doi]['type'])
    else:
        ep = datacite_api % doi
        dumtargets[doi]['type'] = 'dataset'
        #continue
    log.info('citation: %s (%s)' % (doi,ep))
    print 'citation: %s (%s)' % (doi,ep)
    #print dumsql[doi]
    response, content = h.request(ep,
                                  request,
                                  headers=header)
    #print response
    #print content.strip(),len(content)
    if response.status == '404' or response['status'] == '404':
        log.error(response.status)
        #sqllines.append(dumtargets[doi])
        continue

    try:
        js = json.loads(content.decode('utf-8'))
        sqllines.append(dumtargets[doi])
    except:
        try:
            js = json.loads(content)
            sqllines.append(dumtargets[doi])
        except:
            # log.error('Error in reading %s' % f)
            log.error('JSON load: '+response.status)
            continue
        
    #print response.status
    #print len(content)
    #sys.exit()
    if len(content) == 3:
        log.debug('...no results for crossref api request (%s)' % doi)
        sqllines.append(dumtargets[doi])
        continue

    dumdict = {'authors':''}
    #dumdict['publication'] = js['message']['container-title']

    # crossref
    if dumtargets[doi]['type'] == 'publication':

        for k,v in js['message'].iteritems():
            #dumdict = {'authors':''}
            dumdict['publication'] = js['message']['container-title']

            if k =='author':
                for a in v:
                    # print a['family'],a['given']
                    try:
                        dumdict['authors'] += a['family']+', '+a['given']+'; '
                    except:
                        try:
                            dumdict['authors'] += a['name']+'; '
                        except:
                            try:
                                dumdict['authors'] += a['literal']+'; '
                            except:
                                print a
                                print v
                            
            elif k == 'created':
                # print v['date-time']
                try:
                    dumdict['pubdate'] = datetime.datetime.strptime( v['date-time'], "%Y-%m-%dT%H:%M:%SZ" )
                except:
                    try:
                        dumdict['pubdate'] = datetime.datetime.strptime( v['date-time'], "%Y-%m-%dT%H:%M:%S" )
                    except:
                        dumdict['pubdate'] = datetime.datetime(1900,1,1)

            elif k == 'title':
                dumdict['title'] = v[0]
            elif k == 'publisher':
                dumdict['publisher'] = v
            elif k.upper() == 'DOI':
                dumdict['doi'] = v

    # datacite
    else:
        dumdict['publication'] = ''
        for k,v in js['data']['attributes'].iteritems():
            #if k =='author':
            if k =='creators':
                for a in v:
                    # print a['family'],a['given']
                    try:
                        #dumdict['authors'] += a['family']+', '+a['given']+'; '
                        dumdict['authors'] += a['familyName']+', '+a['givenName']+'; '
                    except:
                        try:
                            dumdict['authors'] += a['name']+'; '
                        except:
                            try:
                                dumdict['authors'] += a['literal']+'; '
                            except:
                                print a
                                print v

            elif k == 'registered':
                try:
                    dumdict['pubdate'] = datetime.datetime.strptime( v, "%Y-%m-%dT%H:%M:%SZ" )
                except:
                    try:
                        dumdict['pubdate'] = datetime.datetime.strptime( v, "%Y-%m-%dT%H:%M:%S" )
                    except:
                        try:
                            dumdict['pubdate'] = datetime.datetime.strptime( v, "%Y-%m-%dT%H:%M:%S.%fZ" )
                        except:
                            try:
                                dumdict['pubdate'] = datetime.datetime.strptime( v, "%Y-%m-%dT%H:%M:%S.%f" )
                            except:
                                dumdict['pubdate'] = datetime.datetime(1900,1,1)

            #elif k == 'title':
            #    dumdict['title'] = v
            elif k == 'titles':
                dumdict['title'] = v[0]['title']
            #elif k == 'container-title':
            #    dumdict['publisher'] = v
            elif k == 'publisher':
                dumdict['publisher'] = v
            elif k.upper() == 'DOI':
                dumdict['doi'] = v #re.sub('::',':',v)


        # print k,v
    dumdict['authors'] = dumdict['authors'][:-2]
    
    if dumtargets[doi]['type'] in citid:
        cittypeid=citid[dumtargets[doi]['type']]
    else:
        cittypeid=citid['other']

    # INSERT upd_citation
    if len(testflag) == 0:
        #print 'insert ',sqlcite[pubflag]
        #cur.prepare(sqlcite)
        cur.prepare(sqlcite[pubflag])
        #print dumdict
        #sys.exit()
        try:
            cur.execute(None,doi='doi:'+dumdict['doi'],title=dumdict['title'].encode('utf-8'),authors=dumdict['authors'].encode('utf-8'),publisher=dumdict['publisher'].encode('utf-8'),pubyear=dumdict['pubdate'].strftime("%Y-%m-%d"),citation_type=cittypeid)
            log.info('MERGE into upd_citation (doi=%s)' % (dumdict['doi']))

            num_inserts['citation'] += 1
        except:
            try:
                cur.execute(None,doi='doi:'+dumdict['doi'],title=dumdict['title'].encode('utf-8'),authors=dumdict['authors'],publisher=dumdict['publisher'].encode('utf-8'),pubyear=dumdict['pubdate'].strftime("%Y-%m-%d"),citation_type=cittypeid)
                log.info('MERGE2 into upd_citation (doi=%s)' % (dumdict['doi']))

                num_insert['citation'] += 1
            except:
                try:
                    cur.execute(None,doi='doi:'+dumdict['doi'],title=dumdict['title'].encode('utf-8'),authors=dumdict['authors'],publisher=dumdict['publisher'],pubyear=dumdict['pubdate'].strftime("%Y-%m-%d"),citation_type=cittypeid)
                    log.info('MERGE3 into upd_citation (doi=%s)' % (dumdict['doi']))
                    num_inserts['citation'] += 1
                except:
                    try:
                        cur.execute(None,doi='doi:'+dumdict['doi'],title=dumdict['title'],authors=dumdict['authors'],publisher=dumdict['publisher'],pubyear=dumdict['pubdate'].strftime("%Y-%m-%d"),citation_type=cittypeid)
                        log.info('MERGE4 into upd_citation (doi=%s)' % (dumdict['doi']))

                        num_inserts['citation'] += 1
                    except cx_Oracle.DatabaseError as e:
                        error, = e.args
                        #log.error('MERGE into upd_citation (doi=%s) failed (%s): %s -> exit' % (dumdict['doi'],sqlcite,error.message))
                        log.error('MERGE into upd_citation (doi=%s) failed (%s): %s -> exit' % (dumdict['doi'],sqlcite[pubflag],error.message))

# commit citation inserts
##TESTING
##iconn.rollback()
if len(testflag) == 0:
    iconn.commit()

# INSERT REFERENCES
for r in newrefs:
    # INSERT reference (upd_citation)
    #print r
    if len(testflag) == 0:
        cur.prepare(sqlref[pubflag])
        #print sqlref[pubflag]
        #print dumdict
        #sys.exit()
        try:
            if pubflag == 'ESGF':
                cur.execute(None,entry_id=r['entry_id'],doi=r['access_spec'],refacro=r['ref_type_acronym'],scholix='SCHOLIX')
            else:
                cur.execute(None,entry_id=r['entry_id'],doi=r['access_spec'],refacro=r['ref_type_acronym'])
            log.info('INSERT into reference/upd_citation (entry_id=%s, doi=%s, ref_type_acronym=%s)' % (r['entry_id'],r['access_spec'],r['ref_type_acronym']))

            num_inserts['reference'] += 1
        except:
            try:
                cur.execute(None,entry_id=r['entry_id'],doi=r['access_spec'].encode('utf-8'),refacro=r['ref_type_acronym'].encode('utf-8'))
                log.info('INSERT2 into reference/upd_citation (entry_id=%, doi=%s, ref_type_acronym=%s)' % (r['entry_id'],r['access_spec'].encode('utf-8'),r['ref_type_acronym'].encode('utf-8')))

                num_inserts['reference'] += 1
            except cx_Oracle.DatabaseError as e:
                error, = e.args
                log.error('INSERT into reference/upd_citation (entry_id=%s, doi=%s, ref_type_acronym=%s) -> failed: %s' % (r['entry_id'],r['access_spec'],r['ref_type_acronym'],error.message))
    ##TESTING
    #break
##TESTING
##iconn.rollback()
if len(testflag) == 0:
    iconn.commit()

if num_inserts['citation']>0 or num_inserts['reference']>0:
    log.info('Total number of inserts: citation=%i reference=%i)' % (num_inserts['citation'],num_inserts['reference']))
    print 'Total number of inserts: citation=%i reference=%i)' % (num_inserts['citation'],num_inserts['reference'])
elif counts['missing external'] > 0 and len(testflag) > 0:
    print 'Total number of available new references=%i' % counts['missing external']
else:
    log.info('No new references available')
    print 'No new references available'
    
cur.close()
cur2.close()
iconn.close()
