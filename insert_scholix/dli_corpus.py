#! /usr/bin/env python

""" Insert/Update citation/reference from DataCite Citation Corpus file (https://zenodo.org/doi/10.5281/zenodo.11196858) for WDCC and ESGF DOIs
Version: V0.7 2024-10-14: db change (stockhause@dkrz.de);
Version: V0.6 2024-09-26: dli_access.py -> dli_corpus.py
Version: V0.5 2020-06-17: datacite api v1 -> v2
Version: V0.4 2020-04-16: tcera1 -> testdb
Version: V0.3 2019-12-03: DB hardware/software exchange
Version: V0.2 2018-09-06, WDCC Scholix merge changed after discussion with HHW, stockhause@dkrz.de
Version: V0.1 2018-08-15, stockhause@dkrz.de"""
# Usage: ./dli_corpus.py <WDCC|ESGF> <filedir> [<test|testdb|testdbtest>]

import sys,os,re,httplib2
import urllib
import csv,json
import datetime
from string import Template
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
idf_api='https://doi.org/ra/%s'
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
sqlref = { 'ESGF':'INSERT INTO CMIP6_CITE_TEST."REFERENCE" (citation_id,ref_type_id,ref_citation_id,upd_by,modification_date) SELECT :entry_id,(CASE WHEN (select ref_type_id from CERA2.REF_TYPE where upper(ref_type_acronym)=upper(:refacro)) IS NOT NULL THEN (select ref_type_id from CERA2.REF_TYPE where upper(ref_type_acronym)=upper(:refacro)) ELSE 2000015 END) ref_type_id,c.citation_id,:scholix,sysdate FROM CERA2_UPD.UPD_CITATION c where c.access_spec= :doi and c.entry_id is NULL and rownum=1', 
#'ESGF':'INSERT INTO CMIP6_CITE_TEST."REFERENCE" (citation_id,ref_type_id,ref_citation_id) SELECT :entry_id,(CASE WHEN (select ref_type_id from CERA2.REF_TYPE where upper(ref_type_acronym)=upper(:refacro)) IS NOT NULL THEN (select ref_type_id from CERA2.REF_TYPE where upper(ref_type_acronym)=upper(:refacro)) ELSE 2000015 END) ref_type_id,c.citation_id FROM CERA2_UPD.UPD_CITATION c where c.access_spec= :doi and c.entry_id is NULL and rownum=1', #insert into reference values (citation_id,ref_type_id,ref_citation_id);
           'WDCC':'INSERT INTO CERA2_UPD."REFERENCE" (entry_id,citation_id,ref_type_id) SELECT :entry_id,c.citation_id,(CASE WHEN (select ref_type_id from CERA2.REF_TYPE where upper(ref_type_acronym)=upper(:refacro)) IS NOT NULL THEN (select ref_type_id from CERA2.REF_TYPE where upper(ref_type_acronym)=upper(:refacro)) ELSE 2000015 END) ref_type_id FROM CERA2_UPD.UPD_CITATION c where c.access_spec= :doi and rownum=1' #'insert into cera2_upd.reference values (entry_id,citation_id,ref_type_id);
           #'WDCC':'INSERT INTO CERA2."REFERENCE" (entry_id,citation_id,ref_type_id) SELECT :entry_id,c.citation_id,(CASE WHEN (select ref_type_id from CERA2.REF_TYPE where upper(ref_type_acronym)=upper(:refacro)) IS NOT NULL THEN (select ref_type_id from CERA2.REF_TYPE where upper(ref_type_acronym)=upper(:refacro)) ELSE 2000015 END) ref_type_id FROM CERA2.CITATION c where c.access_spec= :doi and rownum=1' #'insert into cera2_upd.reference values (entry_id,citation_id,ref_type_id);
           #'WDCC':'INSERT INTO CERA2_UPD.UPD_CITATION (citation_id,title,authors,publisher,publication_date,access_spec,presentation_id,citation_type_id,upd_by,upd_date,entry_id,ref_type_id) SELECT cera2_temp.seq_citation.nextVal,c.title,c.authors,c.publisher,c.publication_date,c.access_spec,c.presentation_id,c.citation_type_id,\'MARTINA_STOCKHAUSE - SCHOLIX\',sysdate,:entry_id,(CASE WHEN (select ref_type_id from CERA2.REF_TYPE where upper(ref_type_acronym)=upper(:refacro)) IS NOT NULL THEN (select ref_type_id from CERA2.REF_TYPE where upper(ref_type_acronym)=upper(:refacro)) ELSE 2000015 END) ref_type_id FROM CERA2_UPD.UPD_CITATION c where c.access_spec= :doi and c.entry_id is NULL and rownum=1' 
           }

header = {'Content-Type':'text/plain;charset=UTF-8',
          'Accept':'application/json'}

testflag=''
pubflag = sys.argv[1]
filedir = sys.argv[2]
# MS 2019-12-03: pcera.dkrz.de -> pcera
# MS 2024-10-14: testdb -> tcera; delphi7-scan.dkrz.de -> cera-db.dkrz.de/cera-testdb.dkrz.de
#db='pcera.dkrz.de'
db='pcera'
db2='cera-db.dkrz.de'
fileflag=''
try:
    testflag = sys.argv[3]
    if testflag == 'testdb' or testflag == 'testdbtest':
        # MS 2019-12-03: testdb -> tcera1
        # MS 2020-04-16: tcera1 -> testdb
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

#!!!!!!!!!!!!!!!!!
testflag='test'
    
# configure and read logfile
log=open(config["logdir"]+"/dli_corpus_"+pubflag+".log","r+")
# action required?
for line in log.readlines():
    if re.search(r'^#',line):
        continue
    else:
	(ODATE,STATUS,OVERSION)=re.split(' ',line.strip())

DATE=os.popen('/bin/date +%Y-%m-%d').read().strip()

if STATUS == 'done':
    if int(''.join(re.split('-',DATE)))-int(''.join(re.split('-',ODATE))) < 2:
        sys.exit()


# find corpus files 
corp_files =[]
os.chdir(filedir)
for root, dirs, files in os.walk(".", topdown=False):
    for f in files:
        if not re.search('.csv$',f):
            continue
        corp_files.append(os.path.join(os.path.abspath(root),f))
os.chdir(mydir)

if len(corp_files)==0:
    print "No csv files found in %s" % filedir
    sys.exit()

# read sqls
sqls = {}
for l in open(mydir+'/dli_access_pid_sql.'+pubflag,'r').readlines():
    if re.match(r'^(\#)',l.strip()) or len(l.strip()) == 0:
        continue            
    key   = re.split(':',l.strip())[0]
    value = ':'.join(re.split(':',l.strip())[1:]).strip()
    sqls[key]=value

# connect to db
#if pubflag == 'WDCC':
#    cuser  = "cera2"
#elif pubflag == 'ESGF':
#    cuser  = "cera2_upd"
cuser  = "cera2_upd"
fdb=os.path.abspath(os.path.relpath(mydir+'/../.'+cuser))
cpw    = open(fdb,'r').read().strip()
try:
    # MS 2019-12-03: oda-scan.dkrz.de -> delphi7-scan.dkrz.de
    # MS 2024-10-14: delphi7-scan.dkrz.de -> cera-db.dkrz.de or cera-testdb.dkrz.de
    #sdbfile2 =  cuser+'/'+cpw+'@'+'( DESCRIPTION = ( ADDRESS_LIST = ( ADDRESS = ( PROTOCOL = TCP ) ( HOST = oda-scan.dkrz.de ) ( PORT = 1521 ) ) ) ( CONNECT_DATA = ( SERVER = DEDICATED ) ( SERVICE_NAME = '+db+' ) ))'
    #sdbfile2 =  cuser+'/'+cpw+'@'+'( DESCRIPTION = ( ADDRESS_LIST = ( ADDRESS = ( PROTOCOL = TCP ) ( HOST = delphi7-scan.dkrz.de ) ( PORT = 1521 ) ) ) ( CONNECT_DATA = ( SERVER = DEDICATED ) ( SERVICE_NAME = '+db+' ) ))'
    sdbfile2 =  cuser+'/'+cpw+'@'+'( DESCRIPTION = ( ADDRESS_LIST = ( ADDRESS = ( PROTOCOL = TCP ) ( HOST = '+db2+' ) ( PORT = 1521 ) ) ) ( CONNECT_DATA = ( SERVER = DEDICATED ) ( SERVICE_NAME = '+db+' ) ))'
except:
    raise IOError, "\nCannot connect to DB=\'%s\'. Check password\n" % cuser

try:
    iconn = cx_Oracle.connect(sdbfile2)
    cur  = iconn.cursor()
    cur2 = iconn.cursor()
except IOError,ex:
    print "\nQC DB not found: %s :\n%s" % (':'.join(re.split(':',sdbfile2)[:2]),ex)

# get list of dois
try:
    cur.execute(sqls['DOI_ENTRY'])
except cx_Oracle.DatabaseError as e:
    error, = e.args
    print '%s: %s -> exit' % (sqls['DOI_ENTRY'],error.message)
    sys.exit()

data = cur.fetchall()

# error handling
if not data:
    print'No entries with DOIs found in database -> exit'
    sys.exit()

# get column names for cursor results
col_names=getColumns(cur)

# walk through result list
dumlist = []
dumsql = {}
dumtargets = {}
newrefs = []

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
        print '%s: %s -> exit' % (mysql,error.message)
        sys.exit()

    data2 = cur2.fetchall()

    if len(data2) > 0:
        # get column names for cursor results
        col_names2=getColumns(cur2)

        # walk through result list
        for d2 in data2:
            #line2 = dict(line)
            line2 = {}
            for k2,v2 in zip(col_names2,d2):
                if str(v2) == 'None':
                    v2=''
                line2[str.upper(k2)]=str(v2)
            line['REFERENCES'].append(line2)

    dumlist.append(line)


# NEW read corp file loop
counter=0
corp_entry={}
for file in corp_files:
    with open(file,'r') as f:
        csv.DictReader(f)
        for line in f:
            if counter==0:
                keys=re.split(',',line.strip())
            else:
                dum=re.split(',',line.strip())
                for (k,v) in zip(keys,dum):
                    corp_entry[k]=v
                if not re.search('10.22033',corp_entry['dataset']):
                    continue
                #TESTING
                #print 'csv: ',corp_entry['dataset'],corp_entry['publication']
                breakflag = 1
                for d in dumlist:
                    number += 1
                    pubdoi  = '10.'+re.split('10\.',corp_entry['publication'])[1]
                    datadoi = '10.'+re.split('10\.',corp_entry['dataset'])[1]
                    #print 'MYD',d['PID'],datadoi
                    if d['PID'] != datadoi:
                        continue
                    for r in d['REFERENCES']:
                        if r['REF_PID'].lower() == pubdoi.lower():
                            #print '...existing %s (%s)' % (pubdoi,d['PID'])
                            counts['existing'] += 1
                            breakflag = 0
                            break
                    if breakflag == 0:
                        break

                    #relation='Cites' if (corp_entry['source']=='datacite') else 'IsCitedBy'
                    relation='IsCitedBy'
                    if re.search('wdcc',corp_entry['publication'].lower()) or re.search('esgf',corp_entry['publication'].lower()):
                        print 'NEW INTERNAL: doi:%s (entry_id:%s) %s ref_doi:%s ' % (d['PID'],d['ENTRY_ID'],relation,corp_entry['publication']),corp_entry['source']
                        counts['missing internal'] += 1
                    else:
                        print 'NEW EXTERNAL: doi:%s (entry_id:%s) %s ref_doi:%s datadoi: %s' % (d['PID'],d['ENTRY_ID'],relation,pubdoi,datadoi)
                        newrefs.append({'doi':d['PID'],'entry_id':d['ENTRY_ID'],'ref_type_acronym':relation,'relatedobject_schema':'','access_spec':'doi:'+pubdoi,'relatedobject_type':''})
                        if pubdoi in dumtargets.keys():
                            continue
                        dumtargets[pubdoi] = corp_entry

                    counts['missing external'] += 1
                    continue

            counter += 1

print counts
print '%i NEW in %i entries' % (counter,number)
#print dumtarget
#print newrefs
#sys.exit()

sqllines = []
number=0
for doi in dumtargets.keys():
    doi_prefix=re.split('/',doi)[0]
    # MS: check doi string validity
    if not re.search("^10.",doi):
        continue
    number += 1

    # check if Crossref or DataCite DOI
    # SSL error with httplib access
    ep = idf_api % doi
    try:
        js = json.loads(urllib.urlopen(ep).read())
    except:
        print 'Error in reading from %s' % ep
        continue
    if js[0]["RA"] == 'DataCite':
        dumtargets[doi]['type'] = 'dataset'
        ep = datacite_api % doi
    elif js[0]["RA"] == 'Crossref':
        dumtargets[doi]['type'] = 'publication'
        ep = crossref_api % doi
    else:
        print 'Unknown DOI provider'
        continue
    #sys.exit()
    
    print 'citation: %s (%s)' % (doi,ep)
    #print dumsql[doi]

    
    # SSL error -> replace httplib by urlopen
    try:
        js = json.loads(urllib.urlopen(ep).read().decode('utf-8'))
        sqllines.append(dumtargets[doi])
        #print js
    except:
        try:
            js = json.loads(urllib.urlopen(ep).read())
            sqllines.append(dumtargets[doi])
            #print js
        except:
            print 'Error in reading from %s' % ep
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

    #print sqlcite[pubflag]
    print 'citation - DOI:'+dumdict['doi'],'TITLE:'+dumdict['title'].encode('utf-8'),'AUTHORS:'+dumdict['authors'].encode('utf-8'),'PUBLISHER:'+dumdict['publisher'].encode('utf-8'),'PUBDATE:',dumdict['pubdate'].strftime("%Y-%m-%d"),'CITID:',cittypeid

    # INSERT upd_citation
    if len(testflag) == 0:
        #print 'insert ',sqlcite[pubflag]
        #cur.prepare(sqlcite)
        cur.prepare(sqlcite[pubflag])
        #print dumdict
        #sys.exit()
        try:
            cur.execute(None,doi='doi:'+dumdict['doi'],title=dumdict['title'].encode('utf-8'),authors=dumdict['authors'].encode('utf-8'),publisher=dumdict['publisher'].encode('utf-8'),pubyear=dumdict['pubdate'].strftime("%Y-%m-%d"),citation_type=cittypeid)
            print 'MERGE into upd_citation (doi=%s)' % (dumdict['doi'])

            num_inserts['citation'] += 1
        except:
            try:
                cur.execute(None,doi='doi:'+dumdict['doi'],title=dumdict['title'].encode('utf-8'),authors=dumdict['authors'],publisher=dumdict['publisher'].encode('utf-8'),pubyear=dumdict['pubdate'].strftime("%Y-%m-%d"),citation_type=cittypeid)
                print 'MERGE2 into upd_citation (doi=%s)' % (dumdict['doi'])

                num_insert['citation'] += 1
            except:
                try:
                    cur.execute(None,doi='doi:'+dumdict['doi'],title=dumdict['title'].encode('utf-8'),authors=dumdict['authors'],publisher=dumdict['publisher'],pubyear=dumdict['pubdate'].strftime("%Y-%m-%d"),citation_type=cittypeid)
                    print 'MERGE3 into upd_citation (doi=%s)' % (dumdict['doi'])
                    num_inserts['citation'] += 1
                except:
                    try:
                        cur.execute(None,doi='doi:'+dumdict['doi'],title=dumdict['title'],authors=dumdict['authors'],publisher=dumdict['publisher'],pubyear=dumdict['pubdate'].strftime("%Y-%m-%d"),citation_type=cittypeid)
                        print 'MERGE4 into upd_citation (doi=%s)' % (dumdict['doi'])

                        num_inserts['citation'] += 1
                    except cx_Oracle.DatabaseError as e:
                        error, = e.args
                        print 'MERGE into upd_citation (doi=%s) failed (%s): %s -> exit' % (dumdict['doi'],sqlcite[pubflag],error.message)

# commit citation inserts
##TESTING
iconn.rollback()
if len(testflag) == 0:
    iconn.commit()
#!!!!!!!!!!!!!!!!!
testflag=''

    
# INSERT REFERENCES
for r in newrefs:
    # INSERT reference (upd_citation)
    #print sqlref[pubflag]
    print 'reference - ',r['entry_id'],r['access_spec'],r['ref_type_acronym'],'SCHOLIX'
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
            print 'INSERT into reference/upd_citation (entry_id=%s, doi=%s, ref_type_acronym=%s)' % (r['entry_id'],r['access_spec'],r['ref_type_acronym'])

            num_inserts['reference'] += 1
        except:
            try:
                cur.execute(None,entry_id=r['entry_id'],doi=r['access_spec'].encode('utf-8'),refacro=r['ref_type_acronym'].encode('utf-8'))
                print 'INSERT2 into reference/upd_citation (entry_id=%, doi=%s, ref_type_acronym=%s)' % (r['entry_id'],r['access_spec'].encode('utf-8'),r['ref_type_acronym'].encode('utf-8'))

                num_inserts['reference'] += 1
            except cx_Oracle.DatabaseError as e:
                error, = e.args
                print 'INSERT into reference/upd_citation (entry_id=%s, doi=%s, ref_type_acronym=%s) -> failed: %s' % (r['entry_id'],r['access_spec'],r['ref_type_acronym'],error.message)
    ##TESTING
    #break
##TESTING
#iconn.rollback()
if len(testflag) == 0:
    iconn.commit()
    log.write('%s %s %s\n' % (DATE,'done','V2.0'))



if num_inserts['citation']>0 or num_inserts['reference']>0:
    print 'Total number of inserts: citation=%i reference=%i)' % (num_inserts['citation'],num_inserts['reference'])
elif counts['missing external'] > 0 and len(testflag) > 0:
    print 'Total number of available new references=%i' % counts['missing external']
else:
    print 'No new references available'
    
cur.close()
cur2.close()
iconn.close()
