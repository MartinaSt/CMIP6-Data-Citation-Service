#! /usr/bin/env python

""" Insert/Update model information based on CMIP6_CV at https://github.com/WCRP-CMIP/CMIP6_CVs
Version: V0.9 2024-10-14: db change (stockhause@dkrz.de)
Version: V0.8 2022-06-02: source_id change (part 2): access and process new sections in source_id JSON
Version: V0.7 2022-03-15: source_id change (part 1): extend table
Version: V0.6 2020-04-16: tcera1 -> testdb
Version: V0.5 2019-12-03: DB hardware/software exchange
Version: V0.4 2019-11-21, fill new model table columns with default values
Version: V0.3 2019-07-16, changed format for CV institution_id implemented
Version: V0.2 2018-11-16, support for cx_Oracle 7
Version: V0.1 2018-03-15, stockhause@dkrz.de"""

# Usage: ./generateModel.py [<test|testdb|testdbtest>]

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


def getColumns(gcur):
    """getColumns: get columns for cursor results"""

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


# read options and analyze testflag
mydate = os.popen('date +%F').read().strip()

# MS 2019-12-03: pcera.dkrz.de -> pcera
# MS 2024-10-14: testdb -> tcera; delphi7-scan.dkrz.de -> cera-db.dkrz.de/cera-testdb.dkrz.de
#db='pcera.dkrz.de'
db='pcera'
db2='cera-db.dkrz.de'
fileflag=''
try:
    testflag = sys.argv[1]
    print 'TEST: %s' % testflag
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


# configure logfile and set log file name
LOG_FILENAME = config["logdir"]+"/generateModel"+fileflag+".log"
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
        #print('files ',os.path.join(root, f))
        if not re.search('.json',f):
            continue
        print 'read ',f
        try:
            js2 = json.loads(open(f,'r').read())
        except:
            print 'Error in reading %s' % f
            continue
        
        if re.search('_activity_id.json',f):
            cv_mips = js2

        elif re.search('6_experiment_id.json',f):
            # MS 2016-09-02:error correction still required
            js3 = {}
            #print js2.keys()
            for k,v in js2["experiment_id"].iteritems():
                #print k,v
                #sys.exit()
                if type(v["activity_id"]) != list:
                    ##pass
                    dumv = {}
                    dum = []
                    dum.append(v["activity_id"])
                    for k2,v2 in v.iteritems():
                        if k2 == "activity_id":
                            dumv[k2] = dum
                        else:
                            dumv[k2] = v2
                    js3[k] = dumv
                    #print "LIST ", v["activity_id"]
                else:
                    js3[k] = v
                    #print k,v
                    #sys.exit()
                
                #print v["activity_id"]
            cv_experimentsx = js3
            #print  cv_experimentsx
            #sys.exit()
        elif re.search('_institution_id.json',f):
            dum_institutes = js2
        elif re.search('_source_id.json',f):
            dum_models = js2
        else:
            print 'SKIP %s' % f

# merge institutes/models
#print dum_models
#sys.exit()
cv_modelsx = {}
for k,v in dum_models["source_id"].iteritems():
    for k2,v2 in dum_institutes["institution_id"].iteritems():
        if k2 in v['institution_id']:
            dum_dict = {'source':v['label'],'institution_id':k2,'institution':v2}
            dum_dict['mod_details']=v
            cv_modelsx[k+'$'+k2]=dum_dict

cv_models = sorted(cv_modelsx.items())
# New format for CV institution_id for testing
#cv_models=[(u'ACCESS-CM2$CSIRO-ARCCSS-BoM', {'institution_id': u'CSIRO-ARCCSS-BoM', 'source': u'ACCESS-CM2','mod_details': {u'label_extended': u'Australian Community Climate and Earth System Simulator Climate Model Version 2', u'cohort': [u'Registered'], u'release_year': u'2018', u'model_component': {u'seaIce': {u'native_nominal_resolution': u'100 km', u'description': u'CICE5.1 (same grid as ocean)'}, u'land': {u'native_nominal_resolution': u'250 km', u'description': u'CABLE2.3.5'}, u'landIce': {u'native_nominal_resolution': u'none', u'description': u'none'}, u'atmosChem': {u'native_nominal_resolution': u'none', u'description': u'none'}, u'ocean': {u'native_nominal_resolution': u'100 km', u'description': u'ACCESS-OM2 (GFDL-MOM5, tripolar primarily 1deg; 360 x 300 longitude/latitude; 50 levels; top grid cell 0-10 m)'}, u'atmos': {u'native_nominal_resolution': u'250 km', u'description': u'MetUM-HadGEM3-GA7.1 (N96; 192 x 144 longitude/latitude; 85 levels; top level 85 km)'}, u'aerosol': {u'native_nominal_resolution': u'250 km', u'description': u'UKCA-GLOMAP-mode'}, u'ocnBgchem': {u'native_nominal_resolution': u'none', u'description': u'none'}},u'label': u'ACCESS-CM2', u'institution_id': [u'CSIRO-ARCCSS-BoM'], u'source_id': u'ACCESS-CM2', u'activity_participation': [u'CMIP', u'FAFMIP', u'OMIP', u'RFMIP', u'ScenarioMIP']},'institution': {'name':u'Commonwealth Scientific and Industrial Research Organisation, Australian Research Council Centre of Excellence for Climate System Science, and Bureau of Meteorology','postalAddress':'Aspendale, Victoria 3195, Australia','coordinates':'45.09,45.8976','homepage':'http://www.csiro.au','consortia':{}}}),(u'EC-Earth3$EC-Earth-Consortium', {'institution_id': u'EC-Earth-Consortium', 'source': u'EC-Earth3','mod_details': {u'label_extended': u'EC Earth 3.3', u'cohort': [u'Registered'], u'release_year': u'2019', u'model_component': {u'seaIce': {u'native_nominal_resolution': u'100 km', u'description': u'LIM3'}, u'land': {u'native_nominal_resolution': u'100 km', u'description': u'HTESSEL (land surface scheme built in IFS)'}, u'landIce': {u'native_nominal_resolution': u'none', u'description': u'none'}, u'atmosChem': {u'native_nominal_resolution': u'none', u'description': u'none'}, u'ocean': {u'native_nominal_resolution': u'100 km', u'description': u'NEMO3.6 (ORCA1 tripolar primarily 1 deg with meridional refinement down to 1/3 degree in the tropics; 362 x 292 longitude/latitude; 75 levels; top grid cell 0-1 m)'}, u'atmos': {u'native_nominal_resolution': u'100 km', u'description': u'IFS cy36r4 (TL255, linearly reduced Gaussian grid equivalent to 512 x 256 longitude/latitude; 91 levels; top level 0.01 hPa)'}, u'aerosol': {u'native_nominal_resolution': u'none', u'description': u'none'}, u'ocnBgchem': {u'native_nominal_resolution': u'none', u'description': u'none'}}, u'label': u'EC-Earth3', u'institution_id': [u'EC-Earth-Consortium'], u'source_id': u'EC-Earth3', u'activity_participation': [u'CMIP', u'CORDEX', u'DCPP', u'DynVarMIP', u'LS3MIP', u'PAMIP', u'RFMIP', u'SIMIP', u'ScenarioMIP', u'VIACSAB', u'VolMIP']},'institution': {'name':u'','postalAddress':u'EC-Earth consortium, Rossby Center, Swedish Meteorological and Hydrological Institute/SMHI, SE-601 76 Norrkoping, Sweden','coordinates':'','homepage':'http://www.ec-earth.se','consortia':{u'AEMET':{'name':'','postalAddress':u'Spain','coordinates':'30.59,98.09','homepage':u'http://www.aemet.es'}, u'BSC':{'name':u'Barcelona Supercomputing Centre','postalAddress':u'Spain','coordinates':'30.59,98.09','homepage':u'http://www.bsc.es'}, u'CNR-ISAC':{'name':u'CNR-ISAC','postalAddress':u'Italy','coordinates':'30.59,98.09','homepage':u'http://www.cnr-isac.it'}, u'DMI':{'name':u'Denish Meteorological Institute','postalAddress':u'Denmark','coordinates':'30.59,98.09','homepage':u'http://www.dmi.dk'}}}})]
#print cv_models
#sys.exit()
cv_experiments = sorted(cv_experimentsx.items())

##print 'No. models:      %i' % len(cv_models)
##print 'No. experiments: %i' % len(cv_experiments)


models=[]
for (k,v) in cv_models:
    dummodel = {}
    resolutions = []
    components = []
    for (k2,v2) in v["mod_details"]["model_component"].iteritems():
        if len(v2['description']) == 0 or v2['description'].lower() == 'none': # component does not exist
            continue

        try:
            if len(v2['nominal_resolution']) == 0:
                resolutions.append(k2+': none')
            else:
                resolutions.append(k2+': '+ v2['nominal_resolution'])
        except:
            pass

        # CMIP6 CV: renaming of nominal_resolution in native_nominal_resolution
        try:
            if len(v2['native_nominal_resolution']) == 0:
                resolutions.append(k2+': none')
            else:
                resolutions.append(k2+': '+ v2['native_nominal_resolution'])
        except:
            pass

        components.append(k2+': '+ v2['description'])

    #print k,v
    #print k,v["mod_details"]
    #print v['mod_details']['license_info']

    dummodel['model_id']            = 'cmip6_cite_test.seq_models.nextVal'
    dummodel['model_acronym']       = re.split('\$',k)[0]
    dummodel['model_label']         = v["mod_details"]["label"]
    dummodel['model_name']          = v["mod_details"]["label_extended"]
    dummodel['resolutions']         = ', '.join(sorted(resolutions))
    dummodel['components']          = ', '.join(sorted(components))
    dummodel['release_year']        = int(v["mod_details"]["release_year"])
    dummodel['institution_acronym'] =  v["institution_id"]
    # MS 2019-11-21: model colums added and filled with default values
    #dummodel['rights']              =  'Creative Commons Attribution-ShareAlike 4.0 International License (CC BY-SA 4.0)'
    #dummodel['rights_uri']          =  'http://creativecommons.org/licenses/by-sa/4.0/'
    #dummodel['rights_identifier']   =  'CC BY-SA 4.0'
    try:
        dummodel['rights_uri']          =  v['mod_details']['license_info']['url']
        if dummodel['rights_uri'] == 'https://creativecommons.org/licenses/by-sa/4.0/' or dummodel['rights_uri'] == 'http://creativecommons.org/licenses/by-sa/4.0/':
            dummodel['rights']              =  'Creative Commons Attribution-ShareAlike 4.0 International License (CC BY-SA 4.0)'
            dummodel['rights_identifier']   =  'CC BY-SA 4.0'
        elif dummodel['rights_uri'] == 'https://creativecommons.org/licenses/by/4.0/' or dummodel['rights_uri'] == 'http://creativecommons.org/licenses/by/4.0/':
            dummodel['rights']              =  'Creative Commons Attribution 4.0 International License (CC BY 4.0)'
            dummodel['rights_identifier']   =  'CC BY 4.0'
        elif dummodel['rights_uri'] == 'https://creativecommons.org/licenses/by/3.0/' or dummodel['rights_uri'] == 'http://creativecommons.org/licenses/by/3.0/':
            dummodel['rights']              =  'Creative Commons Attribution 3.0 International License (CC BY 3.0)'
            dummodel['rights_identifier']   =  'CC BY 3.0'
        elif dummodel['rights_uri'] == 'https://creativecommons.org/licenses/by-sa/3.0/' or dummodel['rights_uri'] == 'http://creativecommons.org/licenses/by-sa/3.0/':
            dummodel['rights']              =  'Creative Commons Attribution-ShareAlike 3.0 International License (CC BY-SA 3.0)'
            dummodel['rights_identifier']   =  'CC BY-SA 3.0'
        elif dummodel['rights_uri'] == 'https://creativecommons.org/licenses/by-nc-sa/4.0/' or dummodel['rights_uri'] == 'http://creativecommons.org/licenses/by-nc-sa/4.0/':
            dummodel['rights']              =  'Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International License (CC BY-NC-SA 4.0)'
            dummodel['rights_identifier']   =  'CC BY-NC-SA 4.0'
        elif dummodel['rights_uri'] == 'https://creativecommons.org/publicdomain/zero/1.0/' or dummodel['rights_uri'] == 'http://creativecommons.org/publicdomain/zero/1.0/':
            dummodel['rights']              =  'CC0 1.0 Universal (CC0 1.0) Public Domain Dedication'
            dummodel['rights_identifier']   =  'CC0 1.0'
        elif len(dummodel['rights_uri'])==0:
            dummodel['rights']              =  ''
            dummodel['rights_identifier']   =  ''
        else:
            dummodel['rights']              =  'Unknown license'          
    except:
        dummodel['rights_uri']          =  ''
        dummodel['rights']              =  ''
        dummodel['rights_identifier']   =  ''
    try:
        dummodel['model_url']           =  v['mod_details']['license_info']['source_specific_info']
    except:
        dummodel['model_url']           =  ''
    try:
        dummodel['history']             =  v['mod_details']['license_info']['history']
    except:
        dummodel['history']             =  ''
    try:
        dummodel['contact']             =  v['mod_details']['license_info']['exceptions_contact']
    except:
        dummodel['contact']             =  ''

    if dummodel['rights'] ==  'Unknown license':
        #print 'Error',dummodel
        errorHandling(1,'Unknown license: %s --> exit' %  dummodel['rights_uri'],'Unknown license: %s --> exit' %  dummodel['rights_uri'])

    #print k,v["institution"]
    # MS 2019-07-16: New format for institution_id CV 
    if type(v["institution"]) is dict: # new format
        if len(v["institution"]["name"])>0:
            dummodel['institution_name']=v["institution"]["name"]
        else:
            dummodel['institution_name']=v["institution_id"]
        if len(v["institution"]["homepage"])>0:
            dummodel['institution_name']+=', '+v["institution"]["homepage"]
    else:
        dummodel['institution_name']    =  v["institution"]
    dummodel['upd_time']            = 'SYSTIMESTAMP'
    dummodel['combi']               = k

    models.append(dummodel)
    #print dummodel
    #sys.exit()

#print models[0]
#print models[1]
#sys.exit()

# get all available models from DB
# connect to database
print 'Connect to CERA...'
cuser  = 'cmip6_cite_test'
fdb=os.path.abspath(os.path.relpath(mydir+'/../.cmip6_cite_test'+fileflag))
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
    log.error("Cannot connect to DB=\'%s\'. Check password (%i: %s)" % (':'.join(re.split(':',sdbfile)[:2]),error.code,error.message.strip()))
    #raise IOError, "\nCannot connect to DB=\'%s\'. Check password\n" % cuser
    sys.exit()

cur.prepare('select * from models order by model_acronym')
try:
    cur.execute(None, )
except cx_Oracle.DatabaseError as e:
    error, = e.args
    errorHandling(error.code,error.message,'select from database failed (select * from models) -> exit')

res=cur.fetchall()
col_names=getColumns(cur)
cera_list={}
# walk through result list
for d in res:
    line = {}
    for k,v in zip(col_names,d):
        line[k]=v
        if not v :
            #print k,v
            line[k]=''
    line['COMBI']=line['MODEL_ACRONYM']+'$'+line['INSTITUTION_ACRONYM']
    cera_list[line['COMBI']]=line

#print cera_list
#sys.exit()

# insert/update models
num_insert={'modup':0,'modins':0}
for m in models:
    #print m['model_acronym']
    if m['combi'] not in cera_list: # insert new model
        if len(testflag) > 0:
            print 'NEW: %s' % m['combi']
            #print m
            continue

        # INSERT into model
        #print 'NEW2: %s' % m['combi']
        modelid=cur.var(cx_Oracle.NUMBER)
        #MS 2019-11-21: new columns inserted
        #cur.prepare("insert into models(model_id,model_acronym,model_label,model_name,resolutions,components,release_year,institution_acronym,institution_name,upd_time) values (cmip6_cite_test.seq_models.nextVal,:acr,:lab,:name,:res,:comp,:year,:iacr,:iname,SYSTIMESTAMP) returning model_id into :x")
        #cur.prepare("insert into models(model_id,model_acronym,model_label,model_name,resolutions,components,release_year,institution_acronym,institution_name,upd_time,rights,rights_uri,rights_identifier) values (cmip6_cite_test.seq_models.nextVal,:acr,:lab,:name,:res,:comp,:year,:iacr,:iname,SYSTIMESTAMP,:rights,:ruri,:rid) returning model_id into :x")
        cur.prepare("insert into models(model_id,model_acronym,model_label,model_name,resolutions,components,release_year,institution_acronym,institution_name,upd_time,rights,rights_uri,rights_identifier,model_url,history,contact) values (cmip6_cite_test.seq_models.nextVal,:acr,:lab,:name,:res,:comp,:year,:iacr,:iname,SYSTIMESTAMP,:rights,:ruri,:rid,:murl,:hist,:contact) returning model_id into :x")
        try:
            #cur.execute(None,acr=m['model_acronym'],lab=m['model_label'],name=m['model_name'],res=m['resolutions'],comp=m['components'],year=m['release_year'],iacr=m['institution_acronym'],iname=m['institution_name'], x=modelid)
            #cur.execute(None,acr=m['model_acronym'],lab=m['model_label'],name=m['model_name'],res=m['resolutions'],comp=m['components'],year=m['release_year'],iacr=m['institution_acronym'],iname=m['institution_name'],rights=m['rights'],ruri=m['rights_uri'],rid=m['rights_identifier'], x=modelid)
            cur.execute(None,acr=m['model_acronym'],lab=m['model_label'],name=m['model_name'],res=m['resolutions'],comp=m['components'],year=m['release_year'],iacr=m['institution_acronym'],iname=m['institution_name'],rights=m['rights'],ruri=m['rights_uri'],rid=m['rights_identifier'],murl=m['model_url'],hist=m['history'],contact=m['contact'], x=modelid)
            imodelid = getInt(modelid)
            logMessage('INFO','INSERT %s into %s (id=%i; model_label=\'%s\'; model_name=\'%s\'; institution_acronym=\'%s\'; institution_name=\'%s\')' % (m['model_acronym'],'models', imodelid,m['model_label'],m['model_name'],m['institution_acronym'],m['institution_name']),'GENERATEMODEL:%s' % m['model_acronym'],'INSERT %s into %s (id=%i; model_label=%s; model_name=%s; institution_acronym=%s; institution_name=%s)' % (m['model_acronym'],'models', imodelid,m['model_label'],m['model_name'],m['institution_acronym'],m['institution_name']))
            num_insert['modins'] += 1
        except cx_Oracle.DatabaseError as f:
            error, = f.args
            errorHandling(error.code,error.message,'INSERT %s into %s'  % (m['model_acronym'],'models'))

    else: # update existing model if required
        #print '......existing: %s' % m['combi']
        updflag = 0
        for k,v in cera_list[m['combi'].encode('utf-8')].iteritems():
            if k=='MODEL_ID' or k=='UPD_TIME': # or k=='RIGHTS' or k=='RIGHTS_URI' or k=='RIGHTS_IDENTIFIER':
                continue
            try: 
                if m[k] != v:
                    updflag=1
                    break
            except:
                if m[str.lower(k)] != v:
                    updflag=1
                    break
                
        if updflag == 1: # update
            if len(testflag)>0:
                print 'UPD: %s' % (m['combi'])
                #print m
                continue
            #print 'UPDATE2 %s' % (m['combi'])
            #cur.prepare("update models set model_label=:lab,model_name=:name,resolutions=:res,components=:comp,release_year=:year,institution_name=:iname,upd_time=SYSTIMESTAMP where model_acronym=:acr and institution_acronym=:iacr")
            cur.prepare("update models set model_label=:lab,model_name=:name,resolutions=:res,components=:comp,release_year=:year,institution_name=:iname,upd_time=SYSTIMESTAMP,rights=:rights,rights_uri=:ruri,rights_identifier=:rid,model_url=:murl,history=:hist,contact=:contact where model_acronym=:acr and institution_acronym=:iacr")
            try:
                #cur.execute(None,lab=m['model_label'],name=m['model_name'],res=m['resolutions'],comp=m['components'],year=m['release_year'],iname=m['institution_name'],acr=m['model_acronym'],iacr=m['institution_acronym'])
                cur.execute(None,lab=m['model_label'],name=m['model_name'],res=m['resolutions'],comp=m['components'],year=m['release_year'],iname=m['institution_name'],acr=m['model_acronym'],iacr=m['institution_acronym'],rights=m['rights'],ruri=m['rights_uri'],rid=m['rights_identifier'],murl=m['model_url'],hist=m['history'],contact=m['contact'])
                #cur.execute(None,acr=m['model_acronym'],lab=m['model_label'],name=m['model_name'],res=m['resolutions'],comp=m['components'],year=m['release_year'],iacr=m['institution_acronym'],iname=m['institution_name'],rights=m['rights'],ruri=m['rights_uri'],rid=m['rights_identifier'],murl=m['model_url'],hist=m['history'],contact=m['contact'], x=modelid)
                logMessage('INFO','UPDATE %s for %s %s (model_label=\'%s\'; model_name=\'%s\'; institution_name=\'%s\')' % ('models',m['model_acronym'],m['institution_acronym'],m['model_label'],m['model_name'],m['institution_name']),'GENERATEMODEL:%s %s' % (m['model_acronym'],m['institution_acronym']),'UPDATE %s for %s %s (model_label=%s; model_name=%s; institution_name=%s)' % ('models',m['model_acronym'],m['institution_acronym'],m['model_label'],m['model_name'],m['institution_name']))
                num_insert['modup'] += 1
            except cx_Oracle.DatabaseError as f:
                error, = f.args
                errorHandling(error.code,error.message,'INSERT %s into %s'  % (m['model_acronym'],'models'))

# write report in log file and in log_jobs table
if len(testflag) > 0:
    conn.rollback()
else:
    if num_insert['modins'] == 0 and num_insert['modup'] == 0:
        cur.close()
        conn.close()
        sys.exit()

    log.info('Total number of model inserts=%s and updates=%s' % (str(num_insert['modins']),str(num_insert['modup'])))
    # INSERT into log_job (autocommit)
    try:
        sql='insert into log_jobs (ID,NAME,CALL,LOG_TYPE,LOG_MESS,ERR_CODE,ERR_MESS,TIMESTAMP) values (cmip6_cite_test.seq_log_jobs.nextVal,\'GENERATEMODEL\',\'%s\',\'INFO\',\'Total number of model inserts=%s and updates=%s\',0,\'\',SYSTIMESTAMP)' % (' '.join(sys.argv),str(num_insert['modins']),str(num_insert['modup']))
        cur.execute( sql )
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        log.error('%s' % str(error.code))
        log.error('%s' % error.message)
        raise

    ##testing
    ###conn.rollback()
    conn.commit()


cur.close()
conn.close()

