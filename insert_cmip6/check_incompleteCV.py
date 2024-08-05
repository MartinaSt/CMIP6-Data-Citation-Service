#! /usr/bin/env python

""" Check for ESGF data with incomplete CMIP6 registration in the CV for source_id, i.e. missing activity_participation for MIP (https://github.com/WCRP-CMIP/CMIP6_CVs)
Version: 
         V0.3 2024-07-04: ESGF Search API request changed from DKRZ to LLNL node after DKRZ's transition to ESGF2/metagrid, stockhause@dkrz.de
         V0.2 2020-05-08: revised ESGF Search API request based on recommendations from SA, stockhause@dkrz.de
         V0.1 2020-04-21, stockhause@dkrz.de"""

# Usage: ./check_incompleteCV.py
# c. compare institute, model, MIP combination against CV for source_id
#    (ignore shared experiments:
#    (v1.mip='DCPP' and v2.mip='VolMIP') then 'dcppC-forecast-addPinatubo'
#    (v1.mip='LS3MIP' and v2.mip='LUMIP') then 'land-hist'
#    (v1.mip='RFMIP' and v2.mip='AerChemMIP') then 'piClim-aer'||', '||'piClim-control'
#    (v1.mip='AerChemMIP' and v2.mip='ScenarioMIP') then 'ssp370'
#    (v1.mip='C4MIP' and v2.mip='CDRMIP') then 'esm-1pct-brch-1000PgC'||', '||'esm-1pct-brch-2000PgC'||', '||'esm-1pct-brch-750PgC'||', '||'esm-1pctCO2'||', '||'esm-bell-1000PgC'||', '||'esm-bell-2000PgC'||', '||'esm-bell-750PgC'
#    )



import sys,os,re,urllib2
import logging
import json
from operator import itemgetter

# set environment
mydir=os.path.abspath(os.path.dirname(sys.argv[0]))
#print mydir
f=os.path.abspath(os.path.relpath(mydir+'/../config.json'))
config = json.loads(open(f,'r').read())
for l in config["addpaths"]:
    sys.path.append(l)
sys.path.append(mydir) 

# read options and analyze testflag
mydate = os.popen('date +%F').read().strip()

# configure logfile and set log file name
LOG_FILENAME = config["logdir"]+"/check_incompleteCV.log"
log = logging.getLogger()
console=logging.FileHandler(LOG_FILENAME)
formatter=logging.Formatter('%(asctime)s - %(levelname)s: %(message)s')
simple_formatter=logging.Formatter('%(message)s')
console.setFormatter(formatter)
log.setLevel(logging.INFO)
log.addHandler(console)

# 0. read CMIP6_CVs source_id

# update CMIP6_CVs content from git
try:
    dummy = os.popen('cd '+mydir+'/CMIP6_CVs;git pull https://github.com/WCRP-CMIP/CMIP6_CVs; cd '+mydir).read()
except:
    log.error('git pull for update of CVs failed -> exit')
    sys.exit()

# walk through tables in 'CMIP6_CVs'
os.chdir(mydir+'/CMIP6_CVs')
for root, dirs, files in os.walk(".", topdown=False):
    for f in files:
        #print('files ',os.path.join(root, f))
        if not re.search('source_id.json',f):
            continue
        #print 'read ',f
        try:
            cv_models = json.loads(open(f,'r').read())
        except:
            log.error( 'Error in reading CV %s' % f)
            sys.exit()

# 1. Access ESGF Search API
#
# a. list all models
# MS (2024-07-04))
#esgf_api_models = 'https://esgf-data.dkrz.de/esg-search/search/?offset=0&limit=0&mip_era=CMIP6&activity_id!=input4MIPs&facets=source_id%2Cinstitution_id&fields=source_id&format=application%2Fsolr%2Bjson'
#esgf_api_models = 'https://esgf-data.dkrz.de/esg-search/search/?offset=0&limit=0&project=CMIP6&facets=source_id%2Cinstitution_id&fields=source_id&retracted=false&format=application%2Fsolr%2Bjson'
esgf_api_models = 'https://esgf-node.llnl.gov/esg-search/search/?offset=0&limit=0&project=CMIP6&facets=source_id%2Cinstitution_id&fields=source_id&retracted=false&format=application%2Fsolr%2Bjson'
try:
    response = urllib2.urlopen(esgf_api_models)
    esgf_models = json.loads(response.read())
except:
    log.error('Error in reading ESGF Search API response %s' % esgf_api_models)
    sys.exit()
#
# b. list MIPs for every model
#
for m in esgf_models['facet_counts']['facet_fields']['source_id'][0::2]:
    i_found = []
    i_nfound = []
    a_found = []
    a_nfound = []
    #MS (2024-07-04)
    #esgf_api_mips = 'https://esgf-data.dkrz.de/esg-search/search/?offset=0&limit=0&mip_era=CMIP6&activity_id!=input4MIPs&source_id='+m+'&facets=source_id%2Cinstitution_id%2Cactivity_id&fields=activity_id&format=application%2Fsolr%2Bjson'
    #esgf_api_mips = 'https://esgf-data.dkrz.de/esg-search/search/?offset=0&limit=0&project=CMIP6&source_id='+m+'&facets=source_id%2Cinstitution_id%2Cactivity_drs&fields=activity_drs&retracted=false&format=application%2Fsolr%2Bjson'
    esgf_api_mips = 'https://esgf-node.llnl.gov/esg-search/search/?offset=0&limit=0&project=CMIP6&source_id='+m+'&facets=source_id%2Cinstitution_id%2Cactivity_drs&fields=activity_drs&retracted=false&format=application%2Fsolr%2Bjson'
    try:
        response = urllib2.urlopen(esgf_api_mips)
        esgf_mips = json.loads(response.read())
    except:
        log.error('Error in reading ESGF Search API response %s' % esgf_api_mips)
        sys.exit()
    #print 'source_id',esgf_mips['facet_counts']['facet_fields']['source_id'][0]
    #print 'institution_id',esgf_mips['facet_counts']['facet_fields']['institution_id'][0::2]
    #print 'activity_ids',esgf_mips['facet_counts']['facet_fields']['activity_id'][0::2]
    #print cv_models["source_id"][m]

    if m not in cv_models["source_id"]:
        log.error('%s: source_id not registered in CV' % (m))
        continue

    for i in esgf_mips['facet_counts']['facet_fields']['institution_id'][0::2]:
        if i in cv_models["source_id"][m]['institution_id']:
            #print 'institute found',i
            i_found.append(i)
        else:
            #print 'institute not found',i
            i_nfound.append(i)

    # MS (2020-05-08)
    #for a in esgf_mips['facet_counts']['facet_fields']['activity_id'][0::2]:
    for a in esgf_mips['facet_counts']['facet_fields']['activity_drs'][0::2]:
        if a in cv_models["source_id"][m]['activity_participation']:
            #print 'MIP found',a
            a_found.append(a)
        else:
            #print 'MIP not found',a
            a_nfound.append(a)
    #print found
    #print nfound
    #sys.exit()
    if len(i_nfound)>0:
        log.error('%s: some institutes not found: %s (found: %s)' % (m,','.join(i_nfound),','.join(i_found)))
    else:
        pass
        #log.info('%s: all institutes found: %s' % (m,','.join(i_found)))
    if len(a_nfound)>0:
        log.error('%s: some activities not found: %s (found: %s) [institution_id: %s]' % (m,','.join(a_nfound),','.join(a_found),','.join(i_found)))
    else:
        pass
        #log.info('%s: all activities found: %s' % (m,','.join(a_found)))

