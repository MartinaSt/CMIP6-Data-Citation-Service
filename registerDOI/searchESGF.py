#! /usr/bin/env python
"""check data availability in ESGF via search API for DOI data collection
Version: V0.1 2018-03-02: first version with devel-operational (stockhause@dkrz.de)
Depricated and Superseded by querySolr.py on 2019-05-16"""

import re,os,sys
import time,json

# set pathes
mydir=os.path.abspath(os.path.dirname(sys.argv[0]))
#print mydir
f=os.path.abspath(os.path.relpath(mydir+'/../config.json'))
# MS 2019-02-04
#config = json.loads(open(f,'r').read())
config = []
with open(f,'r') as f:
  config = json.loads(f.read())
for l in config["addpaths"]:
    sys.path.append(l)
sys.path.append(mydir)

# examples at: http://esgf-pyclient.readthedocs.io/en/latest/examples.html
from pyesgf.search import SearchConnection



class SearchESGF:


    def getList(self,ceralist,project,inst_key,gran_drs,comp_flag,nodes):
        """check availability in ESGF for a list of DRS entries (ceralist) for project 'project' on granularities (gran_drs list) with facet name for institutions=inst_key for ESGF node list nodes and compare if comp_flag=0 (register) - no comparison for input4MIPs insert case (comp_fal=1)"""

        start = time.time()
        published = {}  # drs:date

        # check search API availability and get list of institutes
        mynode = ''
        for n in nodes:
            (conn,institutes) = self.check_access_node(n,project,inst_key)
            if len(institutes) > 0:  # results returned
                mynode = n
                break
        
        # no ESGF node available -> exit
        if len(mynode) == 0:
            #print 'No index node available'
            return {},{},'searchESGF:No index node available'

        mytime = time.time()
        #print mytime-start,'0.00 Take mynode',mynode

        # get list of ESGF-published citation entries 
        for i in sorted(institutes):
            published.update(self.checkInstitute(conn,i,project,gran_drs))
            mylasttime = mytime
            mytime = time.time()
            #print mytime-start,mytime-mylasttime,i

        # compare result list against database list depending on comp_flag
        (exist_dict, non_exist_dict) = self.compareResults(ceralist,published,comp_flag)

        end = time.time()
        #print end-start,end-mytime,'END'
        #print 'Time for %i institutes: %f s' % (len(institutes),end-start)
        #print 'Time for per institute: %f s' % ((end-start)/len(institutes))

        return (exist_dict, non_exist_dict,'')


    def check_access_node(self,node,project,inst_key):
        """Get list of insitutes from ESGF search API on node for project using inst_key as search key"""

        institutes = []

        try:
            # open search connection to index node
            conn = SearchConnection(node, distrib=True)
        except:
            return '',institutes

        # get list of institutes for mip_era or project
        try: 
            # search context: Search facets
            #ctx = conn.new_context(mip_era=project)
            ctx = conn.new_context(mip_era=project, fields=inst_key, limit=0)
            institutes = ctx.facet_counts[inst_key].keys()
            #print institutes
        except Exception as e:
            try: 
                #ctx = conn.new_context(project=project)
                ctx = conn.new_context(project=project, fields=inst_key, limit=0)
                institutes = ctx.facet_counts[inst_key].keys()
                #print institutes
            except Exception as e:
                return '',institutes
        
        return conn,institutes


    def checkInstitute(self,conn,inst,project,gran_drs):
        """get citation granularity ESGF entries for institute"""

        returndict = {}
        #print 'checkInstitute: %s' % inst
            
        # definition
        # new_context(context_class=None, latest=None, facets=None, fields=None, from_timestamp=None, to_timestamp=None, replica=None, shards=None, search_type=None, **constraints)
        # ctx.facet_counts and ctx.hit_count properties

        # try ESGF facets: mip_era and project for project
        # MS 2019-02-07: memory leak reduced
        # TODO: loop with limit - offset
        try:
            #ctx = conn.new_context(mip_era=project, institution_id=inst)
          ctx = conn.new_context(mip_era=project, institution_id=inst, fields='instance_id,version', limit=0)
        except:
            try:
                #ctx = conn.new_context(project=project, institution_id=inst)
              ctx = conn.new_context(project=project, institution_id=inst, fields='instance_id,version', limit=0)
            except:
                pass

        # walk through result dataset list
        #for dataset in ctx.search(batch_size=500):
        for dataset in ctx.search(batch_size=1000):
            instance_id = dataset.json['instance_id']
            version = dataset.json['version']
            # aggregate results on requested citation granularities
            for g in gran_drs:
                agg = '.'.join(instance_id.split('.')[:g])
                if agg not in returndict.keys():
                    returndict[agg] = version
                if int(returndict[agg]) > int(version) or int(returndict[agg]) < 20000000:
                    returndict[agg] = version

        return returndict


    def compareResults(self,ceralist,published,comp_flag):
        """compare database list to ESGF-published list in case of comp_flag=0"""

        existdict    = {}
        nonexistdict = {}
        #MS 2019-02-04
        fdate        = os.popen('date +%Y%m%d')
        mdate        = fdate.read().strip()

        # compare lists in case of DOI registration or update of metadata
        if comp_flag == 0: # compare for registerDOI

            # existdict: data in citation db is available in ESGF  
            for e in sorted(published.iterkeys()):
                if e in ceralist:
                    existdict[e] = published[e]

            # nonexistdict: data in citation db is not available in ESGF  
            for c in ceralist:
                if c not in published.keys():
                    #MS 2019-02-04
                    #nonexistdict[c] = os.popen('date +%Y%m%d').read().strip()
                    nonexistdict[c] = mdate

        # no comparison to database list in case of input4MIPs entry insert into citation db
        elif comp_flag == 1: # insert input4MIPs entries into citation DB

            # existdict: data  available in ESGF  
            for e in sorted(published.iterkeys()):
                existdict[e] = published[e]

            # nonexistdict: data in citation db is not available in ESGF  
            for c in ceralist:
                if c not in published.keys():
                    #MS 2019-02-04
                    #nonexistdict[c] = os.popen('date +%Y%m%d').read().strip()
                    nonexistdict[c] = mdate
        fdate.close()

        return existdict,nonexistdict


                    



if __name__=='__main__':

    # test cases...
    #dummyresp = ''
    nodes = ['https://esgf-data.dkrz.de/esg-search','https://esgf-node.llnl.gov/esg-search','https://esgf-node.ipsl.upmc.fr/esg-search','https://esgf-index1.ceda.ac.uk/esg-search' ]
    mylist = ['CMIP6.CMIP.IPSL.IPSL-CM6A-LR','input4MIPs.CMIP6.DCPP.CNRM-Cerfacs.DCPP-C-amv-1-1', 'input4MIPs.CMIP6.CMIP.UReading.UReading-CCMI-1-0','input4MIPs.CMIP6.CMIP.UoM.UoM-CMIP-1-2-0','input4MIPs.CMIP6.DCPP.CNRM-Cerfacs.DCPP-C-ipv-1-1','input4MIPs.CMIP6.DAMIP.CCCma.CCMI-hist-stratO3-1-0','input4MIPs.CMIP6.ScenarioMIP.UofMD.UofMD-landState-IMAGE-ssp126-2-1-f','input4MIPs.CMIP6.ScenarioMIP.UofMD.UofMD-landState-MAGPIE-ssp585-2-1-f','input4MIPs.CMIP6.ScenarioMIP.UofMD.UofMD-landState-AIM-ssp370-2-1-f','input4MIPs.CMIP6.CMIP.IACETH.IACETH-SAGE3lambda-3-0-0','input4MIPs.CMIP6.CMIP.IACETH.IACETH-SAGE3lambda-2-1-0','input4MIPs.CMIP6.CMIP.NCAR.NCAR-CCMI-2-0','input4MIPs.CMIP6.ScenarioMIP.UofMD.UofMD-landState-GCAM-ssp434-2-1-f','input4MIPs.CMIP6.ScenarioMIP.UofMD.UofMD-landState-GCAM-ssp460-2-1-f','input4MIPs.CMIP6.CMIP.PCMDI.PCMDI-AMIP-1-1-5','input4MIPs.CMIP6.CMIP.PCMDI','CMIP6.AerChemMIP.CNRM-CERFACS.CNRM-ESM2-1','CMIP6.AerChemMIP.CNRM-CERFACS.CNRM-ESM2-1.hist-piAer' ]

    start = time.time()
    search = SearchESGF()

    # input4MIPs example registerDOI
    #project = 'input4MIPs'
    #inst_key = 'institution_id'
    #esgf_api = 'https://pcmdi.llnl.gov/esg-search'
    #gran_drs = [4,5]
    #comp_flag = 0
    #input4MIPs_list = [m for m in mylist if re.search('input4MIPs',m)]
    #print 'ILIST',input4MIPs_list
    #sys.exit()

    #(existdict,nonexistdict,dummyresp) = search.getList(input4MIPs_list,project,inst_key,gran_drs,comp_flag,nodes)
    #print 'DATA EXIST', len(existdict),existdict
    #print 'NO DATA EXIST', len(nonexistdict),nonexistdict
    #sys.exit()

    # CMIP6 example registerDOI
    project = 'CMIP6'
    inst_key = 'institution_id'
    #esgf_api = 'http://esgf-data.dkrz.de/esg-search'
    gran_drs = [4,5]
    comp_flag = 0
    CMIP6_list = [m for m in mylist if re.search('CMIP6',m)]
    print CMIP6_list
    #sys.exit()
    (existdict2,nonexistdict2,dummyresp) = search.getList(CMIP6_list,project,inst_key,gran_drs, comp_flag,nodes)
    print 'DATA EXIST', existdict2
    print 'NO DATA EXIST', nonexistdict2
    end = time.time()
    print end-start

    #dum1=[]
    #dum2=[]
    #for e in sorted(existdict.iterkeys()):
    #    dum1.append('%s:%s' % (e,existdict[e]))
    #for n in sorted(nonexistdict.iterkeys()):
    #    dum2.append('%s:%s' % (n,nonexistdict[n]))

    #open('exist.txt','w').write('\n'.join(dum1))
    #open('nonexist.txt','w').write('\n'.join(dum2))
