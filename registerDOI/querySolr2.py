#! /usr/bin/env python
"""check data availability in ESGF via Solr request for DOI data collection
Version: V0.2 2021-07-01: change Solr request to improve performance (stockhause@dkrz.de)
Version: V0.1 2019-05-16: first version replacing searchESGF.py (andrej.fast@dkrz.de,stockhause@dkrz.de)"""

import json
import sys
import os
import time
import re
import requests
import urllib2
from urllib import urlencode
import ssl

# Maximum number of records returned by a Solr query
# 10000

# NOTE: PROTOCOL_TLSv1_2 support requires Python 2.7.13+


class NoServerFoundError(Exception):
  pass


class QuerySolr:

  def __init__(self):
    self.ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
    self.solr_core = 'datasets'
    self.api_params = {'offset': 0,
                       'limit': 0,
                       #'type': 'Dataset',
                       'format': 'application/solr+json',
                       'fields': 'instance_id',
                       'facets': ''
                       }

  def get_shards(self, api_urls, query, facets):
    """Get list of solr shards to be added to solr request and list of institutes
"""

    self.api_params['facets'] = ','.join(facets)
    # self.api_params.update(query)
    # localhost:8983/solr/datasets,localhost:8982/solr/datasets,localhost:8986/solr/datasets,localhost:8987/solr/datasets,localhost:8988/solr/datasets,localhost:8989/solr/datasets,localhost:8990/solr/datasets,localhost:8993/solr/datasets,localhost:8994/solr/datasets,localhost:8995/solr/datasets,localhost:8997/solr/datasets,localhost:8998
    for url in api_urls:
      try:
        #print url,self.api_params
        resp = requests.get(url, params=self.api_params)
        if resp.status_code < 300:
          # MS check for len(content)==0!
          #print self.api_urls[0],self.api_params
          content = resp.json()

          shards = content['responseHeader']['params']['shards']
          institutes = [i for i in content['facet_counts']
                        ['facet_fields'][facets[0]]][0::2]
          return url, institutes, shards

      except requests.exceptions.ConnectionError:
        pass

    # return errof in case no ESGF server found
    raise NoServerFoundError("querySolr: No search index available: Could not get shards")



  def get_list(self, ceralist, ceracomplete, key, comp_flag, nodes):
    """check availability in ESGF for a list of DRS entries (ceralist) for project 'project' on granularities (gran_drs list) with facet name for institutions=inst_key for ESGF node list nodes and compare if comp_flag=0 (register) - no comparison for input4MIPs insert case (comp_fal=1)"""

    api_urls = [i + "/search" for i in nodes]
    published = {}  # drs:date
    api_query = {'master_id':''}
    api_facets = [key]
    esgflist = []

    # select ESGF node from config list and get list of institutes together with shards from get_shards call
    #print "get_shards1 input ",api_urls, api_query, api_facets
    try:
      (selected_url, institutions, shards) = self.get_shards(api_urls, api_query, api_facets)
      #print "get_shards1 ",selected_url, shards
    # try facet project
    except NoServerFoundError, e:
      try:
        api_query = {}
        (selected_url, institutions, shards) = self.get_shards(api_urls, api_query, api_facets)
        #print "get_shards2 ",selected_url, institutions, shards
      except NoServerFoundError, e:
        return {}, str(e)

    # get list of ESGF-published citation entries from solr request to same node
    solr_url = re.sub('esg-search/search', 'solr', selected_url)
    #print 'After API call:',solr_url,shards,key,api_query
    esgflist=self.check_ceralist(solr_url, shards, ceralist)
    #print "check_ceralist ",esgflist
    #published.update(self.check_ceralist(
    #      solr_url, shards, ceralist, gran_drs,api_query))
    if len(esgflist)==0:
      return {}, ''
    # reduce esgflist by model MIPs with registered DOIs
    for e in esgflist:
      if e in ceracomplete:
        published[e]='30000000'
    # add publication date from ESGF dataset version to esgflist
    published = self.add_versiondate(published,solr_url,shards)
    #print "add_versiondate ",published
    #sys.exit()

    return (published, '')


  def add_versiondate(self,published,solr_urls, shards):
    """Add max ESGF dataset version as publication date to dict"""

    facet = 'instance_id'
    for p in published:
      #print p
      url = solr_urls+"/"+self.solr_core + "/select"
      params = [('q', 'master_id:'+p+'.*'),
                ('wt', 'json'), ('indent', 'true'),
                ('replica', 'false'),
                ('rows', 10000),
                #('rows', 10),
                ('fl',facet)
                ]
      params.append(('shards', shards))
      url = url + "?"+urlencode(params)
      #print url
      try:
        fh = urllib2.urlopen(url, context=self.ssl_context)
        response = fh.read().decode("UTF-8")
        jobj = json.loads(response)
      except:
        continue

      for l in jobj['response']['docs']:
        if int(re.split('\.',l[facet])[-1][1:]) < int(published[p]):
          published[p]=re.split('\.',l[facet])[-1][1:]

    return published


  def check_ceralist(self, solr_urls, shards, ceralist):
    """Get list of insitutes and list of solr shards from ESGF search API on node for project using inst_key as search key
"""
    esgflist=[]
    #print ceralist
    # key could be mip_era or project 
    for c in ceralist:
      url = solr_urls+"/"+self.solr_core + "/select"
      #print 'url,master_id:',url,c
      if re.search('input4MIPs',c):
        subfacet='source_id'
      else:
        subfacet='experiment_id'
      #print 'SUBFACET:',subfacet
      params = [('q', 'master_id:'+c+'.*'),
                ('wt', 'json'), ('indent', 'true'),
                ('replica', 'false'),
                ('rows', 0),
                ('fl',subfacet),
                ('stats','on'),
                ('stats.field',subfacet),
                ('stats.calcdistinct','true')
                ]
      params.append(('shards', shards))
      url = url + "?"+urlencode(params)
      #print url
      try:
        fh = urllib2.urlopen(url, context=self.ssl_context)
        response = fh.read().decode("UTF-8")
        jobj = json.loads(response)
      except:
        continue

      if len(jobj['stats']['stats_fields'][subfacet]['distinctValues'])>0:
        esgflist.append(c)
        for l in jobj['stats']['stats_fields'][subfacet]['distinctValues']:
          esgflist.append(c+'.'+l)

    #print esgflist

    return esgflist



if __name__ == '__main__':

  dummyresp=''
  #mylist = ['CMIP6.AerChemMIP.CNRM-CERFACS.CNRM-ESM2-1','CMIP6.AerChemMIP.EC-Earth-Consortium.EC-Earth3-AerChem', 'CMIP6.AerChemMIP.MIROC.MIROC-ES2H',  'CMIP6.AerChemMIP.MIROC.MIROC6']
  mylist = ['CMIP6.CMIP.IPSL.IPSL-CM6A-LR','input4MIPs.CMIP6.DAMIP.CCCma','input4MIPs.CMIP6.DCPP.CNRM-Cerfacs', 'input4MIPs.CMIP6.CMIP.UReading','input4MIPs.CMIP6.CMIP.UoM','input4MIPs.CMIP6.CMIP.PCMDI','CMIP6.ScenarioMIP.CNRM-CERFACS.CNRM-ESM2-1','CMIP6.CMIP.MPI-M.QUATSCH']
  completelist = ['CMIP6.CMIP.IPSL.IPSL-CM6A-LR','CMIP6.CMIP.IPSL.IPSL-CM6A-LR.historical','CMIP6.CMIP.IPSL.IPSL-CM6A-LR.1pctCO2','CMIP6.CMIP.IPSL.IPSL-CM6A-LR.amip','input4MIPs.CMIP6.DAMIP.CCCma','input4MIPs.CMIP6.DCPP.CNRM-Cerfacs', 'input4MIPs.CMIP6.CMIP.UReading','input4MIPs.CMIP6.CMIP.UoM','input4MIPs.CMIP6.CMIP.PCMDI','CMIP6.ScenarioMIP.CNRM-CERFACS.CNRM-ESM2-1.ssp370','CMIP6.ScenarioMIP.CNRM-CERFACS.CNRM-ESM2-1.ssp245','CMIP6.ScenarioMIP.CNRM-CERFACS.CNRM-ESM2-1.ssp585','CMIP6.ScenarioMIP.CNRM-CERFACS.CNRM-ESM2-1.ssp119','CMIP6.CMIP.MPI-M.QUATSCH']
  # 1. search API query to get all index nodes:
  # https://esgf-data.dkrz.de/esg-search/search?facets=index_node&mip_era=CMIP6&fields=index_node&limit=0
  # 2. parallel requests for all solrs
  start = time.time()
  #api_query = {'mip_era': 'CMIP6'}
  api_facets = ['institution_id']
  #query = 'mip_era:CMIP6&institution_id:%s'
  #fields = ['instance_id', 'version']
  nodes = ['https://esgf-node.llnl.gov/esg-search',
           'https://esgf-data.dkrz.de/esg-search',
           'https://esgf-node.ipsl.upmc.fr/esg-search',
           'https://esgf-index1.ceda.ac.uk/esg-search'
          ]
  CMIP6_list = [m for m in mylist if re.search('CMIP6',m)]
  print CMIP6_list

  qs = QuerySolr()
  #gran_drs = [4, 5]
  comp_flag = 0
  project='CMIP6'
  key='experiment_id'
  (existdict2, dummyresp) = qs.get_list(CMIP6_list,completelist,key, comp_flag, nodes)
  print 'DATA EXIST', existdict2
  #print 'NO DATA EXIST', nonexistdict2
  print 'DummyResp', dummyresp
  end = time.time()
  print end-start
