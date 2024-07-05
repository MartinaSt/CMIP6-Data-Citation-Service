#! /usr/bin/env python
"""check data availability in ESGF via Solr request for DOI data collection
Version: V0.1 2019-05-16: first version replacing searchESGF.py (andrej.fast@dkrz.de,stockhause@dkrz.de)"""

import json
import sys
import os
import time
import re
import requests
import urllib2
from urllib import urlencode
#from urllib.parse import urlencode
#from urllib.request import urlopen, Request
import ssl

# Maximum number of records returned by a Solr query
# 10000

# NOTE: PROTOCOL_TLSv1_2 support requires Python 2.7.13+


class NoServerFoundError(Exception):
  pass


class QuerySolr:

  def __init__(self):
    self.ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
    self.MAX_ROWS = 10000  # for Solr query (max)
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
        resp = requests.get(url, params=self.api_params)
        if resp.status_code < 300:
          # MS check for len(content)==0!
          #print url,api_urls,self.api_params
          content = resp.json()

          shards = content['responseHeader']['params']['shards']
          institutes = [i for i in content['facet_counts']
                        ['facet_fields'][facets[0]]][0::2]
          return url, institutes, shards

      except requests.exceptions.ConnectionError:
        pass

    # return errof in case no ESGF server found
    raise NoServerFoundError("querySolr: No search index available: Could not get shards")


  def query_solr(self, solr_url, query, fields, shards):
    # def query_solr(self,query,fields,solr_url='https://esgf-data.dkrz.de/solr',solr_core='datasets'):
    """
    Method to query a Solr catalog for records matching specific constraints.
    query: query constraints, separated by '&'
    fields: list of fields to be returned in matching documents
    returns a list of result documents, each list item is a dictionary of
    the requested fields
    """
    results = {}

    solr_core_url = solr_url+"/"+self.solr_core
    queries = query.split('&')
    start = 0
    numFound = start+1
    next_cursor = '*'

    # 1) query for all matching records
    while start < numFound:
      # build Solr select URL
      # example: http://localhost:8984/solr/datasets/select?q=%2A%3A%2A&fl=id
      #          &fl=version&fl=latest&fl=replica
      #          &fl=master_id&wt=json&indent=true&start=0&rows=5&fq=replica%3Dfalse&fq=latest%3Dtrue
      url = solr_core_url + "/select"
      params = [('q', '*:*'),
                ('wt', 'json'), ('indent', 'true'),
                ('replica', 'false'),
                ('rows', self.MAX_ROWS),
                #('start', start),
                ('start', 0),
                ('sort', 'id asc'),
                ('cursorMark', next_cursor)
                ]
      for fq in queries:
        params.append(('fq', fq))
      for fl in fields:
        params.append(('fl', fl))
      params.append(('shards', shards))

      # execute query to Solr
      url = url + "?"+urlencode(params)
      #print 'Executing Solr search URL=%s' % url
      # sys.exit()
      #fh = urlopen(url, context=ssl_context)
      try:
        fh = urllib2.urlopen(url, context=self.ssl_context)
        response = fh.read().decode("UTF-8")
        jobj = json.loads(response)
      except:
        break

      # summary information
      numFound = jobj['response']['numFound']
      numRecords = len(jobj['response']['docs'])
      next_cursor = jobj['nextCursorMark']
      #params['cursorMark'] = next_cursor
      #params.update(('cursorMark', next_cursor))
      start += numRecords
      #print"\t\tTotal number of records found: %s number of records returned: %s" % ( numFound, numRecords)

      # loop over result documents, add to the list
      for doc in jobj['response']['docs']:
        # results.append(doc)
        if doc['instance_id'] in results and int(results[doc['instance_id']]) > int(doc['version']):
          ##print results[doc['instance_id']]
          continue
        results[doc['instance_id']] = doc['version']

    return results, numFound

  def get_list(self, ceralist, project, inst_key, gran_drs, comp_flag, nodes):
    """check availability in ESGF for a list of DRS entries (ceralist) for project 'project' on granularities (gran_drs list) with facet name for institutions=inst_key for ESGF node list nodes and compare if comp_flag=0 (register) - no comparison for input4MIPs insert case (comp_fal=1)"""

    api_urls = [i + "/search" for i in nodes]
    published = {}  # drs:date
    api_query = {'mip_era': project}
    api_facets = [inst_key]

    # select ESGF node from config list and get list of institutes together with shards from get_shards call
    try:
      (selected_url, institutions, shards) = self.get_shards(
          api_urls, api_query, api_facets)
    # try facet project
    except NoServerFoundError, e:
      try:
        api_query = {'project': project}
        (selected_url, institutions, shards) = self.get_shards(
          api_urls, api_query, api_facets)
      except NoServerFoundError, e:
        return {}, {}, str(e)

    #print 'url: ',selected_url
    #print 'shards: ',shards
    #print 'institutions: ', institutions
    #sys.exit()
    # get list of ESGF-published citation entries from solr request to same node
    solr_url = re.sub('esg-search/search', 'solr', selected_url)
    #print 'After API call:',solr_url,shards
    institutions = sorted(institutions)
    for i in institutions:
      #print i
      published.update(self.check_institutes(
          solr_url, i, shards, inst_key, project, gran_drs,api_query))
      #print published
      #sys.exit()
    # compare result list against database list depending on comp_flag
    (exist_dict, non_exist_dict) = self.compare_results(
        ceralist, published, comp_flag)

    return (exist_dict, non_exist_dict, '')


  def check_institutes(self, solr_urls, inst, shards, inst_key, project, gran_drs,api_query):
    """Get list of insitutes and list of solr shards from ESGF search API on node for project using inst_key as search key
"""

    # key could be mip_era or project 
    #query = ('mip_era:%s&%s' % (project, inst_key)) + ":%s"
    query = ('%s:%s&%s' % (api_query.keys()[0],project, inst_key)) + ":%s"
    fields = ['instance_id', 'version']
    #print 'Before query_solr:',solr_urls, query % inst, fields, shards
    (results, _) = self.query_solr(solr_urls, query % inst, fields, shards)
    returndict = {}
    for instance_id in results:
      version = results[instance_id]
      for g in gran_drs:
        agg = '.'.join(instance_id.split('.')[:g])
        if agg not in returndict.keys():
          returndict[agg] = version
        if int(returndict[agg]) > int(version) or int(returndict[agg]) < 20000000:
          returndict[agg] = version

    return returndict


  def compare_results(self, ceralist, published, comp_flag):
    """compare database list to ESGF-published list in case of comp_flag=0
"""

    existdict = {}
    nonexistdict = {}
    # MS 2019-02-04
    fdate = os.popen('date +%Y%m%d')
    mdate = fdate.read().strip()

    # compare lists in case of DOI registration or update of metadata
    if comp_flag == 0:  # compare for registerDOI

      # existdict: data in citation db is available in ESGF
      for e in sorted(published.iterkeys()):
        if e in ceralist:
          existdict[e] = published[e]

      # nonexistdict: data in citation db is not available in ESGF
      for c in ceralist:
        if c not in published.keys():
          # MS 2019-02-04
          #nonexistdict[c] = os.popen('date +%Y%m%d').read().strip()
          nonexistdict[c] = mdate

    # no comparison to database list in case of input4MIPs entry insert into citation db
    elif comp_flag == 1:  # insert input4MIPs entries into citation DB

      # existdict: data  available in ESGF
      for e in sorted(published.iterkeys()):
        existdict[e] = published[e]

      # nonexistdict: data in citation db is not available in ESGF
      for c in ceralist:
        if c not in published.keys():
          # MS 2019-02-04
          #nonexistdict[c] = os.popen('date +%Y%m%d').read().strip()
          nonexistdict[c] = mdate
    fdate.close()

    return existdict, nonexistdict


if __name__ == '__main__':

  dummyresp=''
  #mylist = ['CMIP6.AerChemMIP.CNRM-CERFACS.CNRM-ESM2-1','CMIP6.AerChemMIP.EC-Earth-Consortium.EC-Earth3-AerChem', 'CMIP6.AerChemMIP.MIROC.MIROC-ES2H',  'CMIP6.AerChemMIP.MIROC.MIROC6']
  mylist = ['CMIP6.CMIP.IPSL.IPSL-CM6A-LR','input4MIPs.CMIP6.DCPP.CNRM-Cerfacs.DCPP-C-amv-1-1', 'input4MIPs.CMIP6.CMIP.UReading.UReading-CCMI-1-0','input4MIPs.CMIP6.CMIP.UoM.UoM-CMIP-1-2-0','input4MIPs.CMIP6.DCPP.CNRM-Cerfacs.DCPP-C-ipv-1-1','input4MIPs.CMIP6.DAMIP.CCCma.CCMI-hist-stratO3-1-0','input4MIPs.CMIP6.ScenarioMIP.UofMD.UofMD-landState-IMAGE-ssp126-2-1-f','input4MIPs.CMIP6.ScenarioMIP.UofMD.UofMD-landState-MAGPIE-ssp585-2-1-f','input4MIPs.CMIP6.ScenarioMIP.UofMD.UofMD-landState-AIM-ssp370-2-1-f','input4MIPs.CMIP6.CMIP.IACETH.IACETH-SAGE3lambda-3-0-0','input4MIPs.CMIP6.CMIP.IACETH.IACETH-SAGE3lambda-2-1-0','input4MIPs.CMIP6.CMIP.NCAR.NCAR-CCMI-2-0','input4MIPs.CMIP6.ScenarioMIP.UofMD.UofMD-landState-GCAM-ssp434-2-1-f','input4MIPs.CMIP6.ScenarioMIP.UofMD.UofMD-landState-GCAM-ssp460-2-1-f','input4MIPs.CMIP6.CMIP.PCMDI.PCMDI-AMIP-1-1-5','input4MIPs.CMIP6.CMIP.PCMDI','CMIP6.AerChemMIP.CNRM-CERFACS.CNRM-ESM2-1','CMIP6.AerChemMIP.CNRM-CERFACS.CNRM-ESM2-1.hist-piAer' ]
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
  gran_drs = [4, 5]
  comp_flag = 0
  project='CMIP6'
  inst_key='institution_id'
  (existdict2, nonexistdict2, dummyresp) = qs.get_list(
      CMIP6_list, project,inst_key, gran_drs, comp_flag, nodes)
  print 'DATA EXIST', existdict2
  print 'NO DATA EXIST', nonexistdict2
  print 'DummyResp', dummyresp
  end = time.time()
  print end-start
