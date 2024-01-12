# CMIP6 Data Citation Service

Software for CMIP6 data citation service:
* [database schema](db_schema.png)
* **[registerDOI](/registerDOI)**: 
   * registerDOI.py: register DataCite metadata and DOI
   * checkDOI.py: check for DOIs without ESGF datasets
   * create_xml.py: update DataCite metadata and push XML to oai server
* **[insert_cmip6](/insert_cmip6)**: 
   * insertcv_cmip6.py: insert citation entries in db from CMIP6 CV
   * generateModel.py: insert/update model information from CMIP6 CV
* **[curate_cmip6](/curate_cmip6)**: check db content
* **[connect_cera2](/connect_cera2)**:
  * insert_citation.py: merge evolving citation into cera2_upd.upd_citation
* **[insert_scholix](/insert_scholix)**:
  * dli_access_pid.py: insert citation and reference entries from scholix

Further resources:
* CMIP6 Data Citation Service: http://cmip6cite.wdc-climate.de
* CMIP6 Data Citation Search: http://bit.ly/CMIP6_Citation_Search
* Statistics:
  * DOI Registrations: http://bit.ly/CMIP6_DOI_Statistic and https://wcrp-cmip.github.io/CMIP6_CVs/docs/CMIP6_source_id_citation.html
  * DataCite statistics for repository ESGF: https://commons.datacite.org/repositories/8orcv25
  * IPCC WGI AR6 Usage of CMIP6 Data: https://bit.ly/CMIP6_in_IPCC
* CMIP6 Data Citation Service Blog: https://cmip6cite.blogspot.com
* CMIP6:
  * https://wcrp-cmip.org/cmip-phase-6-cmip6/
  * https://pcmdi.llnl.gov/CMIP6/ 

Notes:
* python modules required: cx_oracle, http2
* password files to be placed alongside config.json
* The software is released under the [MIT License](LICENSE.md).