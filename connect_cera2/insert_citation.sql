# inserts of evolving citation into cera2 and cera2_upd
#
# 1. insert of new into cera2_upd:
#    using merge
CERA2_UPD.CITATION:MERGE INTO upd_citation clta USING (select c.*,e.external_pid,d.drs_map_esgf from cmip6_cite_test.citation c,cmip6_cite_test.externalid e,cmip6_cite_test.policy_drs d where e.external_id=c.externalid_id and c.policy_drs_id=d.policy_drs_id and e.EXTERNAL_PID_STATUS='registered' and e.external_pid like '10.22033/ESGF/%' ) ccit ON (clta.access_spec = 'doi:'||ccit.external_pid) WHEN NOT MATCHED THEN INSERT (clta.citation_id,clta.title,clta.authors,clta.publication,clta.publisher,clta.editor,clta.publication_date,clta.country,clta.state,clta.place,clta.edition,clta.access_spec,clta.additional_info,clta.presentation_id,clta.citation_type_id,clta.upd_by,clta.upd_date) VALUES (cera2_temp.seq_citation.nextVal,ccit.title,cmip6_cite_test.F_AUTHORS(ccit.citation_id),'not filled',ccit.publisher,'not filled',to_date(ccit.dateversion,'YYYY-MM-DD'),'not filled','not filled','not filled','Version '||to_char(ccit.modification_date,'YYYYMMDD'),'doi:'||ccit.external_pid,ccit.DRS_MAP_ESGF,2000001,2,'MARTINA_STOCKHAUSE',sysdate)
#
# 2. update evolving citation directly in cera2
#    using merge
CERA2.CITATION:MERGE INTO citation clta USING (select c.*,e.external_pid,d.drs_map_esgf from cmip6_cite_test.citation c,cmip6_cite_test.externalid e,cmip6_cite_test.policy_drs d where e.external_id=c.externalid_id and c.policy_drs_id=d.policy_drs_id and e.EXTERNAL_PID_STATUS='registered' and e.external_pid like '10.22033/ESGF/%' and cmip6_cite_test.F_AUTHORS(c.citation_id) is not NULL ) ccit ON (clta.access_spec = 'doi:'||ccit.external_pid) WHEN MATCHED THEN UPDATE SET clta.authors = cmip6_cite_test.F_AUTHORS(ccit.citation_id),clta.title=ccit.title,clta.edition='Version '||to_char(ccit.modification_date,'YYYYMMDD')

