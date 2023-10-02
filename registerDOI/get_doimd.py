#!/usr/bin/env python
"""create DataCite metadata XML: mapping citation db information to DataCite
using mako templates (http://www.makotemplates.org/)
Versions: 
V0.9 2020-04-16: tcera1 -> testdb
V0.8 2019-12-03: DB hardware/software exchange (stockhause@dkrz.de)
V0.7 2019-08-14: changes for DC 4.3 (stockhause@dkrz.de)
V0.6 2019-04-01: changes for DC 4.2 (stockhause@dkrz.de)
V0.5 2018-03-02: first version with devel-operational (stockhause@dkrz.de)"""

import sys,os,os.path,re,getopt
import urllib2,ssl
import decimal
import json
from copy import deepcopy
from subprocess import Popen, PIPE
from string import Template
import dataciteentry,datacite_writer


try:
    import cx_Oracle
except:
    print "Cannot import module cx_Oracle"
    sys.exit()

try:
    from lxml import etree as ET
    from lxml import objectify
except ImportError:
    print "Failed to import lxml"



class GetDoi:

    
    def __init__(self,dsg_name,outdir,mydir,version,db):
        """init: drs name of citation entry, output dir for xml, used database (pcera or testdb) and metadata version"""

        # set environment for utf-8
        #os.environ['NLS_LANG']="German_Germany.AL32UTF8"
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

        # configure database and testflag
        self.mydir     = mydir
        self.db        = db
        self.fileflag  = ''
        # MS 2019-12-03: testdb -> tcera1
        # MS 2020-04-16: tcera1 -> testdb
        #if self.db == 'tcera1':
        if self.db == 'testdb':
            self.fileflag = 'test'

        # configure DataCite schema version
        self.schemeversion = 4 # 3
        self.template  = 'datacite4_3_temp_mako.xml' # 4_3
        #self.template  = 'datacite4_2_temp_mako.xml' # 4_2
        #self.template  = 'datacite4_1_temp_mako.xml' # 4_1
        #self.template  = 'datacite4_0_temp_mako.xml' # 4_0
        self.dc_xml    = outdir+'/Datacite4_'+re.sub('\.','_',dsg_name)+'.xml' # 3
        self.dc_ns      = { "xmlns":"http://datacite.org/schema/kernel-4",
                            "xmlns:xsi":"http://www.w3.org/2001/XMLSchema-instance",
                            "xsi:schemaLocation":"http://datacite.org/schema/kernel-4 http://schema.datacite.org/meta/kernel-4.3/metadata.xsd" } # 4_3
        #self.dc_ns      = { "xmlns":"http://datacite.org/schema/kernel-4",
        #                    "xmlns:xsi":"http://www.w3.org/2001/XMLSchema-instance",
        #                    "xsi:schemaLocation":"http://datacite.org/schema/kernel-4 http://schema.datacite.org/meta/kernel-4.2/metadata.xsd" } # 4_2
        #self.dc_ns      = { "xmlns":"http://datacite.org/schema/kernel-4",
        #                    "xmlns:xsi":"http://www.w3.org/2001/XMLSchema-instance",
        #                    "xsi:schemaLocation":"http://datacite.org/schema/kernel-4 http://schema.datacite.org/meta/kernel-4.1/metadata.xsd" } # 4_1
        #self.dc_ns      = { "xmlns":"http://datacite.org/schema/kernel-4",
        #                    "xmlns:xsi":"http://www.w3.org/2001/XMLSchema-instance",
        #                    "xsi:schemaLocation":"http://datacite.org/schema/kernel-4 http://schema.datacite.org/meta/kernel-4/metadata.xsd" } # 4_0
        #self.dc_ns      = { "xmlns":"http://datacite.org/schema/kernel-3",
        #                    "xmlns:xsi":"http://www.w3.org/2001/XMLSchema-instance",
        #                    "xsi:schemaLocation":"http://datacite.org/schema/kernel-3 http://schema.datacite.org/meta/kernel-3/metadata.xsd" } 


        #self.version   = 'Version 0.1: creation based on DataCite XML generation script version 0.7 (2015-07-21) by M.Stockhause'
        #self.version   = 'Version 0.2: views used and content extended (2016-06-17) by M.Stockhause'
        #self.version   = 'Version 0.3: DataCite 4.0 (2016-06-21) by M.Stockhause'
        #self.version   = 'Version 0.4: DataCite 4.1 (2017-10-24) by M.Stockhause'
        #self.version   = 'Version 0.5: testdb included (2018-03-02) by M.Stockhause'
        #self.version   = 'Version 0.6: changes for DC 4.2 (2019-04-01) by M.Stockhause'
        #self.version   = 'Version 0.7: changes for DC 4.3 (2019-08-14) by M.Stockhause'
        #self.version   = 'Version 0.8: DB hardware/software exchange (2019-12-03) by M.Stockhause'
        self.version   = 'Version 0.9: Test DB changed from tcera1 back to testdb ( 2020-04-16) by M.Stockhause'
        self.list_ri   = []
        self.dversion   = version #'1'
        self.dsg_name    = dsg_name
        self.outdir    = outdir
        self.doi       = ''
        self.doitype   = ''
        self.sqls      = {}
        self.cera      = {}
        self.datacite  = {}
        self.relations = []
        self.creators  = []
        self.contributors = []
        self.dates     = []
        self.descriptions = []
        self.titles    = []
        self.subjects  = []
        self.sizes     = []
        self.formats   = []
        self.alternateIdentifiers   = []
        self.geoLocations   = []
        self.rights    = []
        self.funders   = []
        # MS 2019-02-04
        fdate = os.popen('date +%Y-%m-%d')
        print 'get_doimd.py --id=\'%s\' (%s) on %s' % (self.dsg_name,self.version,fdate.read().strip())
        fdate.close()
        #print 'get_doimd.py --id=\'%s\' (%s) on %s' % (self.dsg_name,self.version,os.popen('date +%Y-%m-%d').read().strip())
        print 'Use template: %s' % self.template
        print 'Outdir: %s' % self.outdir

             
        # access mako template
        templ = dict(dsg_name=self.dsg_name)

        # read select statements from file
        # MS 2019-02-04
        fsql = open(self.mydir+'/get_doimd_sql.conf','r')
        #for l in open(self.mydir+'/get_doimd_sql.conf','r').readlines():
        for l in fsql.readlines():
            if re.match(r'^(\#)',l.strip()) or len(l.strip()) == 0:
                continue            
            key   = re.split(':',l.strip())[0]
            value = ':'.join(re.split(':',l.strip())[1:]).strip()
            self.sqls[key]=Template(value).safe_substitute(templ)
        print 'Use sql selects specified in get_doimd_sql.conf'
        fsql.close()
        
        
        # connect to db
        print 'get_doi:Connect to CERA... %s' % self.db
        cuser  = "cmip6_cite_test"
        # MS 2019-02-04
        #fdb=os.path.abspath(os.path.relpath(self.mydir+'/../.cmip6_cite_test'+self.fileflag))
        #cpw    = open(fdb,'r').read().strip()
        fdb = open(os.path.abspath(os.path.relpath(self.mydir+'/../.cmip6_cite_test'+self.fileflag)),'r')
        cpw = fdb.read().strip()
        fdb.close()
        try:
            # MS 2019-12-03: oda-scan.dkrz.de -> delphi7-scan.dkrz.de
            #sdbfile2 =  cuser+'/'+cpw+'@'+'( DESCRIPTION = ( ADDRESS_LIST = ( ADDRESS = ( PROTOCOL = TCP ) ( HOST = oda-scan.dkrz.de ) ( PORT = 1521 ) ) ) ( CONNECT_DATA = ( SERVER = DEDICATED ) ( SERVICE_NAME = '+self.db+' ) ))'
            sdbfile2 =  cuser+'/'+cpw+'@'+'( DESCRIPTION = ( ADDRESS_LIST = ( ADDRESS = ( PROTOCOL = TCP ) ( HOST = delphi7-scan.dkrz.de ) ( PORT = 1521 ) ) ) ( CONNECT_DATA = ( SERVER = DEDICATED ) ( SERVICE_NAME = '+self.db+' ) ))'
        except:
            raise IOError, "\nCannot connect to DB=\'%s\'. Check password in file .meta_select\n" % cuser

        try:
            self.iconn = cx_Oracle.connect(sdbfile2)
            self.cur = self.iconn.cursor()
        except IOError,ex:
            print "\nQC DB not found: %s :\n%s" % (':'.join(re.split(':',sdbfile2)[:2]),ex)



    def getDoi(self):
        """getDoi: create datacite XML by mapping information form citation db"""

        # read information from db and store in self.cera
        self.readConfSql()
        # check accessibility of url
        url = self.checkCompact()
        # map db information in self.cera to mako template
        (e,message) = self.map()
        if e != 0:
            return (e,message,url)
        
        # write XML
        self.writeXML()

        # validate XML against schema
        (e,message) = self.validateXML()

        # finish metadata creation
        self.finish()
        return (e,message,url)

    
                             
    def checkCompact(self):
        """checkCompact: check accessibility of landing page URL"""

        self.compact=self.cera['DATA_PID'][0]['EXTERNAL_PID_URL']
        p = Popen(['wget',self.compact], stdout=PIPE, stderr=PIPE)
        for ll in p.stderr.readlines():
            if re.search(r'is not a valid CERA entry',ll.strip()) or re.search(r'Error|ERROR|error',ll.strip()):
                print 'Cannot access compact page %s -> EXIT' % self.compact
        for ll in p.stdout.readlines():
            if re.search(r'is not a valid CERA entry',ll.strip()) or re.search(r'Error|ERROR|error',ll.strip()):
                print 'Cannot access compact page %s -> EXIT' % self.compact
        # MS 2019-02-04: closing of pipes required?
        print 'Compact page found: %s' % self.compact
        print 'PID:%s' % self.cera['DATA_PID'][0]['PID']
        return self.cera['DATA_PID'][0]['EXTERNAL_PID_URL']

        
    def readConfSql(self):
        """readConfSql: read information from db"""

        for k,v in self.sqls.iteritems():
            dumlist = []
            try:
                self.cur.execute(v)
            except cx_Oracle.DatabaseError as e:
                error, = e.args
                self.callError(v,error.message)

            data = self.cur.fetchall()

            # error handling
            if not data:
                if k in ('DATA_PID','CITATION_PART','CONTACT'):
                    self.callError(v,"No data entries found for "+k+" and id=\'"+self.dsg_name+"\'")
                else:
                    print 'WARNING: No datasets found for %s:\n%s' % (k,v)

            # get column names for cursor results
            col_names=self.getColumns(self.cur)

            # walk through result list
            for d in data:
                line = {}
                for k2,v2 in zip(col_names,d):
                    if str(v2) == 'None':
                        v2=''
                    line[str.upper(k2)]=str(v2)
                    if line[str.upper(k2)] == ',':
                        line[str.upper(k2)]=None
                    try:
                        if k2=='PID_TYPE' or k2=='PIDACRO':# or k2=='IIDACRO':
                            line[str.upper(k2)]=str.upper(v2)
                    except:
                        pass

                dumlist.append(line)
            
            # write citation db values on dict cera
            self.cera[k]=dumlist
        


    def validateXML (self):
        """validateXML: validate created XML against DataCite schema"""

        schema = self.dc_ns['xsi:schemaLocation']
        try:
            dum = re.split(' ',schema)
            for d in dum:
                if re.search(r'xsd$',d):
                    schema=d
        except:
            pass

        try:
            # MS 2019-02-04
            #xsd_doc  = ET.parse(urllib2.urlopen(schema))
            fxsd = urllib2.urlopen(schema)
            xsd_doc  = ET.parse(fxsd)
        except:
            try:
                ctx = ssl.create_default_context()
                ctx.check_hostname = False
                ctx.verify_mode = ssl.CERT_NONE
                print 'WARNING: SSL certificate insecure -> ignore'
                # MS 2019-02-04
                #xsd_doc  = ET.parse(urllib2.urlopen(schema,context=ctx))
                fxsd = urllib2.urlopen(schema,context=ctx)
                xsd_doc  = ET.parse(fxsd)
            except Exception as e:
                return (2,'No validation because DataCite schema not accessible %s:\n      %s' % (schema,e) )
        # MS 2019-02-04
        fxsd.close()

        try:
            xsd = ET.XMLSchema(xsd_doc)
            # MS 2019-02-04
            #xml = ET.parse(open(self.dc_xml,'r'),ET.XMLParser(encoding='UTF-8'))
            fxml = open(self.dc_xml,'r')
            xml = ET.parse(fxml,ET.XMLParser(encoding='UTF-8'))
            fxml.close()
        except Exception as e:
            return (2,'No validation because DataCite schema not accessible %s:\n      %s' % (schema,e) )
        try:
            xsd.assertValid(xml)
            return (0,'XML validated against schema: %s' % schema  )
        except Exception as e:
            return (1,'XML validation against schema failed: %s:      %s' % (schema,e) )

            
    
    def map (self):
        """map: map db data to datacite"""
 
        # skip if pid_status is not 'registered'
        if str.upper(self.cera['DATA_PID'][0]['PID_STATUS']) in ('INITIAL','NOT FILLED','N/A'):
            return (1,'No data available yet -> EXIT')

        print 'Map CMIP6-CITE to DataCite...'

        # mapping of mandatory content
        # DOI
        if re.search('HTTP',str.upper(self.cera['DATA_PID'][0]['PID'])):
            self.datacite['identifier']=str(self.cera['DATA_PID'][0]['PID'])
        else:
            self.datacite['identifier']=str.upper(self.cera['DATA_PID'][0]['PID'])

        self.datacite['identifier_type']=str.upper(self.cera['DATA_PID'][0]['PID_TYPE'])

        # Creators
        for l in self.cera['CONTACT']:
            # map cmip6_cite_test CREATOR_TYPE to DataCite nameType; default 'Personal'
            if l['CREATOR_TYPE'] == 'institute':
                ltype = 'Organizational'
            else:
                ltype = 'Personal'

            if l['CONTACT_TYPE_NAME'] == 'Creator' and l['CREATOR_TYPE'] == 'person':
                #self.creators.append(dataciteentry.Creator(l['PERSON'].decode('utf-8'),l['INSTITUTE'].decode('utf-8'),l['PIDSCHEME'],l['PIDACRO'],l['PID'],l['GIVENNAME'].decode('utf-8'),l['FAMILYNAME'].decode('utf-8'),ltype))
                self.creators.append(dataciteentry.Creator(l['PERSON'].decode('utf-8'),l['INSTITUTE'].decode('utf-8'),l['PIDSCHEME'],l['PIDACRO'],l['PID'],l['GIVENNAME'].decode('utf-8'),l['FAMILYNAME'].decode('utf-8'),ltype,l['PIID'],l['PIIDACRO'],l['PIIDSCHEME']))
            elif l['CONTACT_TYPE_NAME'] == 'Creator' and l['CREATOR_TYPE'] == 'institute':
                #self.creators.append(dataciteentry.Creator(l['INSTITUTE'].decode('utf-8'),'',l['IIDSCHEME'],l['IIDACRO'],l['IID'],'','',ltype))
                self.creators.append(dataciteentry.Creator(l['INSTITUTE'].decode('utf-8'),'',l['IIDSCHEME'],l['IIDACRO'],l['IID'],'','',ltype,'','',''))
            elif l['CONTACT_TYPE_NAME'] == 'Funder' and l['CREATOR_TYPE'] == 'person': # datacite 4.0
                if self.schemeversion < 4:
                    #self.contributors.append(dataciteentry.Contributor(l['PERSON'].decode('utf-8'),l['CONTACT_TYPE_NAME'],l['INSTITUTE'].decode('utf-8'),l['PIDSCHEME'],l['PIDACRO'],l['PID'],l['GIVENNAME'].decode('utf-8'),l['FAMILYNAME'].decode('utf-8'),ltype))
                    self.contributors.append(dataciteentry.Contributor(l['PERSON'].decode('utf-8'),l['CONTACT_TYPE_NAME'],l['INSTITUTE'].decode('utf-8'),l['PIDSCHEME'],l['PIDACRO'],l['PID'],l['GIVENNAME'].decode('utf-8'),l['FAMILYNAME'].decode('utf-8'),ltype,'','',''))
                # Funder treatment in 4.0
                elif self.schemeversion >= 4 and l['CONTACT_TYPE_NAME'] == 'Funder':
                    #self.funders.append(dataciteentry.Funder(l['PERSON'].decode('utf-8'),l['PIDSCHEME']+l['PID'],l['PIDACRO'],'','','',ltype))
                    self.funders.append(dataciteentry.Funder(l['PERSON'].decode('utf-8'),l['PIDSCHEME']+l['PID'],l['PIDACRO'],'','','',ltype,l['PIDSCHEME']))
                
            elif l['CONTACT_TYPE_NAME'] != 'Creator' and  l['CONTACT_TYPE_NAME'] != 'Funder' and l['CREATOR_TYPE'] == 'person':
                #self.contributors.append(dataciteentry.Contributor(l['PERSON'].decode('utf-8'),l['CONTACT_TYPE_NAME'],l['INSTITUTE'].decode('utf-8'),l['PIDSCHEME'],l['PIDACRO'],l['PID'],l['GIVENNAME'].decode('utf-8'),l['FAMILYNAME'].decode('utf-8'),ltype))
                self.contributors.append(dataciteentry.Contributor(l['PERSON'].decode('utf-8'),l['CONTACT_TYPE_NAME'],l['INSTITUTE'].decode('utf-8'),l['PIDSCHEME'],l['PIDACRO'],l['PID'],l['GIVENNAME'].decode('utf-8'),l['FAMILYNAME'].decode('utf-8'),ltype,l['PIID'],l['PIIDACRO'],l['PIIDSCHEME']))
            elif l['CONTACT_TYPE_NAME'] != 'Creator' and l['CREATOR_TYPE'] == 'institute':
                if self.schemeversion < 4:
                    #self.contributors.append(dataciteentry.Contributor(l['INSTITUTE'].decode('utf-8'),l['CONTACT_TYPE_NAME'],'',l['IIDSCHEME'],l['IIDACRO'],l['IID'],'','',ltype))
                    self.contributors.append(dataciteentry.Contributor(l['INSTITUTE'].decode('utf-8'),l['CONTACT_TYPE_NAME'],'',l['IIDSCHEME'],l['IIDACRO'],l['IID'],'','',ltype,'','',''))
                # Funder treatment in 4.0
                elif self.schemeversion >= 4 and l['CONTACT_TYPE_NAME'] == 'Funder':
                    if l['IIDNAME'] == 'Open Funder Registry':
                        #self.funders.append(dataciteentry.Funder(l['INSTITUTE'].decode('utf-8'),l['IIDSCHEME']+l['IID'],l['IIDACRO'],'','','',ltype))
                        self.funders.append(dataciteentry.Funder(l['INSTITUTE'].decode('utf-8'),l['IIDSCHEME']+l['IID'],l['IIDACRO'],'','','',ltype,l['IIDSCHEME']))
                    else:
                        #self.funders.append(dataciteentry.Funder(l['INSTITUTE'].decode('utf-8'),l['IIDSCHEME']+l['IID'],l['IIDACRO'],'','','',ltype))
                        self.funders.append(dataciteentry.Funder(l['INSTITUTE'].decode('utf-8'),l['IIDSCHEME']+l['IID'],l['IIDACRO'],'','','',ltype,l['IIDSCHEME']))
                elif self.schemeversion >= 4 and l['CONTACT_TYPE_NAME'] != 'Funder':
                    #self.contributors.append(dataciteentry.Contributor(l['INSTITUTE'].decode('utf-8'),l['CONTACT_TYPE_NAME'],'',l['IIDSCHEME'],l['IIDACRO'],l['IID'],'','',ltype))
                    self.contributors.append(dataciteentry.Contributor(l['INSTITUTE'].decode('utf-8'),l['CONTACT_TYPE_NAME'],'',l['IIDSCHEME'],l['IIDACRO'],l['IID'],'','',ltype,'','',''))
            
        self.datacite['publisher'] = self.cera['CITATION_PART'][0]['PUBLISHER'].decode('utf-8')
        self.datacite['publicationYear'] = self.cera['CITATION_PART'][0]['PUBLICATION_YEAR']
        self.titles.append(dataciteentry.Title(self.cera['CITATION_PART'][0]['TITLE'].decode('utf-8'),''))

        # optional metadata content
        self.dates.append(dataciteentry.Date(self.cera['CITATION_PART'][0]['DATEVERSION'],'Created'))
        self.descriptions.append(dataciteentry.Description(self.cera['ABSTRACT'][0]['ABSTRACT'].decode('utf-8'),'Abstract'))
        self.formats.append(dataciteentry.Format('application/x-netcdf'))
        if len(self.cera['RIGHT'])>0 and self.cera['RIGHT'][0]['RIGHTS'] != '':
            self.rights.append(dataciteentry.Rights(self.cera['RIGHT'][0]['RIGHTS'],self.cera['RIGHT'][0]['RIGHTSURI'],self.cera['RIGHT'][0]['RIGHTSIDENTIFIER']))

        for l in self.cera['SUBJECT']:
            self.subjects.append(dataciteentry.Subject(l['SUBJECT'].decode('utf-8'),l['SUBJECT_SCHEME'],l['SCHEME_URI'],''))

        for l in self.cera['RELATION']:
            self.relations.append(dataciteentry.Relation(l['RELATED_IDENTIFIER'],l['RELATION_TYPE'],l['RELATED_IDENTIFIER_TYPE']))

        return (0,'map(): ok')


    def writeXML(self):
        """writeXML: write mapped citation information into mako template"""

        dc = dataciteentry.DCEntry(self.sizes,self.titles,self.datacite['identifier'],self.datacite['identifier_type'] ,self.descriptions,self.dates,self.datacite['publisher'],self.datacite['publicationYear'],self.formats,self.alternateIdentifiers,self.creators,self.contributors,self.relations,self.subjects,self.dversion,self.geoLocations,self.rights,self.funders)

        dc_out = datacite_writer.write(dc,self.template,self.dc_xml)

        print 'Please check XML \'%s\'' % (self.dc_xml)
        
        

    #def writeJSON(self):
    #    """writeJSON: convert XML to JSON representation (not called)"""
    #
    #    tree = objectify.parse(open(self.dc_xml,'r'),ET.XMLParser(encoding='UTF-8'))
    #
    #    d=self.etree_to_dict(tree.getroot())
    #    dc_json2    = self.outdir+'/Datacite3_'+re.sub('\.','_',self.dsg_name)+'.json'
    #    open(dc_json2,'w').write(json.dumps(d,indent=4)) # encoding="utf-8" is default
        

    #def etree_to_dict(self,t):
    #    """etree_to_dict: convert xml etree to json hierarchy (not called)"""
    #
    #    d = {t.tag : map(self.etree_to_dict, t.iterchildren())}
    #    d.update(('@' + k, v) for k, v in t.attrib.iteritems())
    #    d['text'] = t.text.strip()
    #    return d

        
    def finish(self):
        """finish: finish metadata creation and close db connection"""

        self.cur.close()
        self.iconn.close()
        os.popen('rm '+self.dsg_name)


    
    #def getCeraValues(self,line):
    #    """getCeraValues (not called)"""
    #    values = [] # datacite:cera
    #    types  = [] # datacite:cera
    #    (sql_name,column)=re.split('\.',line['cera_name'])
    #    if sql_name not in self.cera:
    #        return values,types
    #
    #    # two types for one entry and taking values from CERA
    #    if line['datacite_name']=='relatedIdentifier':
    #        dumdt = re.split(',',line['datacite_type'])
    #        dumct = re.split(',',line['cera_type'])
    #        if not re.search('\*$',dumdt[0]) or not re.search('\*$',dumdt[1]):
    #            print 'ERROR: Mapping of relatedIdentifiers wrong -> EXIT'
    #            sys.exit()
    #        if not re.search('\*$',dumct[0]) or not re.search('\*$',dumct[1]):
    #            print 'ERROR: Mapping of relatedIdentifiers wrong -> EXIT'
    #            sys.exit()
    #
    #        dt1=re.split('\.\*',dumdt[0])[0] # relationType
    #        dt2=re.split('\.\*',dumdt[1])[0] # identifierType
    #        ct1=re.split('\.\*',dumct[0])[0] # ref_type_acronym
    #        ct2=re.split('\.\*',dumct[1])[0] # ref_id_type
    #
    #        for l in self.cera[sql_name]:
    #            if l[ct1] not in self.list_ri:
    #                continue
    #
    #            dumval={}
    #            dumtype1={dt1:l[ct1]}
    #            dumtype2={dt2:l[ct2]}
    #            types.append(dt1+':'+l[ct1]+','+dt2+':'+str.upper(l[ct2]))
    #            dumval[line['datacite_name']]=l[column]
    #            values.append(dumval)
    #            #print values,types
    #            
    #            
    #    else:  # all but relatedIdentifier
    #        if line['cera_type'] == 'none':
    #            for l in self.cera[sql_name]:
    #                dumtyp={}
    #                dumval = {}
    #                dumval[line['datacite_name']]=l[column]
    #                values.append(dumval)
    #                if line['datacite_type']!='none':
    #                    dumm = re.split('\.',line['datacite_type'])
    #                    dumtyp[dumm[0]] = dumm[1] 
    #                    types.append(dumtyp)                        
    #        else:
    #            (cera_col,col_val)=re.split('\.',line['cera_type'])
    #            for l in self.cera[sql_name]:
    #                dumtyp={}
    #                dumval = {}
    #                if col_val != l[cera_col]:
    #                    continue
    #                dumval[line['datacite_name']]=l[column]
    #                values.append(dumval)
    #                if line['datacite_type']!='none':
    #                    dumm = re.split('\.',line['datacite_type'])
    #                    dumtyp[dumm[0]] = dumm[1] 
    #                    types.append(dumtyp)
    #
    #    return values,types



    #def evaluateSql(self,sql,type,name):
    #    """not used""" 
    #    templ={}
    #    templ[type]=name
    #    return Template(sql).safe_substitute(templ)


         
    def getColumns(self,gcur):
        """getColumns: get columns for cursor results"""

        col_names=[]
        for i in range(0, len(gcur.description)):
            col_names.append(gcur.description[i][0])
        return col_names


    def callError(self,sql,errname):
        """callError: error exit and closing of db connection"""

        print '\nError in execution of SQL statement: \'%s\'\n%s -> EXIT\n' % (sql,errname)
        self.cur.close()
        self.iconn.close()


if __name__=='__main__':
    

    help = """
    Usage: """+sys.argv[0]+""" --id=<DRS_ID> [--outdir=output directory] [--version=<datacite version>]
    
           required: --id
           defaults: --outdir: /pf/k/k204082/export/cmip6cite
                     --version: 1
           db selects for checks in get_doimd_sql.conf
           datacite template in subdirectory templates

    Example call:

           ./get_doimd.py --id='cmip5.output1.MPI-M.MPI-ESM-P.past1000' --outdir=.
           
    """ 


    if len(sys.argv) < 2:
        print help
        sys.exit()
        
    keyw = ['id=','outdir=','version=','help']
    cmd  = 'hi:o:v:'
    
    opt=getopt.getopt(sys.argv[1:],cmd,keyw)

    outdir = ''
    version = ''

    if opt[1]!=[]:
        file=opt[1][-1]
    for o,p in opt[0]:
        if o in ['--id','-i']:
            dsg_name=p
        if o in ['--outdir','-o']:
            outdir=os.path.abspath(p)
        if o in ['--version','-v']:
            version=p
        if o in ['--help','-h','-?']:
            print help
            sys.exit()
            
    if len(outdir)==0:
        outdir = '/home/dkrz/k204082/export/cmip6cite'


    if not os.path.isdir(outdir):        
        os.mkdir(outdir)

    mydir = '/'.join(re.split('/',os.path.abspath(sys.argv[0]).strip())[:-1])
    # MS 2019-12-03: pcera.dkrz.de -> pcera
    #getdoi = GetDoi(dsg_name,outdir,mydir,version,'pcera.dkrz.de')
    getdoi = GetDoi(dsg_name,outdir,mydir,version,'pcera')
    (e,message,url) = getdoi.getDoi()
    print '%s (code: %i)' % (message,e)
    print url
