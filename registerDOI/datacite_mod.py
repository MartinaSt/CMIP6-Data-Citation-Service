#! /usr/bin/env python
"""Module to Register/Ask for DOI metadata at DataCite and
Register/Ask for DOI
return (error message, HTTP status code) -> ok for error code < 300
Version: V0.1 2017-05-02: created by stockhause@dkrz.de
         V0.2 2018-03-02: first version with devel-operational (stockhause@dkrz.de)
         V0.3 2020-06-15 new datacite repository (stockhause@dkrz.de)"""

import sys, base64, codecs
import getopt, re, os.path
import json
import httplib2

class DataCite:

    def __init__(self,mydir):
        self.mydir    = mydir
        f=os.path.abspath(os.path.relpath(mydir+'/../config.json'))
        # MS 2019-02-04
        #config = json.loads(open(f,'r').read())
        config = []
        with open(f,'r') as f:
            config = json.loads(f.read())

        # add paths
        for l in config["addpaths"]:
            sys.path.append(l)
        sys.path.append(mydir) 

        self.endpoint = 'https://mds.datacite.org/'
        #self.user     = 'TIB.WDCC'
        self.user     = 'DKRZ.ESGF'
        # MS 2019-02-04
        #self.passwd   = open(os.path.abspath(os.path.relpath(mydir+'/../.datacite'))).read().strip()
        #fpwd          = open(os.path.abspath(os.path.relpath(mydir+'/../.datacite')))
        fpwd          = open(os.path.abspath(os.path.relpath(mydir+'/../.dataciteesgf')))
        self.passwd   = fpwd.read().strip()
        fpwd.close()

        #self.version = "datacite_mod.py (stockhause@dkrz.de, 2017-01-31/2018-02-23)"
        self.version = "datacite_mod.py (stockhause@dkrz.de, 2017-01-31/2018-02-23/2020-06-15)"
    

    def callDataCite(self,mode,request,test,xml,doi,url):
        """callDataCite: mode:metadata|doi (registration), request:GET|POST, test:0|1, xml:file location"""
        
        endpoint = self.endpoint+mode
        header = {'Content-Type':'text/plain;charset=UTF-8'}
        body_unicode = ''

        if test == 0:  # no testing
            testadd = ''
        else:
            testadd = '?testMode=1'


        # AA
        h = httplib2.Http()
        auth_string = base64.encodestring(self.user + ':' + self.passwd)

        if mode=='metadata' and request=='GET':
            header = {'Content-Type':'text/plain;charset=UTF-8',
                      'Accept':'application/xml',
                      'Authorization':'Basic ' + auth_string}
        elif mode=='doi' and request=='GET':
            header = {'Authorization':'Basic ' + auth_string}
        elif mode=='metadata' and request=='POST':
            header = {'Content-Type':'application/xml;charset=UTF-8',
                      'Authorization':'Basic ' + auth_string}
            # MS 2019-02-04
            #body_unicode = codecs.open(xml, 'r', encoding='utf-8').read()
            fbucode = codecs.open(xml, 'r', encoding='utf-8')
            body_unicode = fbucode.read()
            fbucode.close()
        elif mode=='doi' and request=='POST':
            header = {'Content-Type':'text/plain;charset=UTF-8',
                      'Authorization':'Basic ' + auth_string}
            body_unicode = 'doi='+doi+'\nurl='+url
        else:
            return ('Unsupported combination of request %s and mode %s' % (request,mode),999)

        print 'REQUEST: '+request+'\nMode:'+mode+'\nMetadata: '+xml+'\nDOI: '+doi+'\nURL: '+url+'\nTestmode: '+testadd
        print 'ENDPOINT: '+endpoint+'\n'


        # Access DataCite REST Service
        if request == 'GET':
            endpoint += '/'+doi
            response, content = h.request(endpoint,
                                          request,
                                          headers=header)
    
        # elif request == 'POST' or request == 'PUT':
        elif request == 'POST':
            response, content = h.request(endpoint +testadd,
                                          request,
                                          body = body_unicode.encode('utf-8'),
                                          headers=header)


        # MDS error Codes 
        # 200 OK - operation successful
        # 201 Created - operation successful
        # 204 No Content - DOI is known to MDS, but is not minted (or not resolvable e.g. due to handle's latency)
        # 400 Bad Request - request body must be exactly two lines: DOI and URL; wrong domain, wrong prefix
        # 401 Unauthorized - no login
        # 403 Forbidden - login problem or dataset belongs to another party or quota exceeded
        # 404 Not Found - DOI does not exist in our database
        # 410 Gone - the requested dataset was marked inactive (using DELETE method)
        # 412 Precondition failed - metadata must be uploaded first
        # 500 Internal Server Error - server internal error, try later and if problem persists please contact us 

        return (content.decode('utf-8'),response.status)




if __name__=='__main__':


    # init
    # mode:metadata|doi (registration), request:GET|POST|DELETE, test:0|1, xml:file location
    mydir=os.path.abspath(os.getcwd())
    dc = DataCite(mydir)
    
