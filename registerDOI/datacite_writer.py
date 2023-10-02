import os,re,sys

# not needed: to be checked!
#sys.path.append('/sw/rhel6-x64/python/python-2.7-ve0-gcc49/bin')
#sys.path.append('/home/dkrz/k204082/.local/lib/python2.7/site-packages')
#sys.path.append('/pf/k/k204082/src/registerDOI')
#sys.path.append('/pf/k/k204082/src/registerDOI/templates')
#sys.path.append('/sw/rhel6-x64/python/python-2.7-ve0-gcc49/lib/python2.7')
#sys.path.append('/sw/rhel6-x64/python/python-2.7-ve0-gcc49/lib/python2.7/site-packages')

import tempfile

import mako
from mako.template import Template
from mako.lookup import TemplateLookup
import os,os.path,sys

# set pathes
mydir=os.path.abspath(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(mydir) 
sys.path.append(mydir+'/templates') 

mylookup = TemplateLookup(directories=['templates',mydir+'/templates'],default_filters=['trim'])


def write(datacite, template, outfile=None):

    mytemplate = mylookup.get_template(template)
    result = mytemplate.render_unicode(dc=datacite).encode('utf-8')
    _write_file(target=outfile, content=result)
    return result
    

def _write_file(target, content):
    if target!=None:
        with open(target, 'w') as f:
            f.write(content)



