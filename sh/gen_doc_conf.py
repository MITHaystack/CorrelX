# -*- coding: utf-8 -*-
# <nbformat>3.0</nbformat>

# <codecell>

#!/usr/bin/env python
#
#The MIT CorrelX Correlator
#
#https://github.com/MITHaystack/CorrelX
#Contact: correlX@haystack.mit.edu
#Project leads: Victor Pankratius, Pedro Elosegui Project developer: A.J. Vazquez Alvarez
#
#Copyright 2017 MIT Haystack Observatory
#
#Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
#
#The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
#
#THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
#
#
#------------------------------
#------------------------------
#Project: CorrelX.
#File: gen_doc_conf.py.
#Author: A.J. Vazquez Alvarez (ajvazquez@haystack.mit.edu)
#Description:
"""
Script for generating configuration files for Sphinx. More details in argparser help.

TO DO
-----
 Fix warnings.
"""
#History:
#initial version: 2017.02 ajva
#MIT Haystack Observatory

from __future__ import print_function
import time
import argparse
import sys
import os


# conf.py
#  Project info
C_CX_NAME =       "CorrelX"
C_CX_COPYRIGHT =  "2017, MIT Haystack"
C_CX_AUTHOR =     "MIT Haystack"
C_CX_VERSION =    "0.63"
C_CX_RELEASE =    "0.63"

#  Formatting
C_CX_HTML_THEME = "classic"
C_CX_LOGO =       "MIT_HO_logo_landscape.png"
C_CX_USE_LOGO =   "no"



################################################################

# index.rst


FIRST_LINES = [".. CorrelX documentation master file, created through gen_doc_conf.py + Sphinx + numpydoc on "+time.strftime("%Y.%m.%d")+".",\
               "",\
               "Welcome to CorrelX's documentation!",\
               "===================================",\
               "",\
               ".. toctree::",\
               "   :maxdepth: 1",\
               "",\
               "License",\
               "-------",\
               ""]


STR_V_LICENSE = ["The MIT CorrelX Correlator",\
                 "",\
                 "https://github.com/MITHaystack/CorrelX",\
                 "Contact: correlX@haystack.mit.edu",\
                 "Project leads: Victor Pankratius, Pedro Elosegui Project developer: A.J. Vazquez Alvarez",\
                 "",\
                 "Copyright 2017 MIT Haystack Observatory",\
                 "",\
                 "Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the \"Software\"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:",\
                 "",\
                 "The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.",\
                 "",\
                 "THE SOFTWARE IS PROVIDED \"AS IS\", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE."]


STR_V_INTRO = ["",\
               "Introduction",\
               "------------",\
               "",\
               "This documentation was generated from the CorrelX sources with Sphinx+numpydoc. For more details please refer to \"CorrelX developer's guide 1.0\".",\
               "",\
               ".. contents:: Table of Contents",\
               "   :depth: 2",
               ]


LAST_LINES = ["Indices and tables",
              "==================",
              "* :ref:`genindex`",
              "* :ref:`modindex`",
              "* :ref:`search`"]

C_AUTOMODULE =     ".. automodule:: "
C_MEMBERS =        "   :members:"
C_UNDOC_MEMBERS =  "   :undoc-members:"




# conf.py

STR_V_IMPORT = ["import os",\
                "import sys"]
STR_PATH_0 =    "sys.path.insert(0, os.path.abspath('"
STR_PATH_1 =    "'))"
STR_V_EXT =    ["extensions = ['sphinx.ext.autodoc', 'sphinx.ext.coverage', 'numpydoc']",\
                "templates_path = ['_templates']",\
                "source_suffix = '.rst'",\
                "master_doc = 'index'",\
                "project = u'"+C_CX_NAME+"'",\
                "copyright = u'"+C_CX_COPYRIGHT+"'",\
                "author = u'"+C_CX_AUTHOR+"'",\
                "version = u'"+C_CX_VERSION+"'",\
                "release = u'"+C_CX_RELEASE+"'",\
                "language = None",\
                "exclude_patterns = []",\
                "pygments_style = 'sphinx'",\
                "todo_include_todos = False",\
                "html_theme = '"+C_CX_HTML_THEME+"'"]
STR_LOGO =      "html_logo = \""+C_CX_LOGO+"\""
STR_V_REST =    ["html_static_path = ['_static']",\
                "htmlhelp_basename = '"+C_CX_NAME+"doc'"]




def main(cx_src_path):
    
    sys.path.append(cx_src_path)
    import lib_code_stats
    
    print("Processing sources from "+cx_src_path +" ...")
    
    str_v_index = []
    str_v_conf = []
    #cx_src_path = os.path.dirname(sys.argv[0]) #+"/../src" #"path_to_cx_src"
    file_index_rst = "index.rst_auto"
    file_conf_py = "conf.py_auto"
    
    
    print("Generating "+file_index_rst+" ...")

                                                          #                                 ---index.rst---
    for i in FIRST_LINES:                                 # header
        str_v_index.append(i)
        
    for i in STR_V_LICENSE:                               # license
        str_v_index.append(i)
    
    for i in STR_V_INTRO:                                 # intro
        str_v_index.append(i)
    
    
    for i in lib_code_stats.FILELIST_V:                   # for every section
        section = i[0]
        str_v_index.append("")
        str_v_index.append(section)
        str_v_index.append("="*len(section))
        for j in i[1].split(','):                         #    for every source file
            fname=j.split('.')[0]
            str_v_index.append("")
            str_v_index.append(j)
            str_v_index.append("-"*len(j))
            str_v_index.append("")
            str_v_index.append(C_AUTOMODULE+fname)
            str_v_index.append(C_MEMBERS)
            str_v_index.append(C_UNDOC_MEMBERS)
    
    str_v_index.append("")                                # last lines
    for i in LAST_LINES:
        str_v_index.append(i)
        
     
    with open(file_index_rst,'w') as f_out:
        for i in str_v_index:
            print(i,file=f_out)
        
    
    print("Generating "+file_conf_py+" ...")

                                                         #                                 ---conf.py---
    for i in STR_V_IMPORT:                               # import
        str_v_conf.append(i)
    
    str_v_conf.append(STR_PATH_0+cx_src_path+STR_PATH_1) # path to sources
    
    for i in STR_V_EXT:                                  # contents
        str_v_conf.append(i)
        
    if C_CX_USE_LOGO=="yes":
        str_v_conf.append(STR_LOGO)
    else:
        str_v_conf.append("#"+STR_LOGO)
        
    with open(file_conf_py,'w') as f_out:
        for i in str_v_conf:
            print(i,file=f_out)
    
    print("Done.")
    
if __name__ == '__main__':
    
    help_str='It generates the contents for the files index.rst and conf.py '+\
                                      'used in Sphinx.\n\n'+\
                                      'Requirements\n'+\
                                      '------------\n'+\
                                      ' Sphinx (e.g.: sudo apt-get install sphinx3)\n'+\
                                      ' Numpydoc extension for Sphinx (e.g.: sudo pip install numpydoc)\n\n'+\
                                      'Example\n'+\
                                      '-------\n'+\
                                      ' mkdir doc_cx\n'+\
                                      ' cd doc_cx\n'+\
                                      ' sphinx-quickstart\n'+\
                                      ' python <correlx_path>/correlx/sh/gen_doc_conf.py <correlx_path>/correlx/src\n'+\
                                      ' cp index.rst_auto source/index.rst\n'+\
                                      ' cp conf.py_auto source/conf.py\n'+\
                                      ' sphinx-build -b html source build && make html'
    
    cparser = argparse.ArgumentParser(description='CorrelX Sphinx index+conf files generator.\n',\
                                      epilog=help_str,\
                                      formatter_class=argparse.RawTextHelpFormatter)
    cparser.add_argument('correlx_src',
                         help="Path to CorrelX sources.")

    

    args =     cparser.parse_args()
    cx_src_path =   os.path.abspath(args.correlx_src)
    
    main(cx_src_path)

# <codecell>


# <codecell>


