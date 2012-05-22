import os
import os.path

srcdir = os.path.abspath(os.curdir)
blddir = 'build'
VERSION = '0.0.1'

def set_options(opt):
  opt.tool_options('compiler_cxx')

def configure(conf):
  conf.check_tool('compiler_cxx')
  conf.check_tool('node_addon')
  libhc = conf.check(lib='hyperclient', libpath='', uselib_store='HC', mandatory=True)
  libhs = conf.check(lib='hyperspacehashing', libpath='', uselib_store='HS', mandatory=True)

def build(bld):
  obj = bld.new_task_gen('cxx', 'shlib', 'node_addon', uselib='HC HS'.split())
  obj.use = ['HC', 'HS']
  obj.target = 'hyperclient'
  obj.source = ['hyperclient/nodejs/hyperclient.cc']
  obj.includes = ['HyperDex', os.path.join(srcdir, 'hyperclient/nodejs'), os.path.join(srcdir, 'cvv8/include')]
