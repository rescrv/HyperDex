  # Copyright (c) 2014, Pierre Rochard
  # All rights reserved.

# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:

#     * Redistributions of source code must retain the above copyright notice,
#       this list of conditions and the following disclaimer.
#     * Redistributions in binary form must reproduce the above copyright
#       notice, this list of conditions and the following disclaimer in the
#       documentation and/or other materials provided with the distribution.
#     * Neither the name of HyperDex nor the names of its contributors may be
#       used to endorse or promote products derived from this software without
#       specific prior written permission.

# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.

from flask import render_template, request, redirect, url_for, session
from app import app
from forms import DesignSpace, AddAttribute
import hypersys
import hyperadmin
import hyperclient

@app.route('/', methods=['GET', 'POST'])
@app.route('/index', methods=['GET', 'POST'])
@app.route('/noc', methods=['GET', 'POST'])
def noc():
    coordstatus = hypersys.checkcoordstatus()
    nodestatus = hypersys.checknodestatus()
    spacelist = hypersys.listspaces()
    config = hypersys.getconfig()


    cluster = config[0]
    version = config[1]
    flags = config[2]
    try:
        server = config[3]
    except:
        server = ''


    return render_template("noc.html",
        title = 'NOC', coordstatus = coordstatus, nodestatus = nodestatus, spacelist = spacelist, 
        cluster = cluster, version = version, flags = flags, server = server)


@app.route('/coordinator/start')
def start_coordinator():
    hypersys.startcoord()
    return redirect(url_for('noc'))

@app.route('/coordinator/stop')
def stop_coordinator():
    hypersys.stopcoord()
    return redirect(url_for('noc'))

@app.route('/node/start')
def start_node():
    hypersys.startnode()
    return redirect(url_for('noc'))

@app.route('/node/stop')
def stop_node():
    hypersys.stopnodes()
    return redirect(url_for('noc'))

@app.route('/coordinator/delete')
def delete_coordinator():
    hypersys.deletecoordata()
    return redirect(url_for('noc'))

@app.route('/node/delete')
def delete_node():
    hypersys.deletenodedata()
    return redirect(url_for('noc'))

@app.route('/delete/<space>')
def delete_space(space):
    hypersys.delspace(space)
    return redirect(url_for('noc'))

@app.route('/spaces/new', methods=['GET', 'POST'])
def design_space():
    session['newspace'] = []
    session['attrlist'] = []
    form = DesignSpace()
    if request.method == 'POST':
        spacename = form.spacename.data.encode('utf-8')
        keyname = form.keyname.data.encode('utf-8')
        partitions = form.partitions.data
        failures = form.failures.data
        session['newspace'].extend([spacename, keyname,partitions, failures])
        return redirect(url_for('design_attributes'))
    return render_template('newspace.html', form=form)

@app.route('/spaces/attributes', methods=['GET','POST'])
def design_attributes():
    form = AddAttribute()
    spacename = session['newspace'][0]

    if request.method == 'POST':
        attributetype = form.attributetype.data.encode('utf-8')
        attributename = form.attributename.data.encode('utf-8')
        attributesub = form.attributesub.data
        session['attrlist'].append([attributetype, attributename, attributesub])
        return render_template('newchar.html', form=form,
            attrlist=session['attrlist'],
            spacename=spacename)
    return render_template('newchar.html', form=form,
        spacename=spacename,
        attrlist=session['attrlist'])

@app.route('/space/create',methods=['POST','GET'])
def create_space():
    newspace = session['newspace']
    attrlist = session['attrlist']
    hypersys.create_space(newspace, attrlist)
    return redirect(url_for('noc'))

@app.route('/explore/<space>/', methods=['POST','GET'])
def explore_space(space):
    results = hypersys.getall(space)
    print results
    return render_template('explorespace.html', space = space)