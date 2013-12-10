import os
import sys

BASE = os.path.join(os.path.dirname(__file__), '../..')

sys.path.append(os.path.join(BASE, 'bindings'))

import generator

def generate_func(x):
    assert x.form in (generator.AsyncCall, generator.Iterator)
    func = 'int64_t hyperdex_client_%s(struct hyperdex_client* client' % x.name
    padd = ' ' * 16
    for arg in x.args_in:
        func += ',\n' + padd
        func += ', '.join([p + ' ' + n for p, n in arg.args])
    for arg in x.args_out:
        func += ',\n' + padd
        func += ', '.join([p + '* ' + n for p, n in arg.args])
    func += ');\n'
    return func

ARGS_IN = {(generator.AsyncCall, generator.SpaceName): 'The name of the space as a c-string.'
          ,(generator.AsyncCall, generator.Key): 'The key for the operation where '
           '\\code{key} is a bytestring and \\code{key\\_sz} specifies '
           'the number of bytes in \\code{key}.'
          ,(generator.AsyncCall, generator.Attributes): 'The set of attributes '
           'to modify and their respective values.  \\code{attrs} points to '
           'an array of length \\code{attrs\_sz}.'
          ,(generator.AsyncCall, generator.MapAttributes): 'The set of map '
           'attributes to modify and their respective key/values.  '
           '\\code{mapattrs} points to an array of length '
           '\\code{mapattrs\_sz}.  Each entry specify an attribute that is a '
           'map and a key within that map.'
          ,(generator.AsyncCall, generator.Predicates): 'A set of predicates '
           'to check against.  \\code{checks} points to an array of length '
           '\\code{checks\_sz}.'
          ,(generator.Iterator, generator.SpaceName): 'The name of the space as a c-string.'
          ,(generator.Iterator, generator.SortBy): 'The attribute to sort by.'
          ,(generator.Iterator, generator.Limit): 'The number of results to return.'
          ,(generator.Iterator, generator.MaxMin): 'Maximize (!= 0) or minimize (== 0).'
          ,(generator.Iterator, generator.Predicates): 'A set of predicates '
           'to check against.  \\code{checks} points to an array of length '
           '\\code{checks\_sz}.'
          }
ARGS_OUT = {(generator.AsyncCall, generator.Status): 'The status of the '
            'operation.  The client library will fill in this variable before '
            'returning this operation\'s request id from '
            '\\code{hyperdex\\_client\\_loop}.  The pointer must remain valid '
            'until then, and the pointer should not be aliased to the status '
            'for any other outstanding operation.'
           ,(generator.AsyncCall, generator.Attributes): 'An array of attributes '
            'that comprise a returned object.  The application must free the '
            'returned values with \\code{hyperdex\_client\_destroy\_attrs}.  The '
            'pointers must remain valid until the operation completes.'
           ,(generator.AsyncCall, generator.Count): 'The number of objects which '
            'match the predicates.'
           ,(generator.AsyncCall, generator.Description): 'The description of '
            'the search.  This is a c-string that the client must free.'
           ,(generator.Iterator, generator.Status): 'The status of the '
            'operation.  The client library will fill in this variable before '
            'returning this operation\'s request id from '
            '\\code{hyperdex\\_client\\_loop}.  The pointer must remain valid '
            'until the operation completes, and the pointer should not be '
            'aliased to the status for any other outstanding operation.'
           ,(generator.Iterator, generator.Attributes): 'An array of attributes '
            'that comprise a returned object.  The application must free the '
            'returned values with \\code{hyperdex\_client\_destroy\_attrs}.  The '
            'pointers must remain valid until the operation completes.'
           }

def generate_doc_block(x):
    output = ''
    output += '\\paragraph{{\code{{{0}}}}}\n'.format(c.name.replace('_', '\\_'))
    output += '\\index{{{0}!C API}}\n'.format(c.name.replace('_', '\\_'))
    output += '\\begin{ccode}\n'
    output += generate_func(c)
    output += '\\end{ccode}\n'
    output += '\\funcdesc \input{{\\topdir/api/desc/{0}}}\n\n'.format(c.name)
    output += '\\noindent\\textbf{Parameters:}\n'
    max_label = ''
    parameters = ''
    for arg in c.args_in:
        if (c.form, arg) not in ARGS_IN:
            print 'missing in', (c.form, arg)
            continue
        label = ', '.join(['\\code{' + a[1].replace('_', '\\_') + '}' for a in arg.args])
        parameters += '\\item[{0}] {1}\n'.format(label, ARGS_IN[(c.form, arg)])
        if len(label) > len(max_label):
            max_label = label
    if parameters:
        output += '\\begin{description}[labelindent=\\widthof{{' + max_label + '}},leftmargin=*,noitemsep,nolistsep,align=right]\n'
        output += parameters
        output += '\\end{description}\n'
    else:
        output += 'None\n'
    output += '\n\\noindent\\textbf{Returns:}\n'
    max_label = ''
    returns = ''
    for arg in c.args_out:
        if (c.form, arg) not in ARGS_OUT:
            print 'missing out', (c.form, arg)
            continue
        label = ', '.join(['\\code{' + a[1].replace('_', '\\_') + '}' for a in arg.args])
        returns += '\\item[{0}] {1}\n'.format(label, ARGS_OUT[(c.form, arg)])
        if len(label) > len(max_label):
            max_label = label
    if returns:
        output += '\\begin{description}[labelindent=\\widthof{{' + max_label + '}},leftmargin=*,noitemsep,nolistsep,align=right]\n'
        output += returns
        output += '\\end{description}\n'
    else:
        output += 'Nothing\n'
    output += '\n'
    return output

if __name__ == '__main__':
    with open(os.path.join(BASE, 'doc/api/c.client.tex'), 'w') as fout:
        output = '% This LaTeX file is generated by bindings/c/gen_doc.py\n\n'
        for c in generator.Client:
            output += generate_doc_block(c)
        output = output.strip()
        fout.write(output + '\n')
