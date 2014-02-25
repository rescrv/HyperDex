APIS = [('put',                     False, False, False),
        ('cond_put',                False, True,  False),
        ('put_if_not_exist',        False, False, True),
        ('del',                     True,  True,  False),
        ('cond_del',                True,  True,  False),
        ('atomic_add',              False, True,  False),
        ('cond_atomic_add',         False, True,  False),
        ('atomic_sub',              False, True,  False),
        ('cond_atomic_sub',         False, True,  False),
        ('atomic_mul',              False, True,  False),
        ('cond_atomic_mul',         False, True,  False),
        ('atomic_div',              False, True,  False),
        ('cond_atomic_div',         False, True,  False),
        ('atomic_mod',              False, True,  False),
        ('cond_atomic_mod',         False, True,  False),
        ('atomic_and',              False, True,  False),
        ('cond_atomic_and',         False, True,  False),
        ('atomic_or',               False, True,  False),
        ('cond_atomic_or',          False, True,  False),
        ('atomic_xor',              False, True,  False),
        ('cond_atomic_xor',         False, True,  False),
        ('string_prepend',          False, True,  False),
        ('cond_string_prepend',     False, True,  False),
        ('string_append',           False, True,  False),
        ('cond_string_append',      False, True,  False),
        ('list_lpush',              False, True,  False),
        ('cond_list_lpush',         False, True,  False),
        ('list_rpush',              False, True,  False),
        ('cond_list_rpush',         False, True,  False),
        ('set_add',                 False, True,  False),
        ('cond_set_add',            False, True,  False),
        ('set_remove',              False, True,  False),
        ('cond_set_remove',         False, True,  False),
        ('set_intersect',           False, True,  False),
        ('cond_set_intersect',      False, True,  False),
        ('set_union',               False, True,  False),
        ('cond_set_union',          False, True,  False),
        ('map_add',                 False, True,  False),
        ('cond_map_add',            False, True,  False),
        ('map_remove',              False, True,  False),
        ('cond_map_remove',         False, True,  False),
        ('map_atomic_add',          False, True,  False),
        ('cond_map_atomic_add',     False, True,  False),
        ('map_atomic_sub',          False, True,  False),
        ('cond_map_atomic_sub',     False, True,  False),
        ('map_atomic_mul',          False, True,  False),
        ('cond_map_atomic_mul',     False, True,  False),
        ('map_atomic_div',          False, True,  False),
        ('cond_map_atomic_div',     False, True,  False),
        ('map_atomic_mod',          False, True,  False),
        ('cond_map_atomic_mod',     False, True,  False),
        ('map_atomic_and',          False, True,  False),
        ('cond_map_atomic_and',     False, True,  False),
        ('map_atomic_or',           False, True,  False),
        ('cond_map_atomic_or',      False, True,  False),
        ('map_atomic_xor',          False, True,  False),
        ('cond_map_atomic_xor',     False, True,  False),
        ('map_string_prepend',      False, True,  False),
        ('cond_map_string_prepend', False, True,  False),
        ('map_string_append',       False, True,  False),
        ('cond_map_string_append',  False, True,  False)]

NAMES = set([a[0] for a in APIS])

for name, erase, fail_if_not_found, fail_if_found in APIS:
    content = open('api/desc/%s.tex' % name).read()
    content = content.split('%%% Generated below here')[0].strip()
    content += '\n\n%%% Generated below here\n'
    content += '\\paragraph{Behavior:}\n'
    content += '\\begin{itemize}[noitemsep]\n'
    # What type of operation is this?
    if erase:
        content += '\\input{api/fragments/erase}\n'
    elif not fail_if_not_found and not fail_if_found:
        content += '\\input{api/fragments/no_fail}\n'
    elif fail_if_not_found:
        content += '\\input{api/fragments/fail_if_not_found}\n'
    elif fail_if_found:
        content += '\\input{api/fragments/fail_if_found}\n'
    elif fail_if_not_found and fail_if_found:
        assert False
    # Is it a conditional operation
    if name.startswith('cond_'):
        content += '\\input{api/fragments/conditional}\n'
    # Is it a map operation
    if name.startswith('map_'):
        if name[4:] in NAMES:
            content += '\\input{api/fragments/map_operation}\n'
    if name.startswith('cond_map_'):
        if name[9:] in NAMES:
            content += '\\input{api/fragments/map_operation}\n'
    content += '\\end{itemize}\n'
    fout = open('api/desc/%s.tex' % name, 'w')
    fout.write(content)
    fout.flush()
