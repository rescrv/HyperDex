def test_000_basic_key_value_store(cfg):
    # space kv dimensions k, v key k auto 0 1
    # TODO: cfg.should().rm_space('kv')
    cfg.must().add_space('''
        space kv
        key k
        attributes v 
    ''')
    cfg.should().get('kv','k1').equals(None)
    cfg.should().get('kv','k2').equals(None)
    cfg.should().get('kv','k3').equals(None)
    cfg.should().get('kv','k4').equals(None)
    cfg.must().delete('kv','k1').equals(False)
    cfg.must().delete('kv','k2').equals(False)
    cfg.must().delete('kv','k3').equals(False)
    cfg.must().delete('kv','k4').equals(False)
    cfg.must().put('kv','k1',{'v': 'v1'}).equals(True)
    cfg.should().get('kv','k1').equals({'v': 'v1'})
    cfg.should().get('kv','k2').equals(None)
    cfg.should().get('kv','k3').equals(None)
    cfg.should().get('kv','k4').equals(None)
    cfg.must().put('kv','k2',{'v': 'v2'}).equals(True)
    cfg.should().get('kv','k1').equals({'v': 'v1'})
    cfg.should().get('kv','k2').equals({'v': 'v2'})
    cfg.should().get('kv','k3').equals(None)
    cfg.should().get('kv','k4').equals(None)
    cfg.must().put('kv','k3',{'v': 'v3'}).equals(True)
    cfg.should().get('kv','k1').equals({'v': 'v1'})
    cfg.should().get('kv','k2').equals({'v': 'v2'})
    cfg.should().get('kv','k3').equals({'v': 'v3'})
    cfg.should().get('kv','k4').equals(None)
    cfg.must().put('kv','k4',{'v': 'v4'}).equals(True)
    cfg.should().get('kv','k1').equals({'v': 'v1'})
    cfg.should().get('kv','k2').equals({'v': 'v2'})
    cfg.should().get('kv','k3').equals({'v': 'v3'})
    cfg.should().get('kv','k4').equals({'v': 'v4'})
    cfg.must().delete('kv','k1').equals(True)
    cfg.must().delete('kv','k2').equals(True)
    cfg.must().delete('kv','k3').equals(True)
    cfg.must().delete('kv','k4').equals(True)
    cfg.should().get('kv','k1').equals(None)
    cfg.should().get('kv','k2').equals(None)
    cfg.should().get('kv','k3').equals(None)
    cfg.should().get('kv','k4').equals(None)

def test_000_error_unknownspace(cfg):
    cfg.shouldnt().get('noexist','k')# HYPERCLIENT_UNKNOWNSPACE

def test_010_basic_multi_attribute_space(cfg):
    # space kv dimensions k, v1, v2, v3, v4 key k auto 0 1
    cfg.should().rm_space('kv')
    cfg.must().add_space('''
        space kv
        key k
        attributes v1, v2, v3, v4 
    ''')
    cfg.should().get('kv','k').equals(None)
    cfg.must().delete('kv','k').equals(False)
    cfg.must().put('kv','k',{'v1': 'v1'}).equals(True)
    cfg.should().get('kv','k').equals({'v1': 'v1', 'v2': '', 'v3': '', 'v4': ''})
    cfg.must().put('kv','k',{'v2': 'v2', 'v3': 'v3'}).equals(True)
    cfg.should().get('kv','k').equals({'v1': 'v1', 'v2': 'v2', 'v3': 'v3', 'v4': ''})
    cfg.must().delete('kv','k').equals(True)
    cfg.should().get('kv','k').equals(None)

def test_020_basic_equality_search(cfg):
    cfg.should().rm_space('kv')
    cfg.must().add_space('''
        space kv
        key k
        attributes v 
    ''')
    cfg.should().get('kv','ka').equals(None)
    cfg.should().get('kv',"ka'").equals(None)
    cfg.should().get('kv','kb').equals(None)
    cfg.should().get('kv','kc').equals(None)
    cfg.should().get('kv','kd').equals(None)
    cfg.must().put('kv','ka',{'v': 'a'}).equals(True)
    cfg.must().put('kv',"ka'",{'v': 'a'}).equals(True)
    cfg.must().put('kv','kb',{'v': 'b'}).equals(True)
    cfg.must().put('kv','kc',{'v': 'c'}).equals(True)
    cfg.must().put('kv','kd',{'v': 'd'}).equals(True)
    cfg.should().search('kv',{}).equals([
        {'k': 'ka', 'v': 'a'}, 
        {'k': "ka'", 'v': 'a'},
        {'k': "kb", 'v': 'b'},
        {'k': "kc", 'v': 'c'},
        {'k': "kd", 'v': 'd'}
    ])

    cfg.should().search('kv',{'v':'b'}).equals([{'k': 'kb', 'v': 'b'}])
    cfg.should().search('kv',{'v':'c'}).equals([{'k': 'kc', 'v': 'c'}])
    cfg.should().search('kv',{'v':'d'}).equals([{'k': 'kd', 'v': 'd'}])
    cfg.must().delete('kv','ka').equals(True)
    cfg.must().delete('kv',"ka'").equals(True)
    cfg.must().delete('kv','kb').equals(True)
    cfg.must().delete('kv','kc').equals(True)
    cfg.must().delete('kv','kd').equals(True)
    cfg.should().get('kv','ka').equals(None)
    cfg.should().get('kv',"ka'").equals(None)
    cfg.should().get('kv','kb').equals(None)
    cfg.should().get('kv','kc').equals(None)
    cfg.should().get('kv','kd').equals(None)

def test_030_basic_cond_put(cfg):
    cfg.should().rm_space('kv')
    cfg.must().add_space('''
        space kv
        key k
        attributes v1, v2 
    ''')
    cfg.should().get('kv','k').equals(None)
    cfg.must().put('kv','k',{'v1': '1', 'v2': '2'}).equals(True)
    cfg.should().get('kv','k').equals({'v1': '1', 'v2': '2'})
    cfg.should().cond_put('kv','k',{'v1':'1'}, {'v1': '3'}).equals(True)
    cfg.should().get('kv','k').equals({'v1': '3', 'v2': '2'})
    cfg.should().cond_put('kv','k',{'v1':'1'},{'v1': '4'}).equals(False)
    cfg.should().get('kv','k').equals({'v1': '3', 'v2': '2'})
    cfg.should().cond_put('kv','k',{'v2':'2'},{'v1': '4'}).equals(True)
    cfg.should().get('kv','k').equals({'v1': '4', 'v2': '2'})
    cfg.should().cond_put('kv','k',{'v2':'1'},{'v1': '5'}).equals(False)
    cfg.should().get('kv','k').equals({'v1': '4', 'v2': '2'})
    cfg.must().delete('kv','k').equals(True)
    cfg.should().get('kv','k').equals(None)

def test_040_basic_put_if_not_exist(cfg):
    # space kv dimensions k, v1, v2 key k auto 0 1
    cfg.should().rm_space('kv')
    cfg.must().add_space('''
        space kv
        key k
        attributes v1, v2 
    ''')
    cfg.should().get('kv','k').equals(None)
    cfg.should().put_if_not_exist('kv','k',{'v1': '1', 'v2': '2'}).equals(True)
    cfg.should().get('kv','k').equals({'v1': '1', 'v2': '2'})
    cfg.should().put_if_not_exist('kv','k',{'v1': 'a', 'v2': 'b'}).equals(False)
    cfg.should().get('kv','k').equals({'v1': '1', 'v2': '2'})
    cfg.must().delete('kv','k').equals(True)
    cfg.should().get('kv','k').equals(None)

def test_100_datatype_string(cfg):
    # space kv dimensions k, v key k auto 0 1
    cfg.should().rm_space('kv')
    cfg.must().add_space('''
        space kv
        key k
        attributes v 
    ''')
    cfg.should().get('kv','k').equals(None)
    cfg.must().put('kv','k',{}).equals(True)
    cfg.must().string_prepend('kv','k',{'v': '5'}).equals(True)
    cfg.should().get('kv','k').equals({'v': '5'})
    cfg.must().string_append('kv','k',{'v': '6'}).equals(True)
    cfg.should().get('kv','k').equals({'v': '56'})
    cfg.must().string_prepend('kv','k',{'v': '4'}).equals(True)
    cfg.should().get('kv','k').equals({'v': '456'})
    cfg.must().string_append('kv','k',{'v': '7'}).equals(True)
    cfg.should().get('kv','k').equals({'v': '4567'})
    cfg.must().string_prepend('kv','k',{'v': '3'}).equals(True)
    cfg.should().get('kv','k').equals({'v': '34567'})
    cfg.must().string_append('kv','k',{'v': '8'}).equals(True)
    cfg.should().get('kv','k').equals({'v': '345678'})
    cfg.must().string_prepend('kv','k',{'v': '2'}).equals(True)
    cfg.should().get('kv','k').equals({'v': '2345678'})
    cfg.must().string_append('kv','k',{'v': '9'}).equals(True)
    cfg.should().get('kv','k').equals({'v': '23456789'})
    cfg.must().string_prepend('kv','k',{'v': '1'}).equals(True)
    cfg.should().get('kv','k').equals({'v': '123456789'})
    cfg.must().string_append('kv','k',{'v': '0'}).equals(True)
    cfg.should().get('kv','k').equals({'v': '1234567890'})
    cfg.must().delete('kv','k').equals(True)
    cfg.should().get('kv','k').equals(None)

def test_110_datatype_int64(cfg):
    # space kv dimensions k, v (int64) key k auto 0 1
    cfg.should().rm_space('kv')
    cfg.must().add_space('''
        space kv
        key k
        attributes int64 v 
    ''')
    cfg.should().get('kv','k').equals(None)
    # Test signed-ness and the limits of a two's complement number
    cfg.must().put('kv','k',{'v': 0L}).equals(True)
    cfg.should().get('kv','k').equals({'v': 0L})
    cfg.must().put('kv','k',{'v': 1L}).equals(True)
    cfg.should().get('kv','k').equals({'v': 1L})
    cfg.must().put('kv','k',{'v': -1L}).equals(True)
    cfg.should().get('kv','k').equals({'v': -1L})
    cfg.must().put('kv','k',{'v': 9223372036854775807L}).equals(True)
    cfg.should().get('kv','k').equals({'v': 9223372036854775807L})
    cfg.must().put('kv','k',{'v': -9223372036854775808L}).equals(True)
    cfg.should().get('kv','k').equals({'v': -9223372036854775808L})
    # Test add
    cfg.must().put('kv','k',{'v': 0L}).equals(True)
    cfg.should().get('kv','k').equals({'v': 0L})
    cfg.must().atomic_add('kv','k',{'v': 9223372036854775807L}).equals(True)
    cfg.should().get('kv','k').equals({'v': 9223372036854775807L})
    cfg.mustnt().atomic_add('kv','k',{'v': 1L})# HYPERCLIENT_OVERFLOW
    cfg.should().get('kv','k').equals({'v': 9223372036854775807L})
    cfg.must().atomic_add('kv','k',{'v': -9223372036854775807L}).equals(True)
    cfg.should().get('kv','k').equals({'v': 0L})
    # Test sub
    cfg.must().put('kv','k',{'v': 0L}).equals(True)
    cfg.should().get('kv','k').equals({'v': 0L})
    cfg.should().atomic_sub('kv','k',{'v': 9223372036854775807L}).equals(True)
    cfg.should().get('kv','k').equals({'v': -9223372036854775807L})
    cfg.should().atomic_sub('kv','k',{'v': 1L}).equals(True)
    cfg.should().get('kv','k').equals({'v': -9223372036854775808L})
    cfg.shouldnt().atomic_sub('kv','k',{'v': 1L})# HYPERCLIENT_OVERFLOW
    cfg.should().get('kv','k').equals({'v': -9223372036854775808L})
    cfg.should().atomic_sub('kv','k',{'v': -9223372036854775808L}).equals(True)
    cfg.should().get('kv','k').equals({'v': 0L})
    # Test mul
    cfg.must().put('kv','k',{'v': 0L}).equals(True)
    cfg.should().get('kv','k').equals({'v': 0L})
    cfg.should().atomic_mul('kv','k',{'v': 0L}).equals(True)
    cfg.should().get('kv','k').equals({'v': 0L})
    cfg.should().atomic_mul('kv','k',{'v': 1L}).equals(True)
    cfg.should().get('kv','k').equals({'v': 0L})
    cfg.should().atomic_mul('kv','k',{'v': 9223372036854775807L}).equals(True)
    cfg.should().get('kv','k').equals({'v': 0L})
    cfg.should().atomic_mul('kv','k',{'v': -9223372036854775808L}).equals(True)
    cfg.should().get('kv','k').equals({'v': 0L})
    cfg.must().put('kv','k',{'v': 1L}).equals(True)
    cfg.should().get('kv','k').equals({'v': 1L})
    cfg.should().atomic_mul('kv','k',{'v': 2L}).equals(True)
    cfg.should().get('kv','k').equals({'v': 2L})
    cfg.should().atomic_mul('kv','k',{'v': 2L}).equals(True)
    cfg.should().get('kv','k').equals({'v': 4L})
    cfg.should().atomic_mul('kv','k',{'v': 2L}).equals(True)
    cfg.should().get('kv','k').equals({'v': 8L})
    cfg.should().atomic_mul('kv','k',{'v': 2L}).equals(True)
    cfg.should().get('kv','k').equals({'v': 16L})
    cfg.should().atomic_mul('kv','k',{'v': 2L}).equals(True)
    cfg.should().get('kv','k').equals({'v': 32L})
    cfg.should().atomic_mul('kv','k',{'v': 2L}).equals(True)
    cfg.should().get('kv','k').equals({'v': 64L})
    cfg.should().atomic_mul('kv','k',{'v': 72057594037927936L}).equals(True)
    cfg.should().get('kv','k').equals({'v': 4611686018427387904L})
    cfg.shouldnt().atomic_mul('kv','k',{'v': 2L})# HYPERCLIENT_OVERFLOW
    cfg.should().get('kv','k').equals({'v': 4611686018427387904L})
    # Test div
    cfg.must().put('kv','k',{'v': 4611686018427387904L}).equals(True)
    cfg.should().get('kv','k').equals({'v': 4611686018427387904L})
    cfg.should().atomic_div('kv','k',{'v': 72057594037927936L}).equals(True)
    cfg.should().get('kv','k').equals({'v': 64L})
    cfg.should().atomic_div('kv','k',{'v': 2L}).equals(True)
    cfg.should().get('kv','k').equals({'v': 32L})
    cfg.should().atomic_div('kv','k',{'v': 2L}).equals(True)
    cfg.should().get('kv','k').equals({'v': 16L})
    cfg.should().atomic_div('kv','k',{'v': 2L}).equals(True)
    cfg.should().get('kv','k').equals({'v': 8L})
    cfg.should().atomic_div('kv','k',{'v': 2L}).equals(True)
    cfg.should().get('kv','k').equals({'v': 4L})
    cfg.should().atomic_div('kv','k',{'v': 2L}).equals(True)
    cfg.should().get('kv','k').equals({'v': 2L})
    cfg.should().atomic_div('kv','k',{'v': 2L}).equals(True)
    cfg.should().get('kv','k').equals({'v': 1L})
    cfg.should().atomic_div('kv','k',{'v': 2L}).equals(True)
    cfg.should().get('kv','k').equals({'v': 0L})
    cfg.shouldnt().atomic_div('kv','k',{'v': 0L})# HYPERCLIENT_OVERFLOW
    cfg.should().get('kv','k').equals({'v': 0L})
    # Test mod
    cfg.must().put('kv','k',{'v': 7L}).equals(True)
    cfg.should().get('kv','k').equals({'v': 7L})
    cfg.should().atomic_mod('kv','k',{'v': 3L}).equals(True)
    cfg.should().get('kv','k').equals({'v': 1L})
    cfg.should().atomic_mod('kv','k',{'v': 2L}).equals(True)
    cfg.should().get('kv','k').equals({'v': 1L})
    cfg.must().put('kv','k',{'v': 7L}).equals(True)
    cfg.should().get('kv','k').equals({'v': 7L})
    cfg.should().atomic_mod('kv','k',{'v': -3L}).equals(True)
    cfg.should().get('kv','k').equals({'v': -2L})
    cfg.must().put('kv','k',{'v': -7L}).equals(True)
    cfg.should().get('kv','k').equals({'v': -7L})
    cfg.should().atomic_mod('kv','k',{'v': 3L}).equals(True)
    cfg.should().get('kv','k').equals({'v': 2L})
    cfg.must().put('kv','k',{'v': -7L}).equals(True)
    cfg.should().get('kv','k').equals({'v': -7L})
    cfg.should().atomic_mod('kv','k',{'v': -3L}).equals(True)
    cfg.should().get('kv','k').equals({'v': -1L})
    # Test and
    cfg.must().put('kv','k',{'v': -2401053089206453570L}).equals(True)
    cfg.should().get('kv','k').equals({'v': -2401053089206453570L})
    cfg.should().atomic_and('kv','k',{'v': -374081424649621491L}).equals(True)
    cfg.should().get('kv','k').equals({'v': -2698572151406022644L})
    # Test or
    cfg.must().put('kv','k',{'v': 0L}).equals(True)
    cfg.should().get('kv','k').equals({'v': 0L})
    cfg.should().atomic_or('kv','k',{'v': -6148914691236517206L}).equals(True)
    cfg.should().get('kv','k').equals({'v': -6148914691236517206L})
    cfg.should().atomic_or('kv','k',{'v': 6148914691236517205L}).equals(True)
    cfg.should().get('kv','k').equals({'v': -1L})
    # Test xor
    cfg.must().put('kv','k',{'v': 0L}).equals(True)
    cfg.should().get('kv','k').equals({'v': 0L})
    cfg.should().atomic_xor('kv','k',{'v': -6148914691236517206L}).equals(True)
    cfg.should().get('kv','k').equals({'v': -6148914691236517206L})
    cfg.should().atomic_xor('kv','k',{'v': 6148914691236517205L}).equals(True)
    cfg.should().get('kv','k').equals({'v': -1L})
    # Cleanup
    cfg.must().delete('kv','k').equals(True)
    cfg.should().get('kv','k').equals(None)

def test_120_datatype_float(cfg):
    # space kv dimensions k, v (float) key k auto 0 1
    cfg.should().rm_space('kv')
    cfg.must().add_space('''
        space kv
        key k
        attributes float v 
    ''')
    cfg.should().get('kv','k').equals(None)
    # Test signed-ness and precise floating point numbers
    cfg.must().put('kv','k',{'v': 0.0}).equals(True)
    cfg.should().get('kv','k').equals({'v': 0.0})
    cfg.must().put('kv','k',{'v': 1.0}).equals(True)
    cfg.should().get('kv','k').equals({'v': 1.0})
    cfg.must().put('kv','k',{'v': -1.0}).equals(True)
    cfg.should().get('kv','k').equals({'v': -1.0})
    cfg.must().put('kv','k',{'v': 9006104071832581.0}).equals(True)
    cfg.should().get('kv','k').equals({'v': 9006104071832581.0})
    # Test add
    cfg.must().put('kv','k',{'v': 0.0}).equals(True)
    cfg.should().get('kv','k').equals({'v': 0.0})
    cfg.must().atomic_add('kv','k',{'v': 9006104071832581.0}).equals(True)
    cfg.should().get('kv','k').equals({'v': 9006104071832581.0})
    cfg.must().atomic_add('kv','k',{'v': -9006104071832581.0}).equals(True)
    cfg.should().get('kv','k').equals({'v': 0.0})
    # Test sub
    cfg.must().put('kv','k',{'v': 0.0}).equals(True)
    cfg.should().get('kv','k').equals({'v': 0.0})
    cfg.should().atomic_sub('kv','k',{'v': 9006104071832581.0}).equals(True)
    cfg.should().get('kv','k').equals({'v': -9006104071832581.0})
    cfg.should().atomic_sub('kv','k',{'v': -9006104071832581.0}).equals(True)
    cfg.should().get('kv','k').equals({'v': 0.0})
    # Test mul
    cfg.must().put('kv','k',{'v': 0.0}).equals(True)
    cfg.should().get('kv','k').equals({'v': 0.0})
    cfg.should().atomic_mul('kv','k',{'v': 0.0}).equals(True)
    cfg.should().get('kv','k').equals({'v': 0.0})
    cfg.should().atomic_mul('kv','k',{'v': 1.0}).equals(True)
    cfg.should().get('kv','k').equals({'v': 0.0})
    cfg.should().atomic_mul('kv','k',{'v': 9006104071832581.0}).equals(True)
    cfg.should().get('kv','k').equals({'v': 0.0})
    cfg.should().atomic_mul('kv','k',{'v': -9006104071832581.0}).equals(True)
    cfg.should().get('kv','k').equals({'v': 0.0})
    cfg.must().put('kv','k',{'v': 1.0}).equals(True)
    cfg.should().get('kv','k').equals({'v': 1.0})
    cfg.should().atomic_mul('kv','k',{'v': 2.0}).equals(True)
    cfg.should().get('kv','k').equals({'v': 2.0})
    cfg.should().atomic_mul('kv','k',{'v': 2.0}).equals(True)
    cfg.should().get('kv','k').equals({'v': 4.0})
    cfg.should().atomic_mul('kv','k',{'v': 2.0}).equals(True)
    cfg.should().get('kv','k').equals({'v': 8.0})
    cfg.should().atomic_mul('kv','k',{'v': 2.0}).equals(True)
    cfg.should().get('kv','k').equals({'v': 16.0})
    cfg.should().atomic_mul('kv','k',{'v': 2.0}).equals(True)
    cfg.should().get('kv','k').equals({'v': 32.0})
    cfg.should().atomic_mul('kv','k',{'v': 2.0}).equals(True)
    cfg.should().get('kv','k').equals({'v': 64.0})
    cfg.should().atomic_mul('kv','k',{'v': 7.205759403792794e+16}).equals(True)
    cfg.should().get('kv','k').equals({'v': 4.611686018427388e+18})
    # Test div
    cfg.must().put('kv','k',{'v': 4.611686018427388e+18}).equals(True)
    cfg.should().get('kv','k').equals({'v': 4.611686018427388e+18})
    cfg.should().atomic_div('kv','k',{'v': 7.205759403792794e+16}).equals(True)
    cfg.should().get('kv','k').equals({'v': 64.0})
    cfg.should().atomic_div('kv','k',{'v': 2.0}).equals(True)
    cfg.should().get('kv','k').equals({'v': 32.0})
    cfg.should().atomic_div('kv','k',{'v': 2.0}).equals(True)
    cfg.should().get('kv','k').equals({'v': 16.0})
    cfg.should().atomic_div('kv','k',{'v': 2.0}).equals(True)
    cfg.should().get('kv','k').equals({'v': 8.0})
    cfg.should().atomic_div('kv','k',{'v': 2.0}).equals(True)
    cfg.should().get('kv','k').equals({'v': 4.0})
    cfg.should().atomic_div('kv','k',{'v': 2.0}).equals(True)
    cfg.should().get('kv','k').equals({'v': 2.0})
    cfg.should().atomic_div('kv','k',{'v': 2.0}).equals(True)
    cfg.should().get('kv','k').equals({'v': 1.0})
    cfg.should().atomic_div('kv','k',{'v': 2.0}).equals(True)
    cfg.should().get('kv','k').equals({'v': 0.5})
    cfg.should().atomic_div('kv','k',{'v': 2.0}).equals(True)
    cfg.should().get('kv','k').equals({'v': 0.25})
    cfg.must().put('kv','k',{'v': 1.0}).equals(True)
    cfg.should().get('kv','k').equals({'v': 1.0})
    cfg.should().atomic_div('kv','k',{'v': 0.0}).equals(True)
    # Cleanup
    cfg.must().delete('kv','k').equals(True)
    cfg.should().get('kv','k').equals(None)

def test_200_datatype_list_string(cfg):
    # space kv dimensions k, v (list(string)) key k auto 0 1
    cfg.should().rm_space('kv')
    cfg.must().add_space('''
        space kv
        key k
        attributes list(string) v 
    ''')
    cfg.should().get('kv','k').equals(None)
    cfg.must().put('kv','k',{'v': ['100', '200', '300']}).equals(True)
    cfg.should().get('kv','k').equals({'v': ['100', '200', '300']})
    cfg.must().put('kv','k',{'v': []}).equals(True)
    cfg.should().get('kv','k').equals({'v': []})
    cfg.should().list_lpush('kv','k',{'v': '5'}).equals(True)
    cfg.should().get('kv','k').equals({'v': ['5']})
    cfg.should().list_rpush('kv','k',{'v': '6'}).equals(True)
    cfg.should().get('kv','k').equals({'v': ['5', '6']})
    cfg.should().list_lpush('kv','k',{'v': '4'}).equals(True)
    cfg.should().get('kv','k').equals({'v': ['4', '5', '6']})
    cfg.should().list_rpush('kv','k',{'v': '7'}).equals(True)
    cfg.should().get('kv','k').equals({'v': ['4', '5', '6', '7']})
    cfg.should().list_lpush('kv','k',{'v': '3'}).equals(True)
    cfg.should().get('kv','k').equals({'v': ['3', '4', '5', '6', '7']})
    cfg.should().list_rpush('kv','k',{'v': '8'}).equals(True)
    cfg.should().get('kv','k').equals({'v': ['3', '4', '5', '6', '7', '8']})
    cfg.should().list_lpush('kv','k',{'v': '2'}).equals(True)
    cfg.should().get('kv','k').equals({'v': ['2', '3', '4', '5', '6', '7', '8']})
    cfg.should().list_rpush('kv','k',{'v': '9'}).equals(True)
    cfg.should().get('kv','k').equals({'v': ['2', '3', '4', '5', '6', '7', '8', '9']})
    cfg.should().list_lpush('kv','k',{'v': '1'}).equals(True)
    cfg.should().get('kv','k').equals({'v': ['1', '2', '3', '4', '5', '6', '7', '8', '9']})
    cfg.should().list_rpush('kv','k',{'v': '0'}).equals(True)
    cfg.should().get('kv','k').equals({'v': ['1', '2', '3', '4', '5', '6', '7', '8', '9', '0']})
    cfg.must().delete('kv','k').equals(True)
    cfg.should().get('kv','k').equals(None)

def test_210_datatype_list_int64(cfg):
    # space kv dimensions k, v (list(int64)) key k auto 0 1
    cfg.should().rm_space('kv')
    cfg.must().add_space('''
        space kv
        key k
        attributes list(int64) v
    ''')
    cfg.should().get('kv','k').equals(None)
    cfg.must().put('kv','k',{'v': [100, 200, 300]}).equals(True)
    cfg.should().get('kv','k').equals({'v': [100, 200, 300]})
    cfg.must().put('kv','k',{'v': []}).equals(True)
    cfg.should().get('kv','k').equals({'v': []})
    cfg.should().list_lpush('kv','k',{'v': 5}).equals(True)
    cfg.should().get('kv','k').equals({'v': [5]})
    cfg.should().list_rpush('kv','k',{'v': 6}).equals(True)
    cfg.should().get('kv','k').equals({'v': [5, 6]})
    cfg.should().list_lpush('kv','k',{'v': 4}).equals(True)
    cfg.should().get('kv','k').equals({'v': [4, 5, 6]})
    cfg.should().list_rpush('kv','k',{'v': 7}).equals(True)
    cfg.should().get('kv','k').equals({'v': [4, 5, 6, 7]})
    cfg.should().list_lpush('kv','k',{'v': 3}).equals(True)
    cfg.should().get('kv','k').equals({'v': [3, 4, 5, 6, 7]})
    cfg.should().list_rpush('kv','k',{'v': 8}).equals(True)
    cfg.should().get('kv','k').equals({'v': [3, 4, 5, 6, 7, 8]})
    cfg.should().list_lpush('kv','k',{'v': 2}).equals(True)
    cfg.should().get('kv','k').equals({'v': [2, 3, 4, 5, 6, 7, 8]})
    cfg.should().list_rpush('kv','k',{'v': 9}).equals(True)
    cfg.should().get('kv','k').equals({'v': [2, 3, 4, 5, 6, 7, 8, 9]})
    cfg.should().list_lpush('kv','k',{'v': 1}).equals(True)
    cfg.should().get('kv','k').equals({'v': [1, 2, 3, 4, 5, 6, 7, 8, 9]})
    cfg.should().list_rpush('kv','k',{'v': 0}).equals(True)
    cfg.should().get('kv','k').equals({'v': [1, 2, 3, 4, 5, 6, 7, 8, 9, 0]})
    cfg.must().delete('kv','k').equals(True)
    cfg.should().get('kv','k').equals(None)

def test_220_datatype_list_float(cfg):
    # space kv dimensions k, v (list(float)) key k auto 0 1
    cfg.should().rm_space('kv')
    cfg.must().add_space('''
        space kv
        key k
        attributes list(float) v 
    ''')
    cfg.should().get('kv','k').equals(None)
    cfg.must().put('kv','k',{'v': [100.0, 200.0, 300.0]}).equals(True)
    cfg.should().get('kv','k').equals({'v': [100.0, 200.0, 300.0]})
    cfg.must().put('kv','k',{'v': []}).equals(True)
    cfg.should().get('kv','k').equals({'v': []})
    cfg.should().list_lpush('kv','k',{'v': 5.0}).equals(True)
    cfg.should().get('kv','k').equals({'v': [5.0]})
    cfg.should().list_rpush('kv','k',{'v': 6.0}).equals(True)
    cfg.should().get('kv','k').equals({'v': [5.0, 6.0]})
    cfg.should().list_lpush('kv','k',{'v': 4.0}).equals(True)
    cfg.should().get('kv','k').equals({'v': [4.0, 5.0, 6.0]})
    cfg.should().list_rpush('kv','k',{'v': 7.0}).equals(True)
    cfg.should().get('kv','k').equals({'v': [4.0, 5.0, 6.0, 7.0]})
    cfg.should().list_lpush('kv','k',{'v': 3.0}).equals(True)
    cfg.should().get('kv','k').equals({'v': [3.0, 4.0, 5.0, 6.0, 7.0]})
    cfg.should().list_rpush('kv','k',{'v': 8.0}).equals(True)
    cfg.should().get('kv','k').equals({'v': [3.0, 4.0, 5.0, 6.0, 7.0, 8.0]})
    cfg.should().list_lpush('kv','k',{'v': 2.0}).equals(True)
    cfg.should().get('kv','k').equals({'v': [2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0]})
    cfg.should().list_rpush('kv','k',{'v': 9.0}).equals(True)
    cfg.should().get('kv','k').equals({'v': [2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0]})
    cfg.should().list_lpush('kv','k',{'v': 1.0}).equals(True)
    cfg.should().get('kv','k').equals({'v': [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0]})
    cfg.should().list_rpush('kv','k',{'v': 0.0}).equals(True)
    cfg.should().get('kv','k').equals({'v': [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 0.0]})
    cfg.must().delete('kv','k').equals(True)
    cfg.should().get('kv','k').equals(None)

def test_300_datatype_set_string(cfg):
    # space kv dimensions k, v (set(string)) key k auto 0 1
    cfg.should().rm_space('kv')
    cfg.must().add_space('''
        space kv
        key k
        attributes set(string) v
    ''')
    cfg.should().get('kv','k').equals(None)
    cfg.must().put('kv','k',{}).equals(True)
    cfg.must().set_add('kv','k',{'v': '4'}).equals(True)
    cfg.should().get('kv','k').equals({'v': set(['4'])})
    cfg.must().set_add('kv','k',{'v': '3'}).equals(True)
    cfg.should().get('kv','k').equals({'v': set(['3', '4'])})
    cfg.must().set_add('kv','k',{'v': '7'}).equals(True)
    cfg.should().get('kv','k').equals({'v': set(['3', '4', '7'])})
    cfg.must().set_add('kv','k',{'v': '5'}).equals(True)
    cfg.should().get('kv','k').equals({'v': set(['3', '5', '4', '7'])})
    cfg.must().set_add('kv','k',{'v': '2'}).equals(True)
    cfg.should().get('kv','k').equals({'v': set(['3', '2', '5', '4', '7'])})
    cfg.must().set_add('kv','k',{'v': '8'}).equals(True)
    cfg.should().get('kv','k').equals({'v': set(['3', '2', '5', '4', '7', '8'])})
    cfg.must().set_add('kv','k',{'v': '6'}).equals(True)
    cfg.should().get('kv','k').equals({'v': set(['3', '2', '5', '4', '7', '6', '8'])})
    cfg.must().set_add('kv','k',{'v': '1'}).equals(True)
    cfg.should().get('kv','k').equals({'v': set(['1', '3', '2', '5', '4', '7', '6', '8'])})
    cfg.must().set_add('kv','k',{'v': '9'}).equals(True)
    cfg.should().get('kv','k').equals({'v': set(['1', '3', '2', '5', '4', '7', '6', '9', '8'])})
    cfg.must().delete('kv','k').equals(True)
    cfg.should().get('kv','k').equals(None)

def test_310_datatype_set_int64(cfg):
    # space kv dimensions k, v (set(int64)) key k auto 0 1
    cfg.should().rm_space('kv')
    cfg.must().add_space('''
        space kv
        key k
        attributes set(int64) v
    ''')
    cfg.should().get('kv','k').equals(None)
    cfg.must().put('kv','k',{}).equals(True)
    cfg.must().set_add('kv','k',{'v': 4}).equals(True)
    cfg.should().get('kv','k').equals({'v': set([4])})
    cfg.must().set_add('kv','k',{'v': 3}).equals(True)
    cfg.should().get('kv','k').equals({'v': set([3, 4])})
    cfg.must().set_add('kv','k',{'v': 7}).equals(True)
    cfg.should().get('kv','k').equals({'v': set([3, 4, 7])})
    cfg.must().set_add('kv','k',{'v': 5}).equals(True)
    cfg.should().get('kv','k').equals({'v': set([3, 4, 5, 7])})
    cfg.must().set_add('kv','k',{'v': 2}).equals(True)
    cfg.should().get('kv','k').equals({'v': set([2, 3, 4, 5, 7])})
    cfg.must().set_add('kv','k',{'v': 8}).equals(True)
    cfg.should().get('kv','k').equals({'v': set([2, 3, 4, 5, 7, 8])})
    cfg.must().set_add('kv','k',{'v': 6}).equals(True)
    cfg.should().get('kv','k').equals({'v': set([2, 3, 4, 5, 6, 7, 8])})
    cfg.must().set_add('kv','k',{'v': 1}).equals(True)
    cfg.should().get('kv','k').equals({'v': set([1, 2, 3, 4, 5, 6, 7, 8])})
    cfg.must().set_add('kv','k',{'v': 9}).equals(True)
    cfg.should().get('kv','k').equals({'v': set([1, 2, 3, 4, 5, 6, 7, 8, 9])})
    cfg.must().delete('kv','k').equals(True)
    cfg.should().get('kv','k').equals(None)

def test_320_datatype_set_float(cfg):
    # space kv dimensions k, v (set(float)) key k auto 0 1
    cfg.should().rm_space('kv')
    cfg.must().add_space('''
        space kv
        key k
        attributes set(float) v
    ''')
    cfg.should().get('kv','k').equals(None)
    cfg.must().put('kv','k',{}).equals(True)
    cfg.must().set_add('kv','k',{'v': 4.0}).equals(True)
    cfg.should().get('kv','k').equals({'v': set([4.0])})
    cfg.must().set_add('kv','k',{'v': 3.0}).equals(True)
    cfg.should().get('kv','k').equals({'v': set([3.0, 4.0])})
    cfg.must().set_add('kv','k',{'v': 7.0}).equals(True)
    cfg.should().get('kv','k').equals({'v': set([3.0, 4.0, 7.0])})
    cfg.must().set_add('kv','k',{'v': 5.0}).equals(True)
    cfg.should().get('kv','k').equals({'v': set([3.0, 4.0, 5.0, 7.0])})
    cfg.must().set_add('kv','k',{'v': 2.0}).equals(True)
    cfg.should().get('kv','k').equals({'v': set([2.0, 3.0, 4.0, 5.0, 7.0])})
    cfg.must().set_add('kv','k',{'v': 8.0}).equals(True)
    cfg.should().get('kv','k').equals({'v': set([2.0, 3.0, 4.0, 5.0, 7.0, 8.0])})
    cfg.must().set_add('kv','k',{'v': 6.0}).equals(True)
    cfg.should().get('kv','k').equals({'v': set([2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0])})
    cfg.must().set_add('kv','k',{'v': 1.0}).equals(True)
    cfg.should().get('kv','k').equals({'v': set([1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0])})
    cfg.must().set_add('kv','k',{'v': 9.0}).equals(True)
    cfg.should().get('kv','k').equals({'v': set([1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0])})
    cfg.must().delete('kv','k').equals(True)
    cfg.should().get('kv','k').equals(None)

def test_410_datatype_map_string_string_microops(cfg):
    # space kv dimensions k, v (map(string,string)) key k auto 0 1
    cfg.should().rm_space('kv')
    cfg.must().add_space('''
        space kv
        key k
        attributes map(string,string) v
    ''')
    cfg.should().get('kv','k').equals(None)
    cfg.must().put('kv','k',{'v': {'1': 'nnn', '100': 'xzvwe', '50': '234*'}}).equals(True)
    cfg.should().get('kv','k').equals({'v': {'1': 'nnn', '100': 'xzvwe', '50': '234*'}})
    cfg.must().put('kv','k',{'v': {}}).equals(True)
    cfg.should().get('kv','k').equals({'v': {}})
    cfg.must().put('kv','k',{'v': {'1': 'nnn', '100': 'xzvwe', '50': '234*'}}).equals(True)
    cfg.should().get('kv','k').equals({'v': {'1': 'nnn', '100': 'xzvwe', '50': '234*'}})
    cfg.must().map_string_prepend('kv','k',{'v': {'KEY': '5'}}).equals(True)
    cfg.should().get('kv','k').equals({'v': {'1': 'nnn', '100': 'xzvwe', '50': '234*', 'KEY': '5'}})
    cfg.must().map_string_append('kv','k',{'v': {'KEY': '6'}}).equals(True)
    cfg.should().get('kv','k').equals({'v': {'1': 'nnn', '100': 'xzvwe', '50': '234*', 'KEY': '56'}})
    cfg.must().map_string_prepend('kv','k',{'v': {'KEY': '4'}}).equals(True)
    cfg.should().get('kv','k').equals({'v': {'1': 'nnn', '100': 'xzvwe', '50': '234*', 'KEY': '456'}})
    cfg.must().map_string_append('kv','k',{'v': {'KEY': '7'}}).equals(True)
    cfg.should().get('kv','k').equals({'v': {'1': 'nnn', '100': 'xzvwe', '50': '234*', 'KEY': '4567'}})
    cfg.must().map_string_prepend('kv','k',{'v': {'KEY': '3'}}).equals(True)
    cfg.should().get('kv','k').equals({'v': {'1': 'nnn', '100': 'xzvwe', '50': '234*', 'KEY': '34567'}})
    cfg.must().map_string_append('kv','k',{'v': {'KEY': '8'}}).equals(True)
    cfg.should().get('kv','k').equals({'v': {'1': 'nnn', '100': 'xzvwe', '50': '234*', 'KEY': '345678'}})
    cfg.must().map_string_prepend('kv','k',{'v': {'KEY': '2'}}).equals(True)
    cfg.should().get('kv','k').equals({'v': {'1': 'nnn', '100': 'xzvwe', '50': '234*', 'KEY': '2345678'}})
    cfg.must().map_string_append('kv','k',{'v': {'KEY': '9'}}).equals(True)
    cfg.should().get('kv','k').equals({'v': {'1': 'nnn', '100': 'xzvwe', '50': '234*', 'KEY': '23456789'}})
    cfg.must().map_string_prepend('kv','k',{'v': {'KEY': '1'}}).equals(True)
    cfg.should().get('kv','k').equals({'v': {'1': 'nnn', '100': 'xzvwe', '50': '234*', 'KEY': '123456789'}})
    cfg.must().map_string_append('kv','k',{'v': {'KEY': '0'}}).equals(True)
    cfg.should().get('kv','k').equals({'v': {'1': 'nnn', '100': 'xzvwe', '50': '234*', 'KEY': '1234567890'}})
    cfg.must().map_string_append('kv','k',{'v': {'1': 'nnn', '100': 'xzvwe', '50': '234*', 'KEY': '1234567890'}}).equals(True)
    cfg.should().get('kv','k').equals({'v': {'1': 'nnnnnn', '100': 'xzvwexzvwe', '50': '234*234*', 'KEY': '12345678901234567890'}})
    cfg.must().delete('kv','k').equals(True)
    cfg.should().get('kv','k').equals(None)

