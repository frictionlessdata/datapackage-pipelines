# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import oki


def test_what_do_we_want():
    '''Tests that what we want for open data is correct.'''
    assert 'data raw' in oki.what_we_want()
