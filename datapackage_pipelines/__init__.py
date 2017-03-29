# -*- coding: utf-8 -*-
import io
import os
import logging

from .specs import pipelines
from .manager import execute_pipeline

VERSION_FILE = os.path.join(os.path.dirname(__file__), 'VERSION')

__version__ = io.open(VERSION_FILE, encoding='utf-8').readline().strip()

# __all__ = ['initialize_pipeline']

runner = '%-32s' % 'Main'
logging.basicConfig(level=logging.DEBUG,
                    format="%(levelname)-8s:"+runner+":%(message)s")
logging.root.setLevel(logging.DEBUG)
