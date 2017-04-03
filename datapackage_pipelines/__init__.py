# -*- coding: utf-8 -*-
import io
import os

from .specs import pipelines
from .manager import execute_pipeline

VERSION_FILE = os.path.join(os.path.dirname(__file__), 'VERSION')

__version__ = io.open(VERSION_FILE, encoding='utf-8').readline().strip()
