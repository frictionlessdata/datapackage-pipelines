import sys
import shutil
import gzip

from datapackage_pipelines.wrapper import ingest

params, _, _ = ingest()

load_from = params['load-from']

shutil.copyfileobj(gzip.open(load_from, "rt"), sys.stdout)
