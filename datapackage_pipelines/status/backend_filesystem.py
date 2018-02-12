import os
import codecs
from json.decoder import JSONDecodeError
from datapackage_pipelines.utilities.extended_json import json

DPP_DIRNAME = os.environ.get('DPP_DB_DIRNAME', '.dpp')


class FilesystemBackend(object):

    KIND = 'filesystem'

    def __init__(self):
        os.makedirs(DPP_DIRNAME, exist_ok=True)

    @staticmethod
    def fn(pipeline_id):
        pipeline_id = codecs.encode(pipeline_id.encode('utf8'), 'base64').decode('ascii').replace('\n', '')
        return os.path.join(DPP_DIRNAME, pipeline_id)

    def get_status(self, pipeline_id):
        try:
            with open(self.fn(pipeline_id)) as f:
                return json.load(f)
        except FileNotFoundError:
            pass
        except JSONDecodeError:
            pass

    def set_status(self, pipeline_id, status):
        fn = self.fn(pipeline_id)
        with open(fn+'.tmp', 'w') as f:
            json.dump(status, f)
        os.rename(fn+'.tmp', fn)

    def del_status(self, pipeline_id):
        try:
            os.unlink(self.fn(pipeline_id))
        except FileNotFoundError:
            pass

    def register_pipeline_id(self, pipeline_id):
        pass

    def deregister_pipeline_id(self, pipeline_id):
        self.del_status(pipeline_id)

    def reset(self):
        for p in self.all_pipeline_ids():
            self.del_status(p)

    def all_pipeline_ids(self):
        all_ids = sorted(os.listdir(DPP_DIRNAME))
        all_ids = [
            codecs.decode(_id.encode('utf8'), 'base64').decode('utf8')
            for _id in all_ids
        ]
        return all_ids

    def all_statuses(self):
        return [self.get_status(_id)
                for _id in self.all_pipeline_ids()]
