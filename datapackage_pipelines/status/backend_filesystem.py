import os
import codecs
import ujson


class FilesystemBackend(object):

    KIND = 'filesystem'

    def __init__(self, root_dir='.'):
        dpp_dirname = os.environ.get('DPP_DB_DIRNAME', '.dpp')
        self.base_dir = os.path.join(root_dir, dpp_dirname)
        os.makedirs(self.base_dir, exist_ok=True)

    def fn(self, pipeline_id):
        pipeline_id = codecs.encode(pipeline_id.encode('utf8'), 'base64').decode('ascii').replace('\n', '')
        return os.path.join(self.base_dir, pipeline_id)

    def get_status(self, pipeline_id):
        try:
            with open(self.fn(pipeline_id)) as f:
                return ujson.load(f)
        except FileNotFoundError:
            pass
        except ValueError:
            pass

    def set_status(self, pipeline_id, status):
        fn = self.fn(pipeline_id)
        with open(fn+'.tmp', 'w') as f:
            ujson.dump(status, f)
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
        all_ids = sorted(os.listdir(self.base_dir))
        all_ids = [
            codecs.decode(_id.encode('utf8'), 'base64').decode('utf8')
            for _id in all_ids
        ]
        return all_ids

    def all_statuses(self):
        return [self.get_status(_id)
                for _id in self.all_pipeline_ids()]
