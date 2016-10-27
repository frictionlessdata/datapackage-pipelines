import shelve


class ShelveBackend(object):

    ALL_PIPELINES_KEY = 'all-pipelines'

    def __init__(self):
        self.dbname = '.dppdb'

    def get(self):
        return shelve.open(self.dbname)

    def get_status(self, pipeline_id):
        with self.get() as db:
            if pipeline_id in db:
                return db[pipeline_id]

    def set_status(self, pipeline_id, status):
        with self.get() as db:
            db[pipeline_id] = status
            db.sync()

    def register_pipeline_id(self, pipeline_id):
        with self.get() as db:
            if self.ALL_PIPELINES_KEY not in db:
                all_pipelines = set()
            else:
                all_pipelines = db[self.ALL_PIPELINES_KEY]
            all_pipelines.add(pipeline_id)
            db[self.ALL_PIPELINES_KEY] = all_pipelines
            db.sync()

    def reset(self):
        with self.get() as db:
            db[self.ALL_PIPELINES_KEY] = set()
            db.sync()

    def all_statuses(self):
        with self.get() as db:
            all_ids = sorted(db[self.ALL_PIPELINES_KEY])
            return [db[_id] for _id in all_ids]
