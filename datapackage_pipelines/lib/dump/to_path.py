import os
import shutil

from datapackage_pipelines.lib.dump.dumper_base import CSVDumper


class PathDumper(CSVDumper):

    def initialize(self, params):
        super(PathDumper, self).initialize(params)
        self.out_path = params.get('out-path', '.')
        PathDumper.__makedirs(self.out_path)

    def write_file_to_output(self, filename, path):
        path = os.path.join(self.out_path, path)

        path_part = os.path.dirname(path)
        PathDumper.__makedirs(path_part)

        shutil.move(filename, path)

    @staticmethod
    def __makedirs(path):
        if not os.path.exists(path):
            os.makedirs(path)


PathDumper()()
