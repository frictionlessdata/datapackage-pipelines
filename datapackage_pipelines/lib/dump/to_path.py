import os
import shutil

from datapackage_pipelines.lib.dump.dumper_base import FileDumper


class PathDumper(FileDumper):

    def initialize(self, params):
        super(PathDumper, self).initialize(params)
        self.out_path = params.get('out-path', '.')
        self.add_filehash_to_path = params.get('add-filehash-to-path', False)
        PathDumper.__makedirs(self.out_path)

    def write_file_to_output(self, filename, path):
        path = os.path.join(self.out_path, path)
        # Avoid rewriting existing files
        if self.add_filehash_to_path and os.path.exists(path):
            return
        path_part = os.path.dirname(path)
        PathDumper.__makedirs(path_part)
        shutil.copy(filename, path)
        return path

    @staticmethod
    def __makedirs(path):
        os.makedirs(path, exist_ok=True)


if __name__ == '__main__':
    PathDumper()()
