import zipfile

from datapackage_pipelines.lib.dump.dumper_base import FileDumper


class ZipDumper(FileDumper):

    def initialize(self, params):
        super(ZipDumper, self).initialize(params)
        out_filename = open(params['out-file'], 'wb')
        self.zip_file = zipfile.ZipFile(out_filename, 'w')

    def write_file_to_output(self, filename, path):
        self.zip_file.write(filename, arcname=path,
                            compress_type=zipfile.ZIP_DEFLATED)

    def finalize(self):
        self.zip_file.close()
        super(ZipDumper, self).finalize()


ZipDumper()()
