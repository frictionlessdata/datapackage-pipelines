from datapackage_pipelines.lib.dump.file_formats import CSVFormat, get_path
import os
import openpyxl


class XLSXFormat(CSVFormat):

    def prepare_resource(self, resource):
        super(XLSXFormat, self).prepare_resource(resource)
        basename, _ = os.path.splitext(get_path(resource))
        resource['path'] = basename + '.xlsx'
        resource['format'] = 'xlsx'

    def initialize_file(self, file, headers):
        self.file = file
        wb = openpyxl.Workbook()
        wb.active.append(headers)
        return wb

    def write_transformed_row(self, writer, transformed_row, fields):
        writer.active.append(list(transformed_row.values()))

    def finalize_file(self, writer):
        writer.save(self.file.name)
