import tabulator.parsers.csv
import tabulator.config


# a tabulator parser which allows to parse non-tabular textual data - by splitting it into rows with a single data column
class TabulatorTxtParser(tabulator.parsers.csv.CSVParser):

    def __iter_extended_rows(self):
        # yield (0, None, ["data"])
        for number, line in enumerate(self.__chars, start=1):
            yield (number, None, [{"data": line}])


def register_tabulator_txt_parser():
    tabulator.config.PARSERS["txt"] = "datapackage_pipelines.utilities.tabulator_txt_parser.TabulatorTxtParser"
