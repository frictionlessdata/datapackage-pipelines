import tabulator.parsers.csv
import tabulator.config


#
class TabulatorTxtParser(tabulator.parsers.csv.CSVParser):
    """
    A custom Tabulator parser which allows to parse non-tabular textual data (e.g. Html).
    It splits the content into rows by new-lines, each row has a single "data" column containing the row's content
    """

    def __iter_extended_rows(self):
        for number, line in enumerate(self.__chars, start=1):
            yield number, None, [line]


def register_tabulator_txt_parser():
    tabulator.config.PARSERS["txt"] = "datapackage_pipelines.utilities.tabulator_txt_parser.TabulatorTxtParser"
