import pandas
from Interface import Interface
from DataHandler import DataHandler

class DataProcessor:

    def __init__(self, datasource):
        self._datasource = datasource
        self._dataset = None
        self.result = None
        self.separator = ','

    def read(self):
        try:
            self._dataset = pandas.read_csv(self._datasource, sep=self.separator, header='infer', names=None, encoding="utf-8", index_col="laptop_ID")
            col_names = self._dataset.columns
            if len(col_names) > 1:
                print(f'Columns read: {col_names} using separator {self.separator}')
                return True
        except Exception as e:
            print(e)
        return False

    def run(self):
        list = []
        interface = Interface(self._dataset)
        list = interface.params_type()
        if list == None:
            self.result = self._dataset
        else:
            list.sort()
            handler = DataHandler(self._dataset, list)
            self.result = handler.start_proc()
        return list

    def print_result(self):
        print(f'Running CSV-file processor!\n', self.result)
        return self.result
