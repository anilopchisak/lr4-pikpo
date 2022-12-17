import pandas

class DataHandler:
    def __init__(self, dataset, list):
        self._dataset = dataset
        self._list = list

    def start_proc(self):
        _dataset_filtered = self._dataset.copy()
        i = 0
        new_list = []
        tmp = []
        for n in range (len(self._list)):
            if not tmp:
                tmp.append(self._list[i][0])
                tmp.append(self._list[i][1])
            else:
                if self._list[n][0] == tmp[0]:
                    tmp.append(self._list[n][1])
                else:
                    new_list.append(tmp)
                    tmp = []
                    tmp.append(self._list[n][0])
                    tmp.append(self._list[n][1])
        new_list.append(tmp)
        _dataset_filtered = self.filter(new_list, _dataset_filtered)
        return _dataset_filtered

    def filter(self, list, dataset):
        dataset_res = dataset.copy()
        for i in range (len(list)):
            if (len(list[i]) > 2):
                list_dataset = []
                for j in range (1,(len(list[i]))):
                    if i == 0:
                        dataset_copy = dataset.copy()
                    else:
                        dataset_copy = dataset_res.copy()
                    dataset_copy = dataset_copy.loc[dataset_copy[list[i][0]] == list[i][j]]
                    list_dataset.append(dataset_copy)
                dataset_res = list_dataset[0]
                for j in range(1,len(list_dataset)):
                    dataset_res = pandas.concat([dataset_res, list_dataset[j]])
            else:
                dataset_res = dataset_res.loc[dataset_res[list[i][0]] == list[i][1]]
        return dataset_res