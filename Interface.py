class Interface:

    def __init__(self, dataset):
        self._dataset = dataset

    def params_type(self):
        f = input("Choose parameters:\n1. Default\n2. Optional\n")
        if f == "2": return self.enter_params()
        else: return None

    def enter_params(self):
        list = []
        check = True
        while check:
            print("\nEnter parameter (if you want to stop print stop):")
            print(self._dataset.columns.tolist())
            name = input("\n")
            if (name == "Company" or name == "Product" or name == "TypeName" or name == "Inches" or name == "ScreenResolution" or name == "Cpu"
                or name == "Ram" or name == "Memory" or name == "Gpu" or name == "OpSys" or name == "Weight" or name == "Price_euros"):
                str = self.get_columns(name)
                if str == None:
                    return None
                else:
                    list.append(str)
                    check = True

            elif name == "stop":
                check = False

            elif name == "default":
                return None

            else:
                print("Wrong parameter. Try again.\n\n")
                check = True

        return list

    def get_columns(self,name):
        print("\nEnter value of parameter:")
        print(self._dataset[name].unique().tolist())
        value = input("\n")
        if value == "default":
            return None
        else:
            return name,value
