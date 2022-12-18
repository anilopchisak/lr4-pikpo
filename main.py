from DataProcessor import DataProcessor
from DatabaseProcessor import DatabaseProcessor
from sql_api import *

DATASOURCE = "laptop_dataset.csv"
DB_URL = 'sqlite:///db.db'

if __name__ == '__main__':
    dp = DataProcessor(DATASOURCE)
    dp.read()
    params = dp.run()
    result = dp.print_result()

    # Работа с БД
    if result is not None:
        db_connector = DatabaseProcessor(DB_URL, params)   # получаем объект соединения
        db_connector.connect()
        insert_into_source_files(db_connector, DATASOURCE)                # сохраняем в БД информацию о файле с набором данных
        print(select_all_from_source_files(db_connector))                 # вывод списка всех обработанных файлов
        insert_rows_into_processed_data(db_connector, result, DATASOURCE) # записываем в БД результат
        # Завершаем работу с БД
        db_connector.close()
