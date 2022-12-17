#from pandas import DataFrame
from datetime import *

"""
    В данном модуле реализуется API (Application Programming Interface)
    для взаимодействия с БД с помощью объектов-коннекторов.
    
    ВАЖНО! Методы должны быть названы таким образом, чтобы по названию
    можно было понять выполняемые действия.
"""
# Вывод списка обработанных файлов с сортировкой по дате
def select_all_from_source_files(connector):
    # connector.start_transaction()  # начинаем выполнение запросов
    query = f'SELECT * FROM tracking ORDER BY processed DESC'
    result = connector.execute(query).fetchall()
    # connector.execute(query)
    # result = connector._cursor.fetchall()
    # connector.end_transaction()  # завершаем выполнение запросов
    return result

# Вставка в таблицу обработанных файлов
def insert_into_source_files(connector, filename):
    now = datetime.now() # текущая дата и время
    date_time = now.strftime("%Y-%m-%d %H:%M:%S")   # преобразуем в формат SQL
    # connector.start_transaction()
    query = f'INSERT INTO tracking (filename, processed) VALUES (\'{filename}\',\'{date_time}\')' #, \'{connector._params}\'    , params   	params varchar(255) NOT NULL
    result = connector.execute(query)
    # connector.end_transaction()
    return result

# Вставка строк из DataFrame в БД
def insert_rows_into_processed_data(connector, dataframe, filename: str):
    rows = dataframe.to_dict('records')
    connector.start_transaction()
    files_list = select_all_from_source_files(connector)
    last_file_id = files_list[0][0]
    for row in rows:
        connector.execute(f'INSERT INTO dataset (Company, Product, TypeName, Inches, '
                          f'ScreenResolution, Cpu, Ram, Memory, Gpu, OpSys, Weight, Price_euros, track_id) '
                          f'VALUES (\'{row["Company"]}\', \'{row["Product"]}\', \'{row["TypeName"]}\','
                          f' \'{row["Inches"]}\', \'{row["ScreenResolution"]}\', \'{row["Cpu"]}\', '
                          f'\'{row["Ram"]}\', \'{row["Memory"]}\', \'{row["Gpu"]}\','
                          f' \'{row["OpSys"]}\', \'{row["Weight"]}\', \'{row["Price_euros"]}\', {last_file_id})')
    connector.end_transaction()
