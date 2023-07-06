import pandas as pd
import numpy as np
from sdv.metadata import SingleTableMetadata
from sdv.evaluation.single_table import get_column_plot
def select_from_db(schema:'str', 
                   table:'str', 
                   engine: 'sqlalchemy.engine.base.Engine', 
                   fields:'str'='*', 
                   limit:'str'='LIMIT 30000'):
    """
    
    Назначение:
    
    Выполнение sql-запроса для чтения таблицы в соответствии с аргументами
    
    Аргументы:
    
    schema: схема в БД
    
    table: таблица в БД
    
    engine: sqlalchemy engine для нужной БД
    
    fields: запрашиваемые поля таблицы. По умолчанию запрашиваются все поля. Аргумент
    задается как выражение после ключевого слова SELECT
    
    limit: sql выражение для ограничения количества строк в результате. По умолчанию: LIMIT 30000
    Для другого количества меняется цифра, для отсутствия ограничения передается пустая строка
    
    Результат:
    
    Выгруженная из БД таблица
    
    Возвращаемое значение:
    
    pandas dataframe с результатом запроса
    """
    return pd.read_sql("""SELECT {}
                         FROM {}.{}
                         {}
                    """.format(fields, schema, table, limit), engine)

def read_sql(config:'dict', 
             schema:'str', 
             engine:'sqlalchemy.engine.base.Engine'):
    """
    
    Назначение:
    
    Последовательное чтение заранее определенной таблицы из нужной БД
    В теле функции для каждой таблицы и набора параметров вызывается функция select_from_db
    
    Аргументы:
    
    config: словарь типа таблица: параметры, где параметры - None (арг по умолч.)
    или словарь, определяющий пользовательские значения аргумента/аргументов
    
    schema: схема в БД
    
    engine: sqlalchemy engine для нужной БД
    
    Результат:
    
    Выгруженная из БД схема связанных таблиц
    
    Возвращаемое значение:
    
    Словарь типа название_таблицы: соответствующий_pandas_dataframe
    
    """
    df = {}
    for table, args in config.items():
        if args is None:
            df[table] = select_from_db(schema, table, engine)
        else:
            try:
                fields = args['fields']
            except:
                fields = '*'
            try:
                limit = args['limit']
            except:
                limit = 'LIMIT 30000'
            df[table] = select_from_db(schema, table, engine, fields, limit)
    return df

def read_csv(folder:'str', 
             same_names:'bool', 
             pkeys:'dict'={}, 
             names:'dict'={}, 
             dates:'dict'={}):
    """
    
    Назначение:
    
    Последовательное чтение csv файлов в соответствии с аргументами. Если имена
    таблиц и файлов совпадают (same_names=True), для определения таблиц
    используется аргумент pkeys. Если нет (same_names=False), то используется
    аргумент names. 
    
    Аргументы:
    
    folder: папка с csv-файлами
    
    same_names: флаг совпадения имен файлов и имен таблиц
    
    pkeys: словарь типа имя_таблицы: первичный_ключ
    
    names: словарь типа имя_таблицы: имя_csv_файла
    
    dates: словарь типа имя_таблицы: поля, где поля - список полей, содержащих даты
    
    Результат:
    
    Прочитанная из csv-файлов схема связанных таблиц
    
    Возвращаемое значение:
    
    Словарь типа название_таблицы: соответствующий_pandas_dataframe
    """
    df = {}
    if same_names:
        for table, _ in pkeys.items():
            if table in dates:
                df[table] = pd.read_csv(folder + table + '.csv', parse_dates=dates[table])
            else:
                df[table] = pd.read_csv(folder + table + '.csv')
    else:
        for table, csv in names.items():
            if table in dates:
                df[table] = pd.read_csv(folder + csv, parse_dates=dates[table])
            else:
                df[table] = pd.read_csv(folder + csv)
    return df

def print_len(df:'dict'):
    """
    
    Назначение:
    
    Вывод длины значений словаря
    
    Аргументы:
    
    df - ловарь типа название_таблицы: соответствующий_pandas_dataframe
    
    Результат:
    
    Вывод списка из таблиц и их размеров
    
    Возвращаемое значение:
    
    Отсутствует
    """
    for k, v in df.items():
        print(k, len(v))

def correct_relations(df:'dict', 
                      config:'dict'):
    """
    
    Назначение:
    
    Коррекция отношений между таблицами для отсутствия соответствующих ошибок в 
    дальнейшем (устранение значений child_FK, которые не присутствуют в main_PK) 
    
    Аргументы:
    
    df: словарь типа название_таблицы: соответствующий_pandas_dataframe
    
    config: словарь типа таблицы: ключи, где
        таблицы - кортеж имен связанных таблиц, 0-ой элемент основная таблица,
        1-ый - дочерняя. В случае наличия 2 и более связей между таблицами,
        необходимо добавить 2-ой элемент (или третий по счету) со случайными 
        значением для обхода уникальности ключей словаря
        
        ключи - кортеж типа
        (поле_первичный_ключ_основной_таблицы, поле_внешний_ключ_дочерней_таблицы)
    
    Результат:
    
    Исправленные связи между таблицами
    
    Возвращаемое значение:
    
    Отсутствует
    """
    for k, v in config.items():
        df[k[1]] = df[k[1]][df[k[1]][v[1]].isin(df[k[0]][v[0]])]

def date_corrections_for_dataframe(df:'dict'):
    """
    
    Назначение:
    
    Приведение полей с датой каждой таблицы к оптимальному для дальнейших 
    действий виду (удаление таймзоны)
    
    
    Аргументы:
    
    df: словарь из pandas dataframe для каждой таблицы
    
    Результат:
    
    Оптимальный формат дат во всех таблицах
    
    Возвращаемое значение:
    
    pandas dataframe
    
    """
    for k, v in df.items():
            for index, column in v.dtypes.items():
                if isinstance(column, pd.core.dtypes.dtypes.DatetimeTZDtype):
                    v[index] = pd.to_datetime(v[index]).dt.tz_localize(None)
    return df

def detect_metadata(df:'dict', 
                    meta:'sdv.metadata.multi_table.MultiTableMetadata'):
    """
    
    Назначение:
    
    Автоматическое определение начальных метаданных для заранее заданных таблиц
    
    Аргументы:
    
    df: словарь типа название_таблицы: соответствующий_pandas_dataframe
    
    meta: sdv multi_table_metadata
    
    Результат:
    
    Созданный и заполненный начальной информацией объект метаданных
    
    Возвращаемое значение:
    
    Отсутствует
   
    """
    for k, v in df.items():
        meta.detect_table_from_dataframe(
        table_name=k,
        data=v
    )

def date_correction_for_metadata(meta:'sdv.metadata.multi_table.MultiTableMetadata', 
                                 table:'str', 
                                 column:'str'):
    """
    
    Назначение:
    
    Добавление в объект метадаты определения полей, содержащих даты
    
    Аргументы:
    
    meta: sdv multi_table_metadata
    
    table: имя таблицы
    
    column: поле таблицы
    
    Результат:
    
    Обновленный объект метаданных - тип datetime и формат 
    для одного поля одной таблицы
    
    Возвращаемое значение:
    
    Отсутствует
    
    """
    meta.update_column(
        table_name=table,
        column_name=column,
        sdtype='datetime',
        datetime_format='%Y-%m-%d %H:%M:%S'
)
def add_id_to_metadata(meta:'sdv.metadata.multi_table.MultiTableMetadata', 
                       table:'str', 
                       column:'str',
                       regex:'str'=None):
    """
    
    Назначение:
    
    Добавление в объект метадаты определения поля, которое 
    должно иметь тип id
    
    Аргументы:
    
    meta: sdv multi_table_metadata
    
    table: имя таблицы
    
    column: поле таблицы
    
    Результат:
    
    Обновленный объект метаданных - тип id для одного 
    поля одной таблицы
    
    Возвращаемое значение:
    
    Отсутствует
    """
    if regex:
        meta.update_column(
            table_name=table,
            column_name=column,
            sdtype='id',
            regex_format=regex)
    else:
        meta.update_column(
            table_name=table,
            column_name=column,
            sdtype='id')
            
def add_relation_to_metadata(meta:'sdv.metadata.multi_table.MultiTableMetadata', 
                             parent:'str', 
                             child:'str', 
                             pk:'str', 
                             fk:'str'):
    """
    
    Назначение:
    
    Добавление в объект метадаты определения связей между таблицами
    
    Аргументы:
    
    meta: sdv multi_table_metadata
    
    parent: имя основной таблицы
    
    child: имя дочерней таблицы
    
    pk: поле, представляющее первичный ключ в основной таблице
    
    fk: поле, представляющее первичный ключ в дочерней таблице
    
    Результат:
    
    Обновленный объект метаданных - отношение между
    родительской и дочерней таблицей
    
    Возвращаемое значение:
    
    Отсутствует
    """
    meta.add_relationship(
    parent_table_name=parent,
    child_table_name=child,
    parent_primary_key=pk,
    child_foreign_key=fk
)
def add_pk_to_metadata(meta:'sdv.metadata.multi_table.MultiTableMetadata', 
                       table:'str', 
                       column:'str'):
    """
    
    Назначение:
    
    Добавление в объект метадаты определения первичных ключей
    
    Аргументы:
    
    meta: sdv multi_table_metadata
    
    table: имя таблицы
    
    column: поле таблицы
    
    Результат:
    
    Обновленный объект метаданных - таблице добавлен
    первичный ключ
    
    Возвращаемое значение:
    
    Отсутствует
    """
    meta.set_primary_key(
    table_name=table,
    column_name=column
)
def add_other_type_to_metadata(meta:'sdv.metadata.multi_table.MultiTableMetadata', 
                               table:'str', 
                               column:'str', 
                               column_type:'str'):
    """
    
    Назначение:
    
    Добавление в объект метадаты определения для кастомных типов полей таблицы
    
    Аргументы:
    
    meta: sdv multi_table_metadata
    
    table: имя таблицы
    
    column: поле таблицы
    
    column_type: тип поля (определенные в sdv или из пакета Faker)
    
    Результат:
    
    Обновленный объект метаданных - кастомный тип для поля таблицы
    
    Возвращаемое значение:
    
    Отсутствует
    
    """
    meta.update_column(
        table_name=table,
        column_name=column,
        sdtype=column_type
)

def main_metadata_corrections(df:'dict', 
                              metadata:'sdv.metadata.multi_table.MultiTableMetadata', 
                              pkeys:'dict', 
                              relations_and_keys:'dict',
                              regex:'dict'={}):
    """
    
    Назначение:
    
    Выполнение основных корректировок объекта метадаты для всех заданных таблиц,
    ключей и отношений
    В теле функции для каждой таблицы и соответствующих полей вызываются функции:
    add_id_to_metadata - для первичных и внешних ключей
    date_correction_for_metadata - для полей, содержащих даты
    add_pk_to_metadata - для каждой таблицы
    
    Аргументы:
    
    df: словарь из датафреймов
    
    metadata: sdv multi_table_metadata
    
    pkeys: словарь типа имя_таблицы: первичный_ключ
    
    relations_and_keys: словарь типа таблицы: ключи, где
    
        таблицы - кортеж имен связанных таблиц, 0-ой элемент основная таблица,
        1-ый - дочерняя. В случае наличия 2 и более связей между таблицами,
        необходимо добавить 2-ой элемент (или третий по счету) со случайными 
        значением для обхода уникальности ключей словаря
        
        ключи - кортеж типа
        (поле_первичный_ключ_основной_таблицы, поле_внешний_ключ_дочерней_таблицы)
    
    regex: словарь типа имя_таблицы: поле_и_регулярка, где
        поле_и_регулярка - словарь типа имя_поля: регулярка

    Результат:
    
    Обновленный и полностью готовый к добавлению связей объект метаданных
    
    Возвращаемое значение:
    
    Отсутствует
    """
    for k, v in pkeys.items():
        if k in regex.keys() and v in regex[k].keys():
            add_id_to_metadata(metadata, k, v, regex[k][v])
        else:
            add_id_to_metadata(metadata, k, v)
            
    for k, v in relations_and_keys.items():
        if k[1] in regex.keys() and v[1] in regex[k[1]].keys():
            add_id_to_metadata(metadata, k[1], v[1], regex[k[1]][v[1]])
        else:
            add_id_to_metadata(metadata, k[1], v[1])

    for k, v in df.items():
            for index, column in v.dtypes.items():
                if np.issubdtype(column, np.datetime64):
                    date_correction_for_metadata(metadata, k, index)

    for table, key in pkeys.items():
        add_pk_to_metadata(metadata, table, key)

def hide_not_key_fields(metadata:'sdv.metadata.multi_table.MultiTableMetadata', 
                        other_important_fields:'dict',
                        regex:'dict'={}):
    """
    
    Назначение:
    
    Добавление в объект метадаты определения для оставшихся выбранных полей, 
    выбранных таблиц, которые должны иметь тип id
    
    В теле функции вызывается для определенных таблицы и полей 
    вызывается add_id_to_metadata
    Аргументы:
    
    meta: sdv multi_table_metadata
    
    other_important_fields: словарь типа имя_таблицы: поле_таблицы
    
    regex: словарь типа имя_таблицы: поле_и_регулярка, где
        поле_и_регулярка - словарь типа имя_поля: регулярка
    
    Результат:
    
    Обновленный объект метаданных - дополнительные неключевые поля типа id
    
    Возвращаемое значение:
    
    Отсутствует
   
    """
    for table, field in other_important_fields.items():
        if table in regex.keys() and field in regex[table].keys():
            add_id_to_metadata(metadata, table, field, regex[table][field])
        else:
            add_id_to_metadata(metadata, table, field)

def add_custom_type(metadata:'sdv.metadata.multi_table.MultiTableMetadata', 
                    other_types_fields:'dict'):
    """
    
    Назначение:
    
    Добавление в объект метадаты определения для выбранных полей выбранных таблиц, 
    которым хотим сопоставить кастомный тип (из sdv или Faker)
    
    Аргументы:
    
    metadata: sdv multi_table_metadata
    
    other_types_fields: словарь типа имя_таблицы: поле_и_тип, где
        поле_и_тип - список типа [имя_поля, тип]
    
    Результат:
    
    Обновленный объект метаданных - кастомные типы для всех определенных полей
    
    Возвращаемое значение:
    
    Отсутствует
    """
    for table, value in other_types_fields.items():
        add_other_type_to_metadata(metadata, table, value[0], value[1])

def add_relations(metadata:'sdv.metadata.multi_table.MultiTableMetadata', 
                  relations_and_keys:'dict'):
    """
    
    Назначение:
    Добавление в объект метадаты всех определенных отношений между таблицами
    
    Аргументы:
    
    metadata: sdv multi_table_metadata
    
    relations_and_keys: словарь типа таблицы: ключи, где
    
        таблицы - кортеж имен связанных таблиц, 0-ой элемент основная таблица,
        1-ый - дочерняя. В случае наличия 2 и более связей между таблицами,
        необходимо добавить 2-ой элемент (или третий по счету) со случайными 
        значением для обхода уникальности ключей словаря
        
        ключи - кортеж типа
        (поле_первичный_ключ_основной_таблицы, поле_внешний_ключ_дочерней_таблицы)
    
    Результат:
    
    Обновленный объект метаданных - все определенные в конфиге отношения
    
    Возвращаемое значение:
    
    Отсутствует

    """
    for tables, keys in relations_and_keys.items():
        add_relation_to_metadata(metadata, tables[0], tables[1], keys[0], keys[1])

def save_metadata(metadata:'sdv.metadata.multi_table.MultiTableMetadata', 
                  mode:'str', 
                  name:'str'=None):
    """
    
    Назначение:
    
    Сохранение объекта метадаты в словарь или json
    
    Аргументы:
    
    metadata: sdv multi_table_metadata
    
    mode: тип сохранения - 'dict' или 'json'
    
    name: None по умолч, путь к json файлу в случе mode='json' 
    
    Результат:
    
    Сохраненные в словарь метаданные
    
    Возвращаемое значение:
    
    Словарь с метадатой или сохраненный json-файл
    
    """
    if mode.lower() == 'dict':
        return metadata.to_dict()
    elif mode.lower() == 'json':
        metadata.save_to_json(filepath='{}.json'.format(name))
        

def add_constraint(model:'sdv.multi_table.hma.HMASynthesizer', 
                   constraint_class:'str', 
                   table:'str', 
                   parameters:'list'):
    """
    
    Назначение:
    
    Добавление ограничений к объекту synthesizer
    
    Аргументы:
    
    model: объекту sdv synthesizer
    
    constraint_class: класс ограничения - 'Inequality' 'FixedCombinations'
    
    table: имя таблицы, к которой применяются ограничения
    
    parameters: список параметров, соответствующих типу ограничения
    
    Результат:
    
    Модель дополнена ограничением
    
    Возвращаемое значение:
    
    Отсутствует
    
    """
    constraints_list = []
    if constraint_class.lower() == 'inequality':
        constraint = {
            'constraint_class': 'Inequality',
            'table_name': table,
            'constraint_parameters': {
            'low_column_name': parameters[0],
            'high_column_name': parameters[1]
        }
    }
        constraints_list.append(constraint)
    elif constraint_class.lower() == 'fixedcombinations':
        constraint = {
            'constraint_class': 'Inequality',
            'table_name': table,
            'constraint_parameters': {
            'column_names': parameters
        }
    }
        constraints_list.append(constraint)
    model.add_constraints(constraints=constraints_list)

def plot_field_distribution(real_data:'dict', 
                            gen_data:'dict', 
                            metadata:'sdv.multi_table.hma.HMASynthesizer',
                            table:'str',
                            field:'str'):
    """
    
    Назначение:
    
    Создание графика, отражающего распределение переменной с типом
    numerical, categorical, datetime, bool
    
    Аргументы:
    
    real_data: исходные данные - словарь из датафреймов
    
    gen_data: синтетические данные - словарь из датафреймов
    
    metadata: MultiTableMetadata
    
    table: имя таблицы
    
    field: имя поля (переменной)
    
    Результат:
    
    Построенный интерактивный график распределения переменной
    
    Возвращаемое значение:
    
    Отсутствует
    """
    meta_dict = save_metadata(metadata, 'dict')
    meta_dict = SingleTableMetadata.load_from_dict(meta_dict['tables'][table])
    try:
        fig = get_column_plot(
            real_data=real_data[table],
            synthetic_data=gen_data[table],
            column_name=field,
            metadata=meta_dict
        )
        fig.show()
    except ValueError:
        print('Неверный тип поля. Возможны: categorical, datetime, numeric, bool')