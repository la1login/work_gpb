import pandas as pd
import time
from sdv.single_table import GaussianCopulaSynthesizer, CTGANSynthesizer, TVAESynthesizer, CopulaGANSynthesizer
from sdv.evaluation.single_table import evaluate_quality

def read_csv(path:'str',
             sample_size:'int',
             dates:'list'=[]):
    """
    Назначение:
    
    Чтение выбранного количества строк csv-файла в pandas dataframe 
    с учетом полей, содержащих даты, а также коррекция значений
    таких полей (при необходимости)
    
    Аргументы:
    
    path: путь к csv-файлу
    
    sample_size: количество читаемых строк
    
    dates: список имен полей, содержащих даты. По умолчанию - пустой список,
    чтение без парсинга дат
    
    Результат:
    
    Прочитанный csv-файл
    
    Возвращаемое значение:
    
    df: pandas dataframe
    
    """
    if dates:
        df = pd.read_csv(path, parse_dates=dates).sample(n=sample_size, ignore_index=True)
        for index, column in df.dtypes.items():
                if isinstance(column, pd.core.dtypes.dtypes.DatetimeTZDtype):
                    df[index] = pd.to_datetime(df[index]).dt.tz_localize(None)
    else:
        df = pd.read_csv(path).sample(n=sample_size, ignore_index=True)
    return df

def update_metadata(metadata:'sdv.metadata.multi_table.MultiTableMetadata', 
                    prim_key:'str', 
                    id_dict:'dict', 
                    dates:'list'=[]):
    """
    Назначение:
    
    Обновление/коррекция объекта метаданных. Добавление типов
    id и datetime, шаблонов регулярных выражений, а также первичного
    ключа
    
    Аргументы:
    
    metadata: sdv single_table_metadata - объект метаданных
    
    prim_key: строка с именем поля, которое яввялется первичным
    ключом таблицы
    
    id_dict: список из имен полей, которые должны иметь тип id
    
    dates: список из имен полей, содержащих даты
    
    Результат:
    
    Обновленный объект метаданных, готовый к передаче модели
    для обучения
    
    Возвращаемое значение:
    
    Отсутствует
    
    """
    for table, regex in id_dict.items():
        if regex:
            metadata.update_column(
                column_name=table,
                sdtype='id',
                regex_format=regex
            )
        else:
            metadata.update_column(
                column_name=table,
                sdtype='id'
            )
    if prim_key not in id_dict.keys():
        metadata.update_column(
            column_name=prim_key,
            sdtype='id'
        )
    metadata.set_primary_key(
        column_name=prim_key
    )
    if dates:
        for date_column in dates:
            metadata.update_column(
                column_name=date_column,
                sdtype='datetime',
                datetime_format='%Y-%m-%d %H:%M:%S'
            )

def generate_fake_data(model:'str', 
                       metadata:'sdv.metadata.multi_table.MultiTableMetadata', 
                       df:'pandas.core.frame.DataFrame', 
                       fake_df_size:'int'):
    """ 
    Назначение:
    
    Генерация фейковых данных. В теле функции модель создается
    и обучается. Далее производится выборка синтетических данных,
    создается отчет и визуализация, оценивающие качество 
    сгенерированных данных
    
    Аргументы:
    
    model: Модель, использующаяся для генерации синтетических данных
    Доступно 4 варианта значения аргумента (не зависит от регистра):
        Copula - быстрая статистическая модель
        TVAE - относительно быстрая variational autoencoder-based нейронка
        CTGAN - генеративная(generative adversarial) нейронка, средняя скорость
        CopGAN - комбинация Copula и CTGAN, средняя скорость
    
    metadata: sdv single_table_metadata
    
    df: pandas dataframe с исходными данными
    
    fake_df_size: количество строк в синтетическом наборе данных
    
    Результат:
    
    Сгенерированный набор синтетических данных, а также отчет о качестве
    и соответствующая визуализация
    
    Возвращаемое значение:
    
    generator: обученная модель
    
    df_result: синтетический pandas dataframe
    
    report: отчет о качестве
    
    """
    if model.lower() == 'ctgan':
        generator = CTGANSynthesizer(metadata)
    elif model.lower() == 'tvae':
        generator = TVAESynthesizer(metadata)
    elif model.lower() == 'copula':
        generator = GaussianCopulaSynthesizer(metadata)
    elif model.lower() == 'copgan':
        generator = CopulaGANSynthesizer(metadata)
    generator.validate(df)
    start = time.time()
    generator.fit(df)
    print('Время обучения ', round(time.time() - start, 3), 'секунд')
    df_result = generator.sample(num_rows=fake_df_size)
    report = evaluate_quality(
            real_data=df,
            synthetic_data=df_result,
            metadata=metadata
    )
    fig = report.get_visualization(property_name='Column Shapes')
    fig.show()
    return generator, df_result, report