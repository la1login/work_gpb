## Перенос данных из PostgreSQL в ClickHouse
## Три версии:
## insert_dataframe - чтение в датафрейм и его вставка as is (pandahouse)
## insert_fetchall - чтение без датафрейма, вставка cursor.fetchall()
## insert_str_batch - чтение без датафрейма, вставка запросом-строкой пачками по несколько дней