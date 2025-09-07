import csv
import re
import psycopg2
from psycopg2 import sql

def clean_column_name(name):
    # Заменяем пробелы на нижнее подчёркивание
    name = name.replace(' ', '_')
    # Удаляем все символы, кроме букв, цифр и _
    name = re.sub(r'[^a-zA-Z0-9_]', '', name)
    return name

def generate_unique_columns(columns):
    seen = {}
    new_cols = []
    for col in columns:
        clean_col = clean_column_name(col)
        if clean_col not in seen:
            seen[clean_col] = 1
            new_cols.append(clean_col)
        else:
            seen[clean_col] += 1
            new_cols.append(f"{clean_col}_{seen[clean_col]}")
    return new_cols

def detect_float_columns(columns):
    # Возвращает множество индексов колонок, содержащих "Estimate"
    return {i for i, c in enumerate(columns) if 'Estimate' in c}

def create_table(conn, table_name, columns, float_cols, primary_key):
    with conn.cursor() as cur:
        # Формируем описание полей с типами
        fields = []
        for col in columns:
            if col == primary_key:
                fields.append(sql.SQL("{} TEXT PRIMARY KEY").format(sql.Identifier(col)))
            elif col in float_cols:
                fields.append(sql.SQL("{} DOUBLE PRECISION").format(sql.Identifier(col)))
            else:
                fields.append(sql.SQL("{} TEXT").format(sql.Identifier(col)))

        create_query = sql.SQL("""
                               CREATE TABLE IF NOT EXISTS {} (
                               {}
                               )
                               """).format(
            sql.Identifier(table_name),
            sql.SQL(',\n').join(fields)
        )
        cur.execute(create_query)
        conn.commit()

def insert_data(conn, table_name, columns, float_cols, primary_key, rows):
    with conn.cursor() as cur:
        placeholders = sql.SQL(', ').join(sql.Placeholder() * len(columns))
        insert_query = sql.SQL("INSERT INTO {} ({}) VALUES ({}) ON CONFLICT ({}) DO NOTHING").format(
            sql.Identifier(table_name),
            sql.SQL(', ').join(map(sql.Identifier, columns)),
            placeholders,
            sql.Identifier(primary_key)
        )
        for row in rows:
            # Преобразуем значения для float колонок
            values = []
            for i, val in enumerate(row):
                col = columns[i]
                if col in float_cols:
                    if val == '':
                        values.append(None)
                    else:
                        try:
                            values.append(float(val))
                        except ValueError:
                            values.append(None)
                else:
                    values.append(val if val != '' else None)
            cur.execute(insert_query, values)
        conn.commit()

def main():
    input_csv = 'D:/Work/prometheus-grafana-local/temp_sources_no_git/lops_closed_tasks_year.csv'
    table_name = 'source_jira_closed_tasks'
    primary_key_raw = 'Issue key'

    # Подключение к БД (настройте под свою БД)
    conn = psycopg2.connect(
        dbname='grafanadb',
        user='grafanauser',
        password='grafanapassword',
        host='localhost',
        port='5432'
    )

    with open(input_csv, newline='', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile)
        raw_columns = next(reader)
        cleaned_columns = generate_unique_columns(raw_columns)

        # Найдём индекс первичного ключа
        # Переводим имя к clean_column_name, чтобы искать по одним правилам
        cleaned_pk = clean_column_name(primary_key_raw)
        # В cleaned_columns уже учтены суффиксы, ищем начиная с cleaned_pk
        pk_candidates = [c for c in cleaned_columns if c.startswith(cleaned_pk)]
        if not pk_candidates:
            raise Exception(f"Primary key column '{primary_key_raw}' not found after cleaning")
        primary_key = pk_candidates[0]

        float_cols = {col for col in cleaned_columns if 'Estimate' in col}

        create_table(conn, table_name, cleaned_columns, float_cols, primary_key)

        rows = list(reader)
        insert_data(conn, table_name, cleaned_columns, float_cols, primary_key, rows)

    conn.close()
    print(f"Данные из '{input_csv}' успешно загружены в таблицу '{table_name}'")

if __name__ == '__main__':
    main()
