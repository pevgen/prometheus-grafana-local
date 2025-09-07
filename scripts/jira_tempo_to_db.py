import csv
import re
import psycopg2
from psycopg2 import sql

def clean_column_name(name):
    name = name.strip()
    name = name.replace(' ', '_')
    name = re.sub(r'\W+', '', name)
    return name

DB_PARAMS = {
    'dbname': 'grafanadb',
    'user': 'grafanauser',
    'password': 'grafanapassword',
    'host': 'localhost',
    'port': 5432
}

CSV_FILE = 'D:/Work/prometheus-grafana-local/temp_sources_no_git/source_jira_tempo.csv'
TABLE_NAME = 'source_jira_tempo'
PRIMARY_KEY_CSV = 'Issue Key'
FLOAT_COLUMN_CSV = 'Hours'

def main():
    with open(CSV_FILE, newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        headers_original = next(reader)

        headers_clean = [clean_column_name(h) for h in headers_original]

        try:
            pk_index = headers_original.index(PRIMARY_KEY_CSV)
        except ValueError:
            raise Exception(f"В CSV нет колонки '{PRIMARY_KEY_CSV}'")
        pk_column = headers_clean[pk_index]

        try:
            float_index = headers_original.index(FLOAT_COLUMN_CSV)
        except ValueError:
            raise Exception(f"В CSV нет колонки '{FLOAT_COLUMN_CSV}'")
        float_column = headers_clean[float_index]

        conn = psycopg2.connect(**DB_PARAMS)
        cur = conn.cursor()

        columns_with_types = []
        for i, col in enumerate(headers_clean):
            if i == pk_index:
                col_type = "VARCHAR PRIMARY KEY"
            elif i == float_index:
                col_type = "DOUBLE PRECISION"
            else:
                col_type = "TEXT"
            columns_with_types.append(sql.SQL("{} {}").format(sql.Identifier(col), sql.SQL(col_type)))

        create_table_query = sql.SQL(
            "CREATE TABLE IF NOT EXISTS {} ({})"
        ).format(
            sql.Identifier(TABLE_NAME),
            sql.SQL(', ').join(columns_with_types)
        )
        cur.execute(create_table_query)
        conn.commit()

        insert_query = sql.SQL("INSERT INTO {} ({}) VALUES ({}) ON CONFLICT ({}) DO NOTHING").format(
            sql.Identifier(TABLE_NAME),
            sql.SQL(', ').join(map(sql.Identifier, headers_clean)),
            sql.SQL(', ').join(sql.Placeholder() * len(headers_clean)),
            sql.Identifier(pk_column)
        )

        for row in reader:
            # Преобразуем значение Hours в float, если пусто или не число - None
            if row[float_index].strip() == '':
                row[float_index] = None
            else:
                try:
                    row[float_index] = float(row[float_index])
                except ValueError:
                    row[float_index] = None
            cur.execute(insert_query, row)

        conn.commit()
        cur.close()
        conn.close()

if __name__ == "__main__":
    main()
