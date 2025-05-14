import sys
import duckdb
import pandas as pd
from tqdm import tqdm

from algo_assort_selection import select_assort

query = """
CREATE TABLE avg_sales_data AS (
    SELECT 
        final_store_id AS store, 
        item_id AS item,
        'Группа 4' AS cat4,
        CAST(REPLACE("Средняя в день, шт(2)", ',', '.') AS DECIMAL(10,2)) AS avg_sales_pcs,
        CAST(REPLACE("Средняя в день, руб(2)", ',', '.') AS DECIMAL(10,2)) AS avg_sales_rub,
        CAST(REPLACE("Маржинальность(2)", ',', '.') AS DECIMAL(10,2)) AS marginality
    FROM read_csv_auto(
        'static/ОЗОН продажи.csv',
        header = True,
        delim = ';'
    )
);

CREATE TABLE category_data AS (
    SELECT 
        final_item_id,
        cat.type,
        "Группа 4" AS cat4
    FROM read_csv_auto(
        'static/map_category_sku.csv',
        header = True
    ) AS cat
    LEFT JOIN read_csv_auto(
        'static/mapping_cat4.csv',
        header = True
    ) AS cat_4 
        ON cat_4.Category4 = cat.type
);

CREATE TABLE capacity AS (
    SELECT * 
    FROM read_csv_auto(
        'static/capacity.csv',
        header = True
    )
);

CREATE TABLE cat_equip AS (
    SELECT *
    FROM read_csv_auto(
        'static/cat_equip.csv',
        header = True
    )
);

CREATE TABLE data AS SELECT 
    a.*, 
    ce."Тип оборудования" AS equip_type, 
    cap."Квота" AS capacity
FROM (
    SELECT 
        a.store,
        a.item,
        c.type,
        c.cat4,
        a.avg_sales_pcs,
        a.avg_sales_rub,
        a.marginality,
        a.avg_sales_rub / SUM(a.avg_sales_rub) OVER (
            PARTITION BY a.Store, c.type
        ) AS part_sales_rub,
        0 AS is_standard,
        0.0 AS sales_cum_sum_rub
    FROM avg_sales_data a
    LEFT JOIN category_data c 
        ON c.final_item_id = a.Item
) AS a
LEFT JOIN cat_equip ce 
    ON a.cat4 = ce."Группа 4"
LEFT JOIN capacity cap 
    ON ce."Тип оборудования" = cap."Тип оборудования" 
    AND a.Store = cap."Код Склада";

ALTER TABLE data ADD PRIMARY KEY (store, item);
"""


# Алгоритм выбора эталона
def select_standards(conn) -> (pd.DataFrame, pd.DataFrame):
    df_equip = conn.execute("SELECT * FROM capacity").df()

    subsets = []
    for _, row in tqdm(iterable=df_equip.iterrows(), total=len(df_equip)):
        store = row['Код Склада']
        equip_type = row['Тип оборудования']
        equip_capacity = row['Квота']

        # берем товары по оборудованию и магазину
        subset = conn.execute(f"SELECT * FROM data WHERE equip_type='{equip_type}' AND store='{store}'").df()

        # Считаем накопительные продажи (cum sum по part_sales) внутри категорий (cat4)
        subset['sales_cum_sum_rub'] = subset.groupby('cat4')['part_sales_rub'].cumsum()

        # сортировка по накопительным продажам
        subset = subset.sort_values(by=['sales_cum_sum_rub'], ascending=True)

        # выбираем первые equip_capacity товаров
        selected_idx = subset.index[:equip_capacity]

        # обновляем значения в исходном df
        subset.loc[selected_idx, 'is_standard'] = 1
        subsets.append(subset)

    # объединяем строки в один df
    df_data = pd.concat(subsets)
    print('Updating data...')
    conn.execute("INSERT INTO data SELECT * FROM df_data ON CONFLICT DO UPDATE SET is_standard = EXCLUDED.is_standard, sales_cum_sum_rub = EXCLUDED.sales_cum_sum_rub;")
    df_data = conn.execute("SELECT * FROM data").df()

    # высчитываем финальные эталоны
    df_standards = df_data.groupby(['store', 'cat4']) \
        .agg({'is_standard': 'sum'}) \
        .rename(columns={'is_standard': 'prod_count'}) \
        .reset_index()
    conn.execute("CREATE TABLE standards AS SELECT * FROM df_standards")

    return df_data, df_standards


def main():
    conn = duckdb.connect()
    print('Loading data...')
    conn.execute(query)
    print('----- Starting standards selection -----')
    result_df_data, result_df_standards = select_standards(conn)
    print('Saving standards...')
    result_df_data.to_csv('standards_intermediate.csv', index=False, sep=';', encoding='utf-8-sig')
    result_df_standards.to_csv('standards.csv', index=False, sep=';', encoding='utf-8-sig')
    select_assort(conn)
    conn.close()


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)
