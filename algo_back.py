import sys
import duckdb
import numpy as np
import pandas as pd
from tqdm import tqdm


query = """
        WITH avg_sales_data AS (SELECT * REPLACE(
        CAST(REPLACE(Avg_sales_pc, ',', '.') AS DECIMAL(10,2)) AS Avg_sales_pc
    ) \
                                FROM read_csv_auto( \
                                        'static/avg_sales.csv', \
                                        header = True, \
                                        delim = ';' \
                                     )),
             category_data AS (SELECT final_item_id, \
                                      cat.type, \
                                      "Группа 4" AS cat4 \
                               FROM read_csv_auto( \
                                            'static/map_category_sku.csv', \
                                            header = True \
                                    ) AS cat \
                                        LEFT JOIN read_csv_auto( \
                                       'static/mapping_cat4.csv', \
                                       header = True \
                                                  ) AS cat_4 ON cat_4.Category4 = cat.type),
             capacity AS (SELECT * \
                          FROM read_csv_auto( \
                                  'static/capacity.csv', \
                                  header = True \
                               )),
             cat_equip AS (SELECT * \
                           FROM read_csv_auto( \
                                   'static/cat_equip.csv', \
                                   header = True \
                                ))
        SELECT a.*, ce."Тип оборудования" as equip_type, "Квота" as Capacity \
        FROM (SELECT a.*, \
                     c.type, \
                     c.cat4, \
                     ROW_NUMBER() OVER (PARTITION BY a.Store, c.type ORDER BY a.Avg_sales_pc DESC) AS Row_num, \
                     SUM(a.Avg_sales_pc) OVER (PARTITION BY a.Store, c.type)                       AS Sum_sales, \
                     a.Avg_sales_pc / SUM(a.Avg_sales_pc) OVER (PARTITION BY a.Store, c.type)      AS part_sales \
              FROM avg_sales_data a \
                       LEFT JOIN category_data c ON c.final_item_id = a.Item) as a \
                 LEFT JOIN cat_equip ce ON a.cat4 = "Группа 4" \
                 LEFT JOIN capacity cap ON ce."Тип оборудования" = cap."Тип оборудования" AND Store = "Код Склада" \
        """


# Алгоритм выбора эталона
def select_etalon(df, df_equip) -> (pd.DataFrame, pd.DataFrame):
    # Инициализация полей
    df['is_etalon'] = 0
    df['sales_cum_sum'] = 0.0

    results = []
    for _, row in tqdm(iterable=df_equip.iterrows(), total=len(df_equip)):
        store = row['Код Склада']
        equip_type = row['Тип оборудования']
        equip_capacity = row['Квота']

        # берем товары по оборудованию и магазину
        subset_mask = (df.equip_type == equip_type) & (df.Store == store)
        subset = df.loc[subset_mask].copy()

        # Считаем накопительные продажи (cum sum по part_sales) внутри категорий (cat4)
        subset['sales_cum_sum'] = subset.groupby('cat4')['part_sales'].cumsum()

        # сортировка по накопительным продажам
        subset = subset.sort_values(by=['sales_cum_sum'], ascending=True)

        # выбираем первые equip_capacity товаров
        selected_idx = subset.index[:equip_capacity]

        # обновляем значения в исходном df
        df.loc[selected_idx, 'is_etalon'] = 1
        df.loc[subset_mask, 'sales_cum_sum'] = subset['sales_cum_sum']

        etal = subset.loc[selected_idx].groupby(['cat4', 'equip_type']) \
            .agg({'is_etalon': 'sum'}) \
            .rename(columns={'is_etalon': 'prod_count'}) \
            .reset_index()
        results.append(etal)

    etal = pd.concat(results)
    print(f'{len(df)}=')
    df_standards = df[df['is_etalon'] == 1]
    print(f'{len(df_standards)}=')
    return df, etal


def main():
    conn = duckdb.connect()
    print('loading data')
    df_data = conn.execute(query).df()
    df_equip = conn.execute("SELECT * FROM read_csv_auto('static/capacity.csv',header = True )").df()
    print('starting etalon selection')
    result_df, result_etal = select_etalon(df_data, df_equip)
    print('saving results')
    result_df.to_csv('etalon_selection_intermediate_result.csv', index=False, sep=';', encoding='utf-8-sig')
    result_etal.to_csv('etalon_selection_result.csv', index=False, sep=';', encoding='utf-8-sig')
    conn.close()


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)
