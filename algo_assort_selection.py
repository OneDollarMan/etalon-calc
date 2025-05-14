import pandas as pd


def calc_ratings(conn):
    q = """
        CREATE TABLE data_ratings AS
        WITH ranked_data AS (
            SELECT 
                store,
                item,
                cat4,
                equip_type,
                avg_sales_pcs,
                avg_sales_rub,
                marginality,
                -- Рейтинг по количеству продаж (шт)
                ROW_NUMBER() OVER (PARTITION BY store, cat4 ORDER BY avg_sales_pcs DESC) AS sales_pcs_rank,
                -- Рейтинг по сумме продаж (руб)
                ROW_NUMBER() OVER (PARTITION BY store, cat4 ORDER BY avg_sales_rub DESC) AS sales_rub_rank,
                -- Рейтинг по маржинальности
                ROW_NUMBER() OVER (PARTITION BY store, cat4 ORDER BY marginality DESC) AS margin_rank,
                (0.8 * sales_pcs_rank + 1.0 * sales_rub_rank + 0.5 * margin_rank) AS weighted_rank,
                is_standard
            FROM data
        )
        SELECT 
            store,
            cat4,
            item,
            avg_sales_pcs,
            avg_sales_rub,
            marginality,
            sales_pcs_rank,
            sales_rub_rank,
            margin_rank,
            weighted_rank,
            -- Финальный рейтинг по взвешенному значению
            ROW_NUMBER() OVER (PARTITION BY store, cat4 ORDER BY weighted_rank DESC) AS final_rating,
            is_standard
        FROM ranked_data
        ORDER BY store, cat4, final_rating;
    """
    conn.execute(q)


def select_assortment(conn) -> pd.DataFrame:
    q = """
        SELECT 
            r.*,
            s.prod_count,
            CASE WHEN r.final_rating <= s.prod_count THEN 1 ELSE 0 END AS is_assort
        FROM data_ratings r
        JOIN standards s ON r.store = s.store AND r.cat4 = s.cat4
        ORDER BY r.store, r.cat4;
    """
    return conn.execute(q).df()


def select_assort(conn):
    print('----- Starting assortment selection -----')
    print('Calculating ratings...')
    calc_ratings(conn)
    print('Selecting assortment...')
    df = select_assortment(conn)
    print('Saving assortment...')
    df.to_csv('assort.csv', index=False, sep=';', encoding='utf-8-sig')