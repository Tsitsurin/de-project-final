        MERGE INTO STV2023121121__DWH.global_metrics AS tgt USING
        (WITH t1 AS
            (-- main table with all data
        SELECT o.*,
                c.currency_with_div,
                (o.amount * c.currency_with_div) AS amount_usd
            FROM STV2023121121__STAGING.transactions o
            LEFT JOIN STV2023121121__STAGING.currencies c ON o.currency_code = c.currency_code
            WHERE c.currency_code_with = 420
                AND o.transaction_dt::date = c.date_update::date
                AND o.status = 'done'
                AND o.transaction_dt::date = '_date_' ) SELECT cast(transaction_dt AS date) AS date_update,
                                                                currency_code AS currency_from,
                                                                sum(amount_usd) AS amount_total,
                                                                count(amount_usd) AS cnt_transactions,
                                                                round((count(amount_usd)/count(DISTINCT (account_number_from))),3) AS avg_transactions_per_account,
                                                                count(DISTINCT (account_number_from)) AS cnt_accounts_make_transactions
        FROM t1
        GROUP BY date_update,
                    currency_code
        ORDER BY date_update ASC) src ON tgt.date_update = src.date_update
        AND tgt.currency_from = src.currency_from WHEN MATCHED THEN
        UPDATE
        SET amount_total = src.amount_total,
            cnt_transactions = src.cnt_transactions,
            avg_transactions_per_account = src.avg_transactions_per_account,
            cnt_accounts_make_transactions = src.cnt_accounts_make_transactions WHEN NOT MATCHED THEN
        INSERT (date_update,
                currency_from,
                amount_total,
                cnt_transactions,
                avg_transactions_per_account,
                cnt_accounts_make_transactions)
        VALUES (src.date_update, src.currency_from, src.amount_total, src.cnt_transactions, src.avg_transactions_per_account, src.cnt_accounts_make_transactions);