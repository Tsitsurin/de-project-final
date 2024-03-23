DROP TABLE if exists public.transactions;
CREATE TABLE if not exists STV2023121121__STAGING.transactions (
	operation_id varchar(60),
	account_number_from INT,
	account_number_to INT,
	currency_code INT,
	country varchar(30),
	status varchar(30),
	transaction_type varchar(30),
	amount INT,
	transaction_dt timestamp
) SEGMENTED BY HASH(operation_id, transaction_dt) ALL NODES;


DROP TABLE if exists public.currencies;
CREATE TABLE if not exists STV2023121121__STAGING.currencies (
	date_update timestamp,
	currency_code INT,
	currency_code_with INT,
	currency_with_div decimal(5, 3)
) SEGMENTED BY HASH(date_update, currency_code) ALL NODES;


CREATE PROJECTION if not exists STV2023121121__STAGING.transactions_date_projection
(
    operation_id,
    account_number_from,
    account_number_to,
    currency_code,
    country,
    status,
    transaction_type,
    amount,
    transaction_dt
)
AS
SELECT 
    operation_id,
    account_number_from,
    account_number_to,
    currency_code,
    country,
    status,
    transaction_type,
    amount,
    transaction_dt
FROM STV2023121121__STAGING.transactions
ORDER BY transaction_dt
SEGMENTED BY HASH(operation_id, transaction_dt)
ALL NODES;


CREATE PROJECTION if not exists STV2023121121__STAGING.currencies_date_projection
(
    date_update,
    currency_code,
    currency_code_with,
    currency_code_div
)
AS
SELECT 
    date_update,
    currency_code,
    currency_code_with,
    currency_with_div
FROM STV2023121121__STAGING.currencies
ORDER BY date_update
SEGMENTED BY HASH(date_update)
ALL NODES;


CREATE TABLE STV2023121121__DWH.global_metrics (
    date_update DATE,
    currency_from CHAR(3),
    amount_total DECIMAL(18, 6),
    cnt_transactions INT,
    avg_transactions_per_account DECIMAL(18, 6),
    cnt_accounts_make_transactions INT
) SEGMENTED BY HASH(date_update) ALL NODES;