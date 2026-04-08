{{ config(materialized='table') }}

SELECT c_last_name, c_first_name, substr(s_city, 1, 30), ss_ticket_number, amt, profit
FROM
  (SELECT ss_ticket_number, ss_customer_sk, stg_store.s_city,
          sum(ss_coupon_amt) amt, sum(ss_net_profit) profit
   FROM {{ ref('stg_store_sales') }}, {{ ref('stg_date_dim') }}, {{ ref('stg_store') }}, {{ ref('stg_household_demographics') }}
   WHERE stg_store_sales.ss_sold_date_sk = stg_date_dim.d_date_sk
     AND stg_store_sales.ss_store_sk = stg_store.s_store_sk
     AND stg_store_sales.ss_hdemo_sk = stg_household_demographics.hd_demo_sk
     AND (stg_household_demographics.hd_dep_count = 6 OR
          stg_household_demographics.hd_vehicle_count > 2)
     AND stg_date_dim.d_dow = 1
     AND stg_date_dim.d_year IN (1999, 1999+1, 1999+2)
     AND stg_store.s_number_employees BETWEEN 200 AND 295
   GROUP BY ss_ticket_number, ss_customer_sk, ss_addr_sk, stg_store.s_city) ms,
  {{ ref('stg_customer') }}
WHERE ss_customer_sk = c_customer_sk
ORDER BY c_last_name, c_first_name, substr(s_city, 1, 30), profit
LIMIT 100
