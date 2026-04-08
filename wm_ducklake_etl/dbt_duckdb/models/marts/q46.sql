{{ config(materialized='table') }}

SELECT c_last_name, c_first_name, ca_city, bought_city, ss_ticket_number, amt, profit
FROM
  (SELECT ss_ticket_number, ss_customer_sk, ca_city bought_city,
          sum(ss_coupon_amt) amt, sum(ss_net_profit) profit
   FROM {{ ref('stg_store_sales') }}, {{ ref('stg_date_dim') }}, {{ ref('stg_store') }}, {{ ref('stg_household_demographics') }}, {{ ref('stg_customer_address') }}
   WHERE stg_store_sales.ss_sold_date_sk = stg_date_dim.d_date_sk
     AND stg_store_sales.ss_store_sk = stg_store.s_store_sk
     AND stg_store_sales.ss_hdemo_sk = stg_household_demographics.hd_demo_sk
     AND stg_store_sales.ss_addr_sk = stg_customer_address.ca_address_sk
     AND (stg_household_demographics.hd_dep_count = 4 OR
          stg_household_demographics.hd_vehicle_count = 3)
     AND stg_date_dim.d_dow IN (6, 0)
     AND stg_date_dim.d_year IN (1999, 1999+1, 1999+2)
     AND stg_store.s_city IN ('Fairview','Midway','Fairview','Fairview','Fairview')
   GROUP BY ss_ticket_number, ss_customer_sk, ss_addr_sk, ca_city) dn,
  {{ ref('stg_customer') }},
  {{ ref('stg_customer_address') }} current_addr
WHERE ss_customer_sk = c_customer_sk
  AND stg_customer.c_current_addr_sk = current_addr.ca_address_sk
  AND current_addr.ca_city <> bought_city
ORDER BY c_last_name, c_first_name, ca_city, bought_city, ss_ticket_number
LIMIT 100
