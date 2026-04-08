{{ config(materialized='table') }}

SELECT c_last_name, c_first_name, c_salutation, c_preferred_cust_flag, ss_ticket_number, cnt
FROM
  (SELECT ss_ticket_number, ss_customer_sk, count(*) cnt
   FROM {{ ref('stg_store_sales') }}, {{ ref('stg_date_dim') }}, {{ ref('stg_store') }}, {{ ref('stg_household_demographics') }}
   WHERE stg_store_sales.ss_sold_date_sk = stg_date_dim.d_date_sk
     AND stg_store_sales.ss_store_sk = stg_store.s_store_sk
     AND stg_store_sales.ss_hdemo_sk = stg_household_demographics.hd_demo_sk
     AND (stg_date_dim.d_dom BETWEEN 1 AND 3 OR stg_date_dim.d_dom BETWEEN 25 AND 28)
     AND (stg_household_demographics.hd_buy_potential = '>10000' OR
          stg_household_demographics.hd_buy_potential = 'Unknown')
     AND stg_household_demographics.hd_vehicle_count > 0
     AND (CASE WHEN stg_household_demographics.hd_vehicle_count > 0
          THEN stg_household_demographics.hd_dep_count / stg_household_demographics.hd_vehicle_count
          ELSE null END) > 1.2
     AND stg_date_dim.d_year IN (1999, 1999+1, 1999+2)
     AND stg_store.s_county IN ('Williamson County','Williamson County','Williamson County','Williamson County',
                                'Williamson County','Williamson County','Williamson County','Williamson County')
   GROUP BY ss_ticket_number, ss_customer_sk) dn, {{ ref('stg_customer') }}
WHERE ss_customer_sk = c_customer_sk
  AND cnt BETWEEN 15 AND 20
ORDER BY c_last_name, c_first_name, c_salutation, c_preferred_cust_flag DESC, ss_ticket_number
