---BigQuery Ecommerce Project 
—Big project for SQL
--Link sample: [https://console.cloud.google.com/bigquery?project=ecommerce-349412&ws=!1m5!1m4!4m3!1sbigquery-public-data!2sgoogle_analytics_sample!3sga_sessions_20170801](https://console.cloud.google.com/bigquery?project=ecommerce-349412&ws=!1m5!1m4!4m3!1sbigquery-public-data!2sgoogle_analytics_sample!3sga_sessions_20170801)
--Link instruction: [https://docs.google.com/spreadsheets/d/1WnBJsZXj_4FDi2DyfLH1jkWtfTridO2icWbWCh7PLs8/edit#gid=0](https://docs.google.com/spreadsheets/d/1WnBJsZXj_4FDi2DyfLH1jkWtfTridO2icWbWCh7PLs8/edit#gid=0) (This file is to help you finish the final project in SQL coaching module. In this project, we will write 08 query in Bigquery base on Google Analytics dataset.

--Table Schema: [https://support.google.com/analytics/answer/3437719?hl=en](https://support.google.com/analytics/answer/3437719?hl=en) )

---Query 01: calculate total visit,pageview,transaction and revenue for Jan, Feb and March 2017 order by month

#standardSQL
SELECT
        left(date,6) as month,
        sum(totals.visits) as visits,
        sum(totals.pageviews) as pageviews,
        sum(totals.transactions) as transactions,
        sum(totals.totaltransactionrevenue)/power(10,6) as revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE _table_suffix between '20170101' and '20170331'
GROUP BY month
ORDER BY month;



---Query 02: Bounce rate per traffic source in July 2017( Bounce_rate= num_bounce/total_visit)

#standardSQL
Select
      trafficsource.source as source,
      sum(totals.visits) as total_visits,
      sum(totals.bounces) as total_no_of_bounces,
      sum(totals.bounces)/sum(totals.visits)*100 as bounce_rate
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
GROUP BY source
ORDER BY total_visits DESC;



Query 3: Revenue by traffic source by week, by month in June 2017

--Create CTE ordered by month
With month_data AS(
     select "Month" as time_type,
            left(date,6) as time,
            trafficsource.source as source,
            sum(totals.totaltransactionrevenue)/power(10,6) as revenue
      from `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
      group by time, source
),
--Create CTE ordered by week
 week_data AS(
     select "Week" as time_type,
            format_date("%Y%W", parse_date("%Y%m%d",date)) as time,
            trafficsource.source as source,
            sum(totals.totaltransactionrevenue)/power(10,6) as revenue
      from `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
      group by time, source
 )
--Union all
SELECT * 
FROM month_data
UNION ALL
SELECT *
FROM week_data
ORDER BY revenue DESC;



Query 04: Average number of product pageviews by purchaser type (purchasers vs non-purchasers) in June, July 2017. 

#standardSQL
--Create CTE number of product pageviews by purchasers
With pageview_by_purchaser as(
             select  left(date,6) as month,
                     fullVisitorId,
                     sum(totals.pageviews) as total_pageview_by_purchaser
             from `bigquery-public-data.google_analytics_sample.ga_sessions_*`
             where _table_suffix between '20170601' and '20170731'
                  and totals.transactions is not null
             group by month, fullVisitorId
),
--Create CTE number of product pageviews by non-purchasers
pageview_by_nonpurchaser as(
             select  left(date,6) as month,
                     fullVisitorId,
                     sum(totals.pageviews) as total_pageview_by_nonpurchaser
             from `bigquery-public-data.google_analytics_sample.ga_sessions_*`
             where _table_suffix between '20170601' and '20170731'
                  and totals.transactions is null
            group by month, fullVisitorId
)   
--Average number of product pageviews
SELECT
      month,
      AVG(total_pageview_by_purchaser) AS avg_total_pageview_by_purchaser,
      AVG(total_pageview_by_nonpurchaser) AS avg_total_pageview_by_nonpurchaser
FROM pageview_by_purchaser
JOIN pageview_by_nonpurchaser using(month)
GROUP BY month;



Query 05: Average number of transactions per user that made a purchase in July 2017

#standardSQL
--Create CTE transactions_per_user
With transactions_per_user AS(
     select
         left(date,6) as month,
         fullVisitorId,
         sum(totals.transactions) as total_transactions
    from `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
    where totals.transactions is not null
    group by month, fullVisitorId
)
--Average number of transactions per user
SELECT
  month,
  AVG(total_transactions) AS avg_total_transactions_per_user
FROM transactions_per_user
GROUP BY month;



Query 06: Average amount of money spent per session

#standardSQL
--Create CTE amount of money spent per session
with revenue_by_user as(
     select left(date,6) as month,
            fullVisitorId,
            sum(totals.visits) as total_visits,
            sum(totals.totaltransactionrevenue) as total_revenue
     from `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
     where totals.transactions IS NOT NULL
     group by month, fullVisitorId
)
--Average number of amount spent per session
SELECT month,
      ROUND(sum(total_revenue)/sum(total_visits),2) as avg_total_revenue_by_user_per_visit
FROM revenue_by_user
GROUP BY month;



Query 07: Other products purchased by customers who purchased product “YouTube Men’sVintage Henley” in July 2017. Output should show product name and the quantity was ordered.

#standardSQL
--Create CTE table including customers purchased product “YouTube Men’s Vintage Henley”
With special_customer as(
             select distinct fullVisitorId
             from `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
                  unnest(hits) as hits,
                  unnest(hits.product) as product
             where product.v2ProductName ="YouTube Men's Vintage Henley"
                  and product.productrevenue is not null
)
              
--Other product purchased by those customers
SELECT product.v2ProductName AS other_product_purchased,
       sum(product.productquantity) AS quantity
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
      unnest(hits) as hits,
      unnest(hits.product) as product
INNER JOIN special_customer USING(fullVisitorId)
WHERE product.v2ProductName <> "YouTube Men's Vintage Henley"
     AND product.productrevenue is not null
GROUP BY product.v2ProductName
ORDER BY sum(product.productquantity) DESC;




Query 08: Calculate cohort map from pageview to addtocart to purchase in last 3 month. For example, 100% pageview then 40% add_to_cart and 10% purchase.

#standardSQL
--create table CTE: product_view
With product_view as(
             select left(date,6) as month,
                    count(product.productSKU) as num_product_view
              from `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
                    unnest (hits) as hits,
                    unnest (hits.product) as product
              where _table_suffix between '20170101' and '20170331'
                   and hits.eCommerceAction.action_type = '2'
              group by month
),
--create table CTE:add_to_cart
 add_to_cart  as(
             select left(date,6) as month,
                    count(product.productSKU) as num_add_to_cart
              from `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
                    unnest (hits) as hits,
                    unnest (hits.product) as product
              where _table_suffix between '20170101' and '20170331'
                   and hits.eCommerceAction.action_type = '3'
              group by month
),
--create table CTE: purchase
purchase  as(
             select left(date,6) as month,
                    count(product.productSKU) as num_purchase
              from `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
                    unnest (hits) as hits,
                    unnest (hits.product) as product
              where _table_suffix between '20170101' and '20170331'
                   and hits.eCommerceAction.action_type = '6'
              group by month
)
--calculate cohort map from pageview to addtocart to purchase
SELECT pv.month,
  num_product_view,
  num_add_to_cart,
  num_purchase,
  round(num_add_to_cart/num_product_view*100,2) as add_to_cart_rate,
  round(num_purchase/num_product_view*100,2) as purchase_rate
from product_view pv
join add_to_cart a on pv.month = a.month 
join purchase p on pv.month = p.month
order by month;
