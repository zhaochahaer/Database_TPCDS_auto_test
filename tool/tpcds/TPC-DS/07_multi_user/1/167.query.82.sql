:EXPLAIN_ANALYZE
-- start query 67 in stream 0 using template query82.tpl and seed 1021206229
select  i_item_id
       ,i_item_desc
       ,i_current_price
 from item, inventory, date_dim, store_sales
 where i_current_price between 43 and 43+30
 and inv_item_sk = i_item_sk
 and d_date_sk=inv_date_sk
 and d_date between cast('1999-04-18' as date) and (cast('1999-04-18' as date) +  '60 days'::interval)
 and i_manufact_id in (598,413,411,285)
 and inv_quantity_on_hand between 100 and 500
 and ss_item_sk = i_item_sk
 group by i_item_id,i_item_desc,i_current_price
 order by i_item_id
 limit 100;

-- end query 67 in stream 0 using template query82.tpl
