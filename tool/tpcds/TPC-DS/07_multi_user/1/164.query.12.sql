:EXPLAIN_ANALYZE
-- start query 64 in stream 0 using template query12.tpl and seed 450033423
select  i_item_id
      ,i_item_desc 
      ,i_category 
      ,i_class 
      ,i_current_price
      ,sum(ws_ext_sales_price) as itemrevenue 
      ,sum(ws_ext_sales_price)*100/sum(sum(ws_ext_sales_price)) over
          (partition by i_class) as revenueratio
from	
	web_sales
    	,item 
    	,date_dim
where 
	ws_item_sk = i_item_sk 
  	and i_category in ('Home', 'Jewelry', 'Women')
  	and ws_sold_date_sk = d_date_sk
	and d_date between cast('2001-06-19' as date) 
				and (cast('2001-06-19' as date) + '30 days'::interval)
group by 
	i_item_id
        ,i_item_desc 
        ,i_category
        ,i_class
        ,i_current_price
order by 
	i_category
        ,i_class
        ,i_item_id
        ,i_item_desc
        ,revenueratio
limit 100;

-- end query 64 in stream 0 using template query12.tpl