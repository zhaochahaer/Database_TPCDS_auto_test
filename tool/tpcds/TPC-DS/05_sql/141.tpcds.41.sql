:EXPLAIN_ANALYZE
-- start query 62 in stream 0 using template query41.tpl and seed 1117675118
select  distinct(i_product_name)
 from item i1
 where i_manufact_id between 868 and 868+40 
   and (select count(*) as item_cnt
        from item
        where (i_manufact = i1.i_manufact and
        ((i_category = 'Women' and 
        (i_color = 'mint' or i_color = 'peru') and 
        (i_units = 'Unknown' or i_units = 'Bunch') and
        (i_size = 'petite' or i_size = 'extra large')
        ) or
        (i_category = 'Women' and
        (i_color = 'moccasin' or i_color = 'white') and
        (i_units = 'Oz' or i_units = 'Case') and
        (i_size = 'large' or i_size = 'medium')
        ) or
        (i_category = 'Men' and
        (i_color = 'snow' or i_color = 'lawn') and
        (i_units = 'N/A' or i_units = 'Tsp') and
        (i_size = 'economy' or i_size = 'small')
        ) or
        (i_category = 'Men' and
        (i_color = 'honeydew' or i_color = 'ghost') and
        (i_units = 'Ton' or i_units = 'Cup') and
        (i_size = 'petite' or i_size = 'extra large')
        ))) or
       (i_manufact = i1.i_manufact and
        ((i_category = 'Women' and 
        (i_color = 'hot' or i_color = 'navy') and 
        (i_units = 'Each' or i_units = 'Gross') and
        (i_size = 'petite' or i_size = 'extra large')
        ) or
        (i_category = 'Women' and
        (i_color = 'blush' or i_color = 'firebrick') and
        (i_units = 'Dozen' or i_units = 'Tbl') and
        (i_size = 'large' or i_size = 'medium')
        ) or
        (i_category = 'Men' and
        (i_color = 'royal' or i_color = 'cornflower') and
        (i_units = 'Box' or i_units = 'Bundle') and
        (i_size = 'economy' or i_size = 'small')
        ) or
        (i_category = 'Men' and
        (i_color = 'cyan' or i_color = 'burlywood') and
        (i_units = 'Gram' or i_units = 'Ounce') and
        (i_size = 'petite' or i_size = 'extra large')
        )))) > 0
 order by i_product_name
 limit 100;

-- end query 62 in stream 0 using template query41.tpl
