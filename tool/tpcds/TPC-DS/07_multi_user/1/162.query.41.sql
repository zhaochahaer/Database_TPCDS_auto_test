:EXPLAIN_ANALYZE
-- start query 62 in stream 0 using template query41.tpl and seed 856656327
select  distinct(i_product_name)
 from item i1
 where i_manufact_id between 669 and 669+40 
   and (select count(*) as item_cnt
        from item
        where (i_manufact = i1.i_manufact and
        ((i_category = 'Women' and 
        (i_color = 'almond' or i_color = 'pink') and 
        (i_units = 'N/A' or i_units = 'Gross') and
        (i_size = 'large' or i_size = 'economy')
        ) or
        (i_category = 'Women' and
        (i_color = 'indian' or i_color = 'papaya') and
        (i_units = 'Ounce' or i_units = 'Box') and
        (i_size = 'medium' or i_size = 'small')
        ) or
        (i_category = 'Men' and
        (i_color = 'antique' or i_color = 'cyan') and
        (i_units = 'Lb' or i_units = 'Bundle') and
        (i_size = 'N/A' or i_size = 'extra large')
        ) or
        (i_category = 'Men' and
        (i_color = 'red' or i_color = 'hot') and
        (i_units = 'Cup' or i_units = 'Dram') and
        (i_size = 'large' or i_size = 'economy')
        ))) or
       (i_manufact = i1.i_manufact and
        ((i_category = 'Women' and 
        (i_color = 'midnight' or i_color = 'sienna') and 
        (i_units = 'Bunch' or i_units = 'Pound') and
        (i_size = 'large' or i_size = 'economy')
        ) or
        (i_category = 'Women' and
        (i_color = 'smoke' or i_color = 'grey') and
        (i_units = 'Each' or i_units = 'Tsp') and
        (i_size = 'medium' or i_size = 'small')
        ) or
        (i_category = 'Men' and
        (i_color = 'peru' or i_color = 'lemon') and
        (i_units = 'Case' or i_units = 'Oz') and
        (i_size = 'N/A' or i_size = 'extra large')
        ) or
        (i_category = 'Men' and
        (i_color = 'chiffon' or i_color = 'frosted') and
        (i_units = 'Pallet' or i_units = 'Dozen') and
        (i_size = 'large' or i_size = 'economy')
        )))) > 0
 order by i_product_name
 limit 100;

-- end query 62 in stream 0 using template query41.tpl
