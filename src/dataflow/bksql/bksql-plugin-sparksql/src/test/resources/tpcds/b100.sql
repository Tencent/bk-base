SELECT
  i_item_desc,
  i_category,
  i_class,
  i_current_price,
  count(*) AS itemrevenue
FROM
  item
GROUP BY
i_item_desc,
i_category,
i_class,
i_current_price
grouping sets(i_item_desc, i_category, (i_class, i_current_price))

