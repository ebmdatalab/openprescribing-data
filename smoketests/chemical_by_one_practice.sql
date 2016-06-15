SELECT
  month,
  SUM(items) AS items,
  FORMAT("%.2f", ROUND(SUM(actual_cost), 2)) AS actual_cost,
  SUM(quantity) AS quantity
FROM
  `hscic.prescribing`
WHERE
  bnf_code LIKE '0212000AA%'
  AND practice='A81015'
GROUP BY
  month
ORDER BY
  month ASC
