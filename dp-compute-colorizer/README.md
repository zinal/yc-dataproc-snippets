# Раскраска динамических вычислительных ресурсов Data Proc метками проектов

Пример запроса Yandex Query над данными биллинга для разметки позиций затрат кластеров Data Proc метками проектов:

```SQL
SELECT
    COALESCE(y.project_id, '-') AS project_id,
    x.`cloud_name`,
    x.`folder_name`,
    x.`resource_id`,
    x.`service_name`,
    x.`sku_name`,
    x.`date`,
    x.`currency`,
    x.`pricing_quantity`,
    x.`pricing_unit`,
    x.`cost`,
    x.`credit`,
    x.`monetary_grant_credit`,
    x.`volume_incentive_credit`,
    x.`cud_credit`,
    x.`misc_credit`,
FROM bindings.`billing-billing1_billing1-detail/` AS x
LEFT JOIN (
SELECT DISTINCT
    `date`,
    `label.user_labels.cluster_id` AS cluster_id,
    `label.user_labels.project_id` AS project_id
FROM bindings.`billing-billing1_billing1-detail/`
WHERE `label.user_labels.cluster_id` IS NOT NULL
  AND `label.user_labels.cluster_id`<>''
  AND `label.user_labels.project_id` IS NOT NULL
  AND `label.user_labels.project_id`<>''
  AND `date`>=Date('2022-10-01')
  AND `date`<Date('2022-11-01')
) AS y
ON y.cluster_id = x.`label.user_labels.cluster_id` AND y.`date`=x.`date`
WHERE x.`date`>=Date('2022-10-01')
  AND x.`date`<Date('2022-11-01')
;
```
