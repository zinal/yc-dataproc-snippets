# Раскраска динамических вычислительных ресурсов Data Proc метками проектов

При создании множества кластеров [Yandex Data Proc](https://cloud.yandex.ru/services/data-proc) в рамках одного облака и одной папки иногда возникает необходимость отнесения затрат на эти кластера к разным бизнес-проектам.

Информация о затратах с посуточной детализацией доступна из файлов экспорта в формате CSV, которые могут формироваться средствами биллинга Yandex Cloud и сохраняться в объектном хранилище в соответствии с заданными настройками.

Привязка идентификаторов проектов к кластерам Data Proc может поставляться в виде внешнего справочника, либо устанавливаться непосредственно средствами Yandex Cloud в виде пользовательской метки на ресурс соответствующего кластера Data Proc, как показано ниже:

```bash
yc dataproc cluster add-labels --name <Кластер> --labels project_id=<МеткаПроекта>
```

В приведённой выше команде:
* <Кластер> - логическое имя кластера Data Proc;
* <МеткаПроекта> - идентификатор, используемый для ссылки на бизнес-проект.

Пользовательские метки доступны в данных биллинга в виде дополнительных колонок формата `label.user_labels.<ИмяМетки>`.

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
