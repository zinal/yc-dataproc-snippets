# Настройка выполнения заданий Apache Spark на кластерах Yandex Data Proc в условиях применения автоскейлинга 

Механизм автоскейлинга в кластерах [Yandex Data Proc](https://cloud.yandex.ru/services/data-proc) позволяет автоматизировать управление вычислительными ресурсами созданного кластера, добавляя в кластер дополнительные узлы при наличии ожидающих операций в очереди выполнения, и освобождая узлы в периоды снижения нагрузки на кластер.

Для стабильной работы заданий [Apache Spark](https://spark.apache.org) в кластерах Data Proc, использующих автоскейлинг, необходимо соблюсти два условия:
1. для всех заданий должен использоваться режим запуска (deploy mode) "cluster" или "client";
2. в составе кластера должен присутствовать подкластер, не подлежащий автоскелингу, и используемый для размещения контейнеров YARN Application Master (AM)).

При запуске задания Spark в режиме "client" основной управляющий компонент (драйвер) задания выполняется на мастер-узле кластера Data Proc, а контейнер AM запускается на одном из исполнительных узлов кластера и используется только для взаимодействия с YARN при запросе и освобождении ресурсов. При запуске в режиме "cluster" драйвер задания запускается непосредственно в контейнере AM. В обоих случаях контейнер AM не должен размещаться на узлах, управляемых автоскейлером Data Proc. Автоскейлер при снижении нагрузки может удалить любой из контролируемых им узлов, а завершение контейнера AM приводит к немедленному аварийному завершению задания в целом.

В качестве подкластера, не подлежащего автоскейлингу, могут служить узлы HDFS (подкластер узлов с ролью Data Node), либо дополнительно созданный подкластер вычислительных узлов (роль Compute Node) с выключенным автоскейлингом.

Размещение контейнеров AM строго на узлах без автоскейлинга в среде Data Proc обеспечивается механизмом меток узлов, устанавливаемых средствами ресурсного менеджера YARN. Необходимые метки узлов могут быть установлены с помощью скрипта инициализации, анализирующего роль узла (в случае использования Data Node) или имя подкластера (в случае специально созданного подкластера Compute Node).

Размещение контейнеров AM на узлах с нужной меткой обеспечивается настройкой `spark.yarn.am.nodeLabelExpression`, устанавливаемой глобально либо на уровне задания. Для корректной работы планировщика YARN в условиях установки меток на часть хостов кластера необходима корректировка параметров компонента YARN Capacity Scheduler.

Типовой набор параметров кластера Data Proc с автоскейлингом выглядит следующим образом:

| Компонент | Параметр | Типовое значение | Пояснение |
| --------- | -------- | ---------------- | -------------------------- |
| `spark` | `spark.yarn.am.nodeLabelExpression` | `SPARKAM` | Имя метки для отбора узлов для запуска AM-контейнеров заданий Spark |
| `yarn` | `yarn.node-labels.enabled` | `true` | Включить поддержку меток узлов в YARN |
| `yarn` | `yarn.node-labels.fs-store.root-dir` | `file:///hadoop/yarn/node-labels` | Каталог для хранения меток узлов в файловой системе мастер-узла кластера |
| `yarn` | `yarn.node-labels.configuration-type` | `centralized` | Режим управления метками, обычно `centralized` |
| `capacity-scheduler` | `yarn.scheduler.capacity.maximum-am-resource-percent` | `1.00` | Максимальная доля ресурсов (от 0.0 до 0.1) на выполнение контейнеров AM |
| `capacity-scheduler`  | `yarn.scheduler.capacity.root.default.accessible-node-labels` | `SPARKAM` | Разрешить заданиям в очереди `default` использовать узлы с меткой `SPARKAM` |
| `capacity-scheduler`  | `yarn.scheduler.capacity.root.accessible-node-labels.SPARKAM.capacity` | `100` | Установить допустимую долю использования узлов с меткой `SPARKAM` в 100% |
| `capacity-scheduler`  | `yarn.scheduler.capacity.root.default.accessible-node-labels.SPARKAM.capacity` | `100` | Установить допустимую долю использования заданиями очереди `default` узлов с меткой `SPARKAM` в 100% |
| `livy` | `livy.spark.deploy-mode` | `cluster` | Использовать в сессиях Apache Livy режим запуска `cluster`, вместо используемого по умолчанию для легковесных кластеров Data Proc режима `client` |

Подготовленный [пример скрипта инициализации](init_nodelabels.sh) обеспечивает установку YARN-метки "SPARKAM" на все узлы с ролью "Data Node", а также на все узлы с ролью "Compute Node", у которых имя подкластера совпадает с параметром запуска скрипта. 

> **Примечание.** Пример скрипта использует утилиты [JQ](https://github.com/jqlang/jq) и [YQ](https://github.com/mikefarah/yq) для разбора метаданных виртуальной машины, скачивая скомпилированные версии этих утилит из общедоступного бакета Yandex Object Storage.

Пример скрипта [создания кластера Data Proc](dp-sample-labels.sh) устанавливает необходимые настройки на кластер и определяет два подкластера вычислительных узлов с именами "dynamic" (автоскейлинг включён, используются прерываемые виртуальные машины) и "static" (автоскейлинг выключен). Для кластера настроен скрипт инициализации, в который передаётся в качестве параметра имя подкластера без автоскейлинга (значение "static").

Дополнительная информация:
* [документация Apache Hadoop по работе с метками узлов YARN](https://hadoop.apache.org/docs/stable/hadoop-yarn/hadoop-yarn-site/NodeLabel.html);
* [документация Apache Hadoop по настройке YARN Capacity Scheduler](https://hadoop.apache.org/docs/stable/hadoop-yarn/hadoop-yarn-site/CapacityScheduler.html);
* [документация Hortonworks по настройке меток узлов YARN](https://docs.cloudera.com/HDPDocuments/HDP2/HDP-2.4.2/bk_yarn_resource_mgt/content/configuring_node_labels.html);
* [документация Apache Spark по выполнению на кластерах YARN](https://spark.apache.org/docs/3.0.3/running-on-yarn.html).