# Настройка внешней базы данных Hive Metastore для сервиса Yandex Data Proc

[Сервис Yandex Data Proc](https://cloud.yandex.ru/services/data-proc) включает в себя технологии [Apache Hive](https://hive.apache.org) и [Apache Spark](https://spark.apache.org), и использует компонент Hive Metastore для ведения информации о структуре таблиц, форматах хранения их данных, размещении файлов с данными.

Информация, с которой работает Hive Metastore, хранится в базе данных [одного из поддерживаемых типов](https://cwiki.apache.org/confluence/display/Hive/AdminManual+Metastore+Administration#AdminManualMetastoreAdministration-SupportedBackendDatabasesforMetastore). По умолчанию Yandex Data Proc размещает базу данных Hive Metastore под управлением PostgreSQL на мастер-сервере кластера Data Proc, но такая конфигурация не является полностью отказоустойчивой, имеет ограниченную масштабируемость и не позволяет организовать полноценное резервное копирование данных. Кроме того, использование отдельных копий данных Hive Metastore в каждом кластере Data Proc не позволяет организовать обработку единого массива данных с использованием нескольких кластеров.

Вместо использования встроенной базы данных Hive Metastore, на сегодня (до готовности в Yandex Cloud управляемого сервиса Hive Metastore) рекомендуется применять внешнюю базу данных, в виде сервиса [Yandex Managed Service for PostgreSQL](https://cloud.yandex.ru/services/managed-postgresql). Тем самым эффективно решаются вопросы отказоустойчивости, масштабирования и резервного копирования базы данных Hive Metastore. Время жизни метаданных в такой конфигурации превышает время существования отдельных кластеров Data Proc, и возникает возможность согласованного доступа из нескольких кластеров Data Proc к общему массиву данных, размещённому в [объектном хранилище Yandex Object Storage](https://cloud.yandex.ru/services/storage).

В ближайшее время ожидается выход сервиса Yandex Managed Hive Metastore. После доступности этого сервиса пользователям Data Proc рекомендовано заменить внешнюю базу данных Hive Metastore на этот новый сервис, для чего будет подготовлена отдельная инструкция.

Настройка внешней базы данных Hive Metastore выполняется в следующем порядке:
1. Создание и настройка сервиса PostgreSQL для базы данных Hive Metastore
2. Инициализация базы данных Hive Metastore
3. Запуск кластеров Data Proc с использованием внешней базы данных Hive Metastore

## 1. Создание и настройка сервиса PostgreSQL для базы данных Hive Metastore

Сервис PostgreSQL в Yandex Cloud создаётся в соответствии с инструкциями в [официальной документации сервиса](https://cloud.yandex.ru/docs/managed-postgresql/operations/cluster-create).

Рекомендуемые настройки сервиса для типовой инсталляции Hive Metastore:
* имя кластера - осмысленное наименование кластера, например `metastore1`;
* тип окружения - `PRODUCTION`;
* версия СУБД - `14`;
* класс хоста - `s3-c2-m8` для небольшой инсталляции Data Proc, не менее `s3-c4-m16` для нагруженной инсталляции;
* тип диска - реплицируемые сетевые SSD диски `network-ssd`, объём не менее 30 Гбайт;
* имя базы данных - `hive`;
* логин пользователя - `hive`;
* пароль - сгенерированное случайное значение длиною не менее 12 букв и цифр;
* локали сортировки и набора символов - по умолчанию (`C`);
* сеть и группа безопасности - дающие возможность доступа со стороны кластеров Data Proc;
* размещение хостов - без предоставления публичного доступа:
    * в 3 зонах доступности для максимальной отказоустойчивости,
    * в зоне, где работают кластера Data Proc, если максимальная отказоустойчивость не требуется;
* окно обслуживания - период минимальной активности кластеров Data Proc;
* разрешить доступ из консоли управления;
* разрешить сбор статистик;
* режим работы менеджера подключений - `session`;
* защита от удаления - включить для продуктивных окружений.

## 2. Инициализация базы данных Hive Metastore

Для выполнения инициализации базы данных Hive Metastore необходим кластер Data Proc, для которого включён компонент Hive. Можно использовать любой из существующих кластеров, либо создать специальный временный кластер. Пример команды для создания временного кластера Data Proc с использованием YC CLI приведён ниже:

```bash
yc dataproc cluster create hive-ms-init \
  --zone ru-central1-b \
  --service-account-name dp1 \
  --version 2.0.58 --ui-proxy \
  --services yarn,tez,hive \
  --bucket dproc1 \
  --subcluster name="master",role='masternode',resource-preset='s3-c2-m8',disk-type='network-hdd',disk-size=100,hosts-count=1,subnet-name=default-ru-central1-b \
  --subcluster name="compute",role='computenode',resource-preset='s3-c2-m8',disk-type='network-hdd',disk-size=100,hosts-count=1,max-hosts-count=1,subnet-name=default-ru-central1-b \
  --ssh-public-keys-file ssh-keys.tmp
```

Перед созданием временного кластера Data Proc должен быть создан бакет Object Storage (в примере выше - `dproc1`) и сервисный аккаунт, обладающий правами доступа к этому бакету (в примере выше - `dp1`). Также необходимо выбрать подсеть для работы кластера (в примере - `default-ru-central1-b`).

Доступ к кластеру будет осуществляться с промежуточного хоста, который должен быть предварительно создан (например, в виде виртуальной машины под управлением ОС Linux), и подключен к необходимой подсети и группе безопасности. На промежуточном хосте должен быть сгенерирован SSH-ключ, публичная часть ключа используется при создании временного кластера Data Proc (в примере команды выше ключ записан в файл `ssh-keys.tmp`).

После завершения создания кластера необходимо зайти с помощью клиента ssh от имени пользователя `ubuntu` на мастер-узел кластера. Имя хоста мастер-узла кластера Data Proc можно получить из Web-консоли либо из вывода команды `yc dataproc cluster list-hosts --name hive-ms-init` (здесь аргумент `hive-ms-init` - имя созданного кластера Data Proc).

Пример последовательности команд для входа на мастер-узел кластера Data Proc:

```
$ yc dataproc cluster list-hosts --name hive-ms-init
+------------------------------------------------------+----------------------+-------------+----------------------+--------+
|                         NAME                         | COMPUTE INSTANCE ID  |    ROLE     |    SUBCLUSTER ID     | HEALTH |
+------------------------------------------------------+----------------------+-------------+----------------------+--------+
| rc1b-dataproc-m-v2vn8bjkhogp07t1.mdb.yandexcloud.net | epdke0h9lv2717o6u122 | MASTERNODE  | c9qee4kkui19nd87t81q | ALIVE  |
| rc1b-dataproc-g-otal.mdb.yandexcloud.net             | epdirroqc79157797f7q | COMPUTENODE | c9qur5s18or19o7tr5h0 | ALIVE  |
+------------------------------------------------------+----------------------+-------------+----------------------+--------+

$ ssh gw1
Welcome to Ubuntu 22.04.1 LTS (GNU/Linux 5.15.0-56-generic x86_64)
...
demo@gw1:~$ ssh ubuntu@rc1b-dataproc-m-v2vn8bjkhogp07t1.mdb.yandexcloud.net
Welcome to Ubuntu 20.04.5 LTS (GNU/Linux 5.4.0-132-generic x86_64)
...
ubuntu@rc1b-dataproc-m-v2vn8bjkhogp07t1:~$ 
```

В примере выше `gw1` - имя промежуточного хоста, имеющего доступ к защищённой сети, в которой размещён временный кластер Data Proc.

Инициализация базы данных Hive Metastore осуществляется с помощью [инструмента schematool](https://cwiki.apache.org/confluence/display/Hive/Hive+Schema+Tool), который установлен в каталоге `/lib/hive/bin` мастер-узла кластера Data Proc. Пример команды инициализации приведён ниже:

```bash
/lib/hive/bin/schematool -dbType postgres -initSchema \
    -url 'jdbc:postgresql://host:port/hive?targetServerType=master&ssl=true&sslmode=require' \
    -userName 'hive' -passWord 'passw0rd'
```

Значение параметра `url` можно получить на странице управляемого сервиса PostgreSQL, нажав на кнопку "Подключится" в верхней правой части экрана, и выбрав вариант для Java. При этом необходимо учитывать, что атрибут `sslmode` в значении URL может потребовать замены со значения `verify-full`, используемого по умолчанию, на значение `require`, как указано в примере команды выше. Режим `verify-full` требует наличия на стороне клиента сертификата центра регистрации (CA Certificate), что требует дополнительной настройки, выходящей за рамки этой инструкции.

После успешной инициализации базы данных инструмент schematool выводит следующие сообщения:

```
Initialization script completed
schemaTool completed
```

Необходимо убедиться в отсутствии любых сообщений об ошибках в выводе schematool, после чего инициализацию базы данных Hive Metastore можно считать завершённой.

Если для инициализации базы данных Hive Metastore создавался временный кластер Data Proc, то его можно удалить средствами  Web-консоли либо следующей командой:

```bash
yc dataproc cluster delete hive-ms-init
```

## 3. Запуск кластеров Data Proc с использованием внешней базы данных Hive Metastore

Необходимые настройки для работы Apache Hive с использованием внешней базы данных Metastore описаны в [документации Hive](https://cwiki.apache.org/confluence/display/Hive/AdminManual+Metastore+Administration#AdminManualMetastoreAdministration-RemoteMetastoreDatabase). Настройки описывают параметры подключения к базе данных и включают в себя следуюшие свойства:
* `javax.jdo.option.ConnectionURL` - JDBC URL для подключения к базе данных (см. предыдущий раздел и пояснения по формированию параметра `url` для инструмента schematool);
* `javax.jdo.option.ConnectionDriverName` - имя класса JDBC драйвера (для PostgreSQL - значение `org.postgresql.Driver`);
* `javax.jdo.option.ConnectionUserName` - логин пользователя для подключения (обычно значение `hive`);
* `javax.jdo.option.ConnectionPassword` - пароль пользователя.

Параметры компонентов Data Proc задаются с помощью свойств кластера, в данном случае применительно к компоненту `hive`. Ниже приведён пример команды создания автомасштабируемого кластера Data Proc, использующего внутренний сервис Hive Metastore и внешнюю базу данных Hive Metastore:

```bash
YC_SUBNET=default-ru-central1-b
YC_BUCKET=dproc1
yc dataproc cluster create hive-sample-cluster \
  --zone ru-central1-b \
  --service-account-name dp1 \
  --version 2.0.58 --ui-proxy \
  --services hdfs,yarn,tez,mapreduce,hive \
  --bucket ${YC_BUCKET} \
  --subcluster name="master",role='masternode',resource-preset='s2.medium',disk-type='network-ssd',disk-size=150,hosts-count=1,subnet-name=${YC_SUBNET} \
  --subcluster name="data",role='datanode',resource-preset='s2.xlarge',disk-type='network-ssd',disk-size=500,hosts-count=1,max-hosts-count=1,subnet-name=${YC_SUBNET} \
  --subcluster name="compute",role='computenode',resource-preset='s2.xlarge',disk-type='network-ssd-nonreplicated',disk-size=186,hosts-count=1,max-hosts-count=8,subnet-name=${YC_SUBNET} \
  --ssh-public-keys-file ssh-keys.tmp \
  --property core:fs.s3a.committer.name=directory \
  --property core:fs.s3a.committer.staging.conflict-mode=append \
  --property core:mapreduce.outputcommitter.factory.scheme.s3a=org.apache.hadoop.fs.s3a.commit.S3ACommitterFactory \
  --property hive:javax.jdo.option.ConnectionURL='jdbc:postgresql://host:port/hive?targetServerType=master&ssl=true&sslmode=require' \
  --property hive:javax.jdo.option.ConnectionDriverName=org.postgresql.Driver \
  --property hive:javax.jdo.option.ConnectionUserName=hive \
  --property hive:javax.jdo.option.ConnectionPassword='passw0rd' \
  --property hive:hive.metastore.warehouse.dir=s3a://${YC_BUCKET}/warehouse \
  --property hive:hive.exec.compress.output=true
```

Необходимый комплект компонентов Data Proc в примере выше включает в себя:
* собственно сервис Hive;
* YARN и Tez, для управления вычислительными ресурсами;
* HDFS и MapReduce, от которых сервис Hive сейчас зависит - они необходимы даже при хранении данных в Object Storage.

Для эффективной записи данных в Object Storage в примере выше включён S3A Committer в режиме `directory`.

По умолчанию базы данных и таблицы Hive размещаются в бакете Object Storage, что установлено с помощью свойства `hive.metastore.warehouse.dir`.

Проверить работоспособность Hive можно путём запуска клиента `hive cli` или `beeline` на мастер-узле кластера Data Proc и выполнения команд для создания таблицы, вставки в таблицу данных и проверки выборки из таблицы. Пример действий по проверке работоспособности Hive приведён ниже.

```
$ yc dataproc cluster list
+----------------------+---------------------+---------------------+--------+---------+
|          ID          |        NAME         |     CREATED AT      | HEALTH | STATUS  |
+----------------------+---------------------+---------------------+--------+---------+
| c9qhol07vtdmckpj1mo9 | hive-sample-cluster | 2022-12-15 10:21:58 | ALIVE  | RUNNING |
+----------------------+---------------------+---------------------+--------+---------+

$ yc dataproc cluster list-hosts --name=hive-sample-cluster
+------------------------------------------------------+----------------------+-------------+----------------------+--------+
|                         NAME                         | COMPUTE INSTANCE ID  |    ROLE     |    SUBCLUSTER ID     | HEALTH |
+------------------------------------------------------+----------------------+-------------+----------------------+--------+
| rc1b-dataproc-d-0jdg23hq790duz5b.mdb.yandexcloud.net | epd9c7av31jjjf0jnv0f | DATANODE    | c9q2k4dev49aavgk8i7l | ALIVE  |
| rc1b-dataproc-m-iyzhjdcj90ktrhty.mdb.yandexcloud.net | epdrtasd2ab2r4lm8tsq | MASTERNODE  | c9q7lkkkmjahooh8kq0c | ALIVE  |
| rc1b-dataproc-g-itat.mdb.yandexcloud.net             | epdp37t2ik2173rl3r62 | COMPUTENODE | c9qec3qu0rul015m37r5 | ALIVE  |
+------------------------------------------------------+----------------------+-------------+----------------------+--------+

$ ssh gw1
Welcome to Ubuntu 22.04.1 LTS (GNU/Linux 5.15.0-56-generic x86_64)
...
demo@gw1:~$ ssh ubuntu@rc1b-dataproc-m-iyzhjdcj90ktrhty.mdb.yandexcloud.net
Welcome to Ubuntu 20.04.5 LTS (GNU/Linux 5.4.0-132-generic x86_64)
...
ubuntu@rc1b-dataproc-m-iyzhjdcj90ktrhty:~$ hive cli
Hive Session ID = ac15b890-413e-4b0f-85fb-eb0fb2e2512a

Logging initialized using configuration in file:/etc/hive/conf.dist/hive-log4j2.properties Async: true
Hive Session ID = 4a1acdee-9100-47f3-a223-938387b36401

    > create database demo1;
OK
Time taken: 2.698 seconds
hive> use demo1;
OK
Time taken: 0.077 seconds
hive> create table test1(a integer not null, b varchar(100)) stored as orc;
OK
Time taken: 0.928 seconds
hive> insert into test1(a,b) values(1,'One'),(2,'Two'),(3,'Three');
Query ID = ubuntu_20221215103226_23a90afb-5872-4f77-9de7-5908a466fe5f
Total jobs = 1
Launching Job 1 out of 1
Tez session was closed. Reopening...
Session re-established.
Session re-established.
Status: Running (Executing on YARN cluster with App id application_1671099904244_0006)

----------------------------------------------------------------------------------------------
        VERTICES      MODE        STATUS  TOTAL  COMPLETED  RUNNING  PENDING  FAILED  KILLED  
----------------------------------------------------------------------------------------------
Map 1 .......... container     SUCCEEDED      1          1        0        0       0       0  
Reducer 2 ...... container     SUCCEEDED      1          1        0        0       0       0  
----------------------------------------------------------------------------------------------
VERTICES: 02/02  [==========================>>] 100%  ELAPSED TIME: 8.10 s     
----------------------------------------------------------------------------------------------
Loading data to table demo1.test1
OK
Time taken: 19.697 seconds
hive> select * from test1;
OK
1	One
2	Two
3	Three
Time taken: 0.401 seconds, Fetched: 3 row(s)
hive> 
```
