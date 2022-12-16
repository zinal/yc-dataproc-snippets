# Скорость basic-queries и узкие места

Распределение времени:
1. Параллельное задание Spark: 11:29 - 11:43 (14 минут)
2. Подтверждение S3 pending uploads: 11:43 - 11:47 (4 минуты)
3. Регистрация партиций в Hive Metastore, включая сбор размеров через листинги S3: 11:47 - 11:58 (11 минут)

Количество объектов для таблицы после завершения задания:
```
$ find /s3data/wh/demo1.db/megatab -type d | grep tv_day | wc -l
11575
$ find /s3data/wh/demo1.db/megatab -type f | wc -l
11576
```

Пробую профилировать Hive Metastore средствами [Async Profiler](https://github.com/jvm-profiling-tools/async-profiler#wall-clock-profiling).

```bash
ps -ef | grep metastore
sudo ./profiler.sh -e wall -t -i 5ms -d 600 -f /tmp/out-run1.html 4675
```

Логирование операций с бакетом:

```bash
aws s3api --endpoint-url=https://storage.yandexcloud.net put-bucket-logging --bucket dproc-wh --bucket-logging-status='{"LoggingEnabled": {"TargetBucket": "dproc-logs","TargetPrefix": "dproc-wh/"}}'

aws s3api --endpoint-url=https://storage.yandexcloud.net get-bucket-logging --bucket dproc-wh
```