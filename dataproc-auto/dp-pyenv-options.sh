#! /bin/sh

# Настройки создания кластеров для демонстрации подготовки окружения Python.

YC_VERSION=2.0.61
YC_ZONE=ru-central1-c
YC_SUBNET=default-ru-central1-c
YC_BUCKET=dproc-wh
YC_SA=dp1
YC_MS_URL='jdbc:postgresql://rc1b-pntr3g0uume3q4ob.mdb.yandexcloud.net:6432/hive?targetServerType=master&ssl=true&sslmode=require'
YC_MS_PASS='chahle1Eiqu5BukieZoh'

echo "ecdsa-sha2-nistp256 AAAAE2VjZHNhLXNoYTItbmlzdHAyNTYAAAAIbmlzdHAyNTYAAABBBKbQbtWaYC/XW5efMnhHr0G+6GEl/pCpUmg9+/DpYXYAdqdB67N1EafbsS6JJiI97B+48vwWMJ0iRQ3Ysihg1jk= demo@gw1" >ssh-keys.tmp
