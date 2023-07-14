package ru.yandex.cloud.custom.delta;

/**
 * Original DeltaLake code taken from:
 * https://github.com/delta-io/delta/blob/v2.4.0/storage-s3-dynamodb/src/main/java/io/delta/storage/utils/ThrowingSupplier.java
 */
@FunctionalInterface
public interface ThrowingSupplier<T, E extends Exception> {
    T get() throws E;
}
