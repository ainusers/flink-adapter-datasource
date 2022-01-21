package com.analysis.data.sink.clickhouse.applied;

import java.util.concurrent.ExecutionException;

public interface Sink extends AutoCloseable {
    void put(String message) throws ExecutionException, InterruptedException;
}
