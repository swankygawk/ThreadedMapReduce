package org.threads.mr;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.threads.mr.core.Coordinator;
import org.threads.mr.core.KeyValue;
import org.threads.mr.core.Worker;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

@RequiredArgsConstructor
public class MapReduce {
    @NonNull
    private final String inputPath;
    @NonNull
    private final String outputPath;

    private final int workersNumber;
    private final int reduceTasksNumber;

    @NonNull
    private final BiFunction<String, String, List<KeyValue>> mapFunction;
    @NonNull
    private final BiFunction<String, List<String>, String> reduceFunction;

    public void execute(long timeout, @NonNull TimeUnit unit) {
        ExecutorService executor = Executors.newFixedThreadPool(workersNumber);
        try {
            Coordinator coordinator = new Coordinator(inputPath, reduceTasksNumber, workersNumber);
            for (int i = 0; i < workersNumber; i++) {
                Worker worker = new Worker(coordinator, mapFunction, reduceFunction, outputPath);
                executor.submit(worker);
            }
        } finally {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(timeout, unit)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }
}
