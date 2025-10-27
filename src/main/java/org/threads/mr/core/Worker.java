package org.threads.mr.core;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.threads.mr.task.MapTask;
import org.threads.mr.task.ReduceTask;
import org.threads.mr.task.ShutdownTask;
import org.threads.mr.task.Task;
import org.threads.mr.util.MapReduceUtil;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.BiFunction;
import java.util.stream.Stream;

@RequiredArgsConstructor
public class Worker implements Runnable {
    @NonNull
    private final Coordinator coordinator;

    @NonNull
    private final BiFunction<String, String, List<KeyValue>> mapFunction;
    @NonNull
    private final BiFunction<String, List<String>, String> reduceFunction;

    @NonNull
    private final String outputPath;

    @Override
    public void run() {
        System.out.println("Worker " + Thread.currentThread().getName() + " started");

        while (true) {
            Task task = coordinator.getTask();

            if (task instanceof MapTask mapTask) {
                handleMapTask(mapTask);
            } else if (task instanceof ReduceTask reduceTask) {
                handleReduceTask(reduceTask);
            } else if (task instanceof ShutdownTask) {
                break;
            }
        }

        System.out.println("Worker " + Thread.currentThread().getName() + " finished");
    }

    private void handleMapTask(@NonNull MapTask mapTask) {
        try {
            String content = Files.readString(Path.of(mapTask.fileName()));
            List<KeyValue> intermediatePairs = mapFunction.apply(mapTask.fileName(), content);

            Map<Integer, List<KeyValue>> buckets = new HashMap<>();
            for (int i = 0; i < mapTask.reduceTasksNumber(); i++) {
                buckets.put(i, new ArrayList<>());
            }

            for (KeyValue pair : intermediatePairs) {
                // masking off the sign bit to avoid NPE if pair.key().hashCode() is Integer.MIN_VALUE
                int bucket = (pair.key().hashCode() & 0x7fffffff) % mapTask.reduceTasksNumber();
                buckets.get(bucket).add(pair);
            }

            List<String> generatedFiles = new ArrayList<>();
            for (Map.Entry<Integer, List<KeyValue>> entry : buckets.entrySet()) {
                int reduceTaskNumber = entry.getKey();
                String fileName = MapReduceUtil.formatIntermediateFileName(mapTask.taskId(), reduceTaskNumber);

                List<String> lines = entry.getValue().stream()
                    .map(kv -> kv.key() + "\t" + kv.value())
                    .toList();

                Files.write(Path.of(fileName), lines);
                generatedFiles.add(fileName);
            }

            coordinator.completeMapTask(generatedFiles);
        } catch (IOException e) {
            // в реальном приложении был бы вызов метода координатора (что-нибудь типа coordinator.failTask(mapTask);)
            throw new RuntimeException("Worker failed on map task " + mapTask.taskId(), e);
        }

    }

    private void handleReduceTask(@NonNull ReduceTask reduceTask) {
        try {
            Map<String, List<String>> groupedByKey = new TreeMap<>();

            for (String fileName : reduceTask.intermediateFiles()) {
                try (Stream<String> lines = Files.lines(Path.of(fileName))) {
                    lines.forEach(line -> {
                        String[] parts = line.split("\t", 2);
                        if (parts.length == 2) {
                            groupedByKey.computeIfAbsent(parts[0], k -> new ArrayList<>()).add(parts[1]);
                        }
                    });
                }
            }

            List<String> outputLines = new ArrayList<>();
            for (Map.Entry<String, List<String>> entry : groupedByKey.entrySet()) {
                String result = reduceFunction.apply(entry.getKey(), entry.getValue());
                outputLines.add(entry.getKey() + " " + result);
            }

            Path filePath = Paths.get(outputPath, String.format("mr-out-%d", reduceTask.taskId()));
            Files.createDirectories(filePath.getParent());
            Files.write(filePath, outputLines, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);

            coordinator.completeReduceTask();
        } catch (IOException e) {
            // тут тоже был бы coordinator.failTask(reduceTask);
            throw new RuntimeException("Worker failed on reduce task " + reduceTask.taskId(), e);
        }
    }
}
