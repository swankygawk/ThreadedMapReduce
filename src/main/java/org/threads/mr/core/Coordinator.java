package org.threads.mr.core;

import lombok.NonNull;
import org.threads.mr.task.MapTask;
import org.threads.mr.task.ReduceTask;
import org.threads.mr.task.ShutdownTask;
import org.threads.mr.task.Task;
import org.threads.mr.util.MapReduceUtil;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

public class Coordinator {
    private final int workersNumber;

    private final AtomicInteger mapTasksRemaining;
    private final AtomicInteger reduceTasksRemaining;

    private final BlockingQueue<Task> taskQueue = new LinkedBlockingQueue<>();
    private final ConcurrentMap<Integer, Queue<String>> intermediateFiles = new ConcurrentHashMap<>();

    public Coordinator(
        @NonNull String inputPath,
        int reduceTasksNumber,
        int workersNumber
    ) {
        this.workersNumber = workersNumber;

        List<String> inputFiles = findInputFiles(inputPath);

        this.mapTasksRemaining = new AtomicInteger(inputFiles.size());
        this.reduceTasksRemaining = new AtomicInteger(reduceTasksNumber);

        if (inputFiles.isEmpty()) {
            System.err.println("No input files found in " + inputPath + ". Shutting down");
            finishJob();
            return;
        }

        for (int i = 0; i < reduceTasksNumber; i++) {
            intermediateFiles.put(i, new ConcurrentLinkedQueue<>());
        }

        int taskId = 0;
        for (String fileName : inputFiles) {
            taskQueue.add(new MapTask(taskId++, fileName, reduceTasksNumber));
        }
    }

    public Task getTask() {
        try {
            return taskQueue.take();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return ShutdownTask.INSTANCE;
        }
    }

    public void completeMapTask(@NonNull List<String> generatedFiles) {
        for (String fileName : generatedFiles) {
            int reduceTaskId = MapReduceUtil.parseReduceTaskIdFromFileName(fileName);
            intermediateFiles.get(reduceTaskId).add(fileName);
        }

        if (mapTasksRemaining.decrementAndGet() == 0) {
            startReducePhase();
        }
    }

    public void completeReduceTask() {
        if (reduceTasksRemaining.decrementAndGet() == 0) {
            finishJob();
        }
    }

    private void startReducePhase() {
        System.out.println("Map tasks completed. Starting reduce phase");
        for (int i = 0; i < reduceTasksRemaining.get(); i++) {
            taskQueue.add(new ReduceTask(i, new ArrayList<>(intermediateFiles.get(i))));
        }
    }

    private void finishJob() {
        System.out.println("Reduce tasks completed. Finishing");
        for (int i = 0; i < workersNumber; i++) {
            taskQueue.add(ShutdownTask.INSTANCE);
        }
    }

    private List<String> findInputFiles(@NonNull String inputPath) {
        try (Stream<Path> pathStream = Files.list(Paths.get(inputPath))) {
            return pathStream
                .filter(Files::isRegularFile)
                .map(Path::toString)
                .toList();
        } catch (IOException e) {
            System.err.println("Failed to read input directory: " + e.getMessage());
            return Collections.emptyList();
        }
    }
}
