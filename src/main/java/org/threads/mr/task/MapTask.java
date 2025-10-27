package org.threads.mr.task;

public record MapTask(
    int taskId,
    String fileName,
    int reduceTasksNumber
) implements Task {}
