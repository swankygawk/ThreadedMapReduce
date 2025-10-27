package org.threads.mr.task;

import java.util.List;

public record ReduceTask(
    int taskId,
    List<String> intermediateFiles
) implements Task {}
