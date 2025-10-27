package org.threads.mr.util;

import lombok.NonNull;

public final class MapReduceUtil {
    public static String formatIntermediateFileName(int mapTaskId, int reduceTaskId) {
        return String.format("mr-%d-%d", mapTaskId, reduceTaskId);
    }

    public static int parseReduceTaskIdFromFileName(@NonNull String fileName) {
        String[] parts = fileName.split("-");
        return Integer.parseInt(parts[parts.length - 1]);
    }
}
