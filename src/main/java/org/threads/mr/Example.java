package org.threads.mr;

import org.threads.mr.core.KeyValue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.stream.Stream;

public class Example {
    public static final String INPUT_DIR = "input_data";
    public static final String OUTPUT_DIR = "output_data";
    public static final int N_WORKERS = 4;
    public static final int M_REDUCERS = 3;

    public static void main(String[] args) throws IOException {
        cleanup();
        setupInputFiles();

        BiFunction<String, String, List<KeyValue>> mapFunction = (fileName, content) ->
            Arrays.stream(content.toLowerCase(Locale.ROOT).split("\\P{L}+"))
                .filter(word -> !word.isBlank())
                .map(word -> new KeyValue(word, "1"))
                .toList();

        BiFunction<String, List<String>, String> reduceFunction = (key, values) ->
            String.valueOf(values.size());

        MapReduce mapReduce = new MapReduce(INPUT_DIR, OUTPUT_DIR, N_WORKERS, M_REDUCERS, mapFunction, reduceFunction);
        mapReduce.execute(1L, TimeUnit.MINUTES);

//        clearIntermediateFiles(); // uncomment to remove intermediate files
    }

    private static void setupInputFiles() throws IOException {
        Files.createDirectories(Paths.get(INPUT_DIR));
        Files.writeString(Paths.get(INPUT_DIR, "example1.txt"), "hello ТЕСТ world hello world goodbye world");
        Files.writeString(Paths.get(INPUT_DIR, "example2.txt"), "hello java ТЕСТ mapreduce java");
        Files.writeString(Paths.get(INPUT_DIR, "example3.txt"), "some text java пример world");
        Files.writeString(Paths.get(INPUT_DIR, "example4.txt"), "ТЕСТ ТЕСТ пример\ngoodbye\ntext");
    }

    private static void cleanup() {
        try {
            clearDir(INPUT_DIR);
            clearDir(OUTPUT_DIR);
            clearIntermediateFiles();
        } catch (IOException e) {
            System.err.println("Error while cleaning up files: " + e.getMessage());
        }
    }

    private static void clearDir(String path) throws IOException {
        Path dir = Paths.get(path);
        if (Files.exists(dir)) {
            try (Stream<Path> dirStream = Files.walk(dir)) {
                dirStream.forEach(p -> {
                    try {
                        Files.delete(p);
                    } catch (IOException ignored) {}
                });
            }
        }
    }

    private static void clearIntermediateFiles() throws IOException {
        try (Stream<Path> intermediateFiles = Files.list(Paths.get("."))) {
            intermediateFiles.filter(p -> p.getFileName().toString().startsWith("mr-"))
                .forEach(p -> {
                    try {
                        Files.delete(p);
                    } catch (IOException ignored) {}
                });
        }
    }
}
