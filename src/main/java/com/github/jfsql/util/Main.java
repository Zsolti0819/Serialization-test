package com.github.jfsql.util;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import org.instancio.Instancio;

public class Main {

    private static final String CSV_FILE_PATH = "src/main/resources/output/csv_version.csv";
    private static final String CSV_FILE_PATH2 = "src/main/resources/output/csv_version2.csv";
    private static final String JSON_FILE_PATH = "src/main/resources/output/json_version.json";
    private static final String JSON_FILE_PATH2 = "src/main/resources/output/json_version2.json";
    private static final String XML_FILE_PATH = "src/main/resources/output/xml_version.xml";
    private static final String XML_FILE_PATH2 = "src/main/resources/output/xml_version2.xml";
    private static final String YAML_FILE_PATH = "src/main/resources/output/yaml_version.yaml";
    private static final String YAML_FILE_PATH2 = "src/main/resources/output/yaml_version2.yaml";

    private static final int NUMBER_OF_RANDOM_OBJECTS = 2000000;
    private static final List<RandomClass> RANDOM_VALUE_OBJECT_LIST = populateListWithRandomObjects();

    private static List<RandomClass> populateListWithRandomObjects() {
        System.out.println("Populating the list with random value objects");
        return Instancio.stream(RandomClass.class)
            .limit(NUMBER_OF_RANDOM_OBJECTS)
            .collect(Collectors.toList());
    }

    public static void compareByMemoryMappedFiles(final Path path1, final Path path2) throws IOException {
        try (final RandomAccessFile randomAccessFile1 = new RandomAccessFile(path1.toFile(), "r");
            final RandomAccessFile randomAccessFile2 = new RandomAccessFile(path2.toFile(), "r")) {

            final FileChannel ch1 = randomAccessFile1.getChannel();
            final FileChannel ch2 = randomAccessFile2.getChannel();
            if (ch1.size() != ch2.size()) {
                System.out.println(
                    "Files '" + path1.getFileName() + "' and '" + path2.getFileName() + "' were NOT identical");
                return;
            }
            final long size = ch1.size();
            final MappedByteBuffer m1 = ch1.map(FileChannel.MapMode.READ_ONLY, 0L, size);
            final MappedByteBuffer m2 = ch2.map(FileChannel.MapMode.READ_ONLY, 0L, size);

            if (m1.equals(m2)) {
                System.out.println(
                    "Files '" + path1.getFileName() + "' and '" + path2.getFileName() + "' were identical");
            } else {
                System.out.println(
                    "Files '" + path1.getFileName() + "' and '" + path2.getFileName() + "' were NOT identical");
            }
        }
    }

    private static void csv() throws IOException {
        final CsvMapper csvMapper = new CsvMapper();
        final CsvSchema csvSchema = csvMapper.schemaFor(RandomClass.class);
        final long serializationStartTime = System.nanoTime();
        csvMapper.writer(csvSchema.withUseHeader(true)).writeValue(new File(CSV_FILE_PATH), RANDOM_VALUE_OBJECT_LIST);
        final long serializationEndTime = System.nanoTime() - serializationStartTime;
        System.out.println("csv serialization duration: " + serializationEndTime / 1000000000 + "s");
        final long deserializationStartTime = System.nanoTime();
        final MappingIterator<RandomClass> iterator;
        iterator = csvMapper.readerFor(RandomClass.class).with(CsvSchema.emptySchema().withHeader())
            .readValues(new File(CSV_FILE_PATH));
        final List<RandomClass> randomClasses = iterator.readAll();
        final long deserializationEndTime = System.nanoTime() - deserializationStartTime;
        System.out.println("csv deserialization duration: " + deserializationEndTime / 1000000000 + "s");
        final long reserializationStartTime = System.nanoTime();
        csvMapper.writer(csvSchema.withUseHeader(true)).writeValue(new File(CSV_FILE_PATH2), randomClasses);
        final long reserializationEndTime = System.nanoTime() - reserializationStartTime;
        System.out.println("csv reserialization duration: " + reserializationEndTime / 1000000000 + "s");
        compareByMemoryMappedFiles(Path.of(CSV_FILE_PATH), Path.of(CSV_FILE_PATH2));
    }

    private static void generic(final String format, final ObjectMapper objectMapper, final String filePath,
        final String filePath2) throws IOException {
        final long serializationStartTime = System.nanoTime();
        objectMapper.writerWithDefaultPrettyPrinter().writeValue(new File(filePath), RANDOM_VALUE_OBJECT_LIST);
        final long serializationEndTime = System.nanoTime() - serializationStartTime;
        System.out.println(format + " serialization duration: " + serializationEndTime / 1000000000 + "s");
        final long deserializationStartTime = System.nanoTime();
        final MappingIterator<RandomClass> iterator;
        iterator = objectMapper.readerFor(RandomClass.class).readValues(new File(filePath));
        final List<RandomClass> randomClasses = iterator.readAll();
        final long deserializationEndTime = System.nanoTime() - deserializationStartTime;
        System.out.println(format + " deserialization duration: " + deserializationEndTime / 1000000000 + "s");
        final long reserializationStartTime = System.nanoTime();
        objectMapper.writerWithDefaultPrettyPrinter().writeValue(new File(filePath2), randomClasses);
        final long reserializationEndTime = System.nanoTime() - reserializationStartTime;
        System.out.println(format + " reserialization duration: " + reserializationEndTime / 1000000000 + "s");
        compareByMemoryMappedFiles(Path.of(filePath), Path.of(filePath2));
    }

    private static void json() throws IOException {
        generic("json", new JsonMapper(), JSON_FILE_PATH, JSON_FILE_PATH2);
    }

    private static void xml() throws IOException {
        generic("xml", new XmlMapper(), XML_FILE_PATH, XML_FILE_PATH2);
    }

    private static void yaml() throws IOException {
        generic("yaml", new YAMLMapper(new YAMLFactory().disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER)),
            YAML_FILE_PATH, YAML_FILE_PATH2);
    }

    public static void main(final String[] args) throws IOException {
        Files.createDirectories(Path.of("src/main/resources/output/"));
        System.out.println("Starting serialization processes");
        csv();
        json();
        xml();
        yaml();
    }
}

