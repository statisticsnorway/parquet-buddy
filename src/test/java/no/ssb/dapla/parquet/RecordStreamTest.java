package no.ssb.dapla.parquet;

import org.apache.commons.io.FileUtils;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class RecordStreamTest {

    private Path testDir;

    @BeforeEach
    void setUp() throws IOException {
        testDir = Files.createTempDirectory("DataTest");
    }

    @AfterEach
    void tearDown() throws IOException {
        FileUtils.deleteDirectory(testDir.toFile());
    }

    @Test
    void thatReadWorks() throws IOException {
        MessageType schema = MessageTypeParser.parseMessageType("""
                message root {
                   required group person {
                       required binary id (UTF8);
                   }
                }
                """);

        String json = """
                {
                    "person": {
                        "id": "%s"
                    }
                }                
                """;

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        outputStream.writeBytes("[".getBytes());
        for (int i = 0; i < 10; i++) {
            outputStream.writeBytes(String.format(json, i).getBytes());
            if (i == 9) {
                continue;
            }
            outputStream.writeBytes(",".getBytes());
        }
        outputStream.writeBytes("]".getBytes());

        Path path = testDir.resolve(Path.of("thatReadWorks.parquet"));
        File.writeJson(new ByteArrayInputStream(outputStream.toByteArray()), path, schema);

        List<String> ids = new ArrayList<>();
        try (RecordStream stream = new RecordStream(FileChannel.open(path), schema)) {
            Map<String, Object> next;
            while ((next = stream.read()) != null) {
                ids.add((String) ((Map<String, Object>) next.get("person")).get("id"));
            }
        }
        assertThat(ids).containsExactly("0", "1", "2", "3", "4", "5", "6", "7", "8", "9");
    }
}
