package no.ssb.dapla.parquet;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
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
import java.util.Set;
import java.util.UUID;

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
                   optional group addresses (LIST) {
                       repeated group array {
                           required binary streetName (STRING);
                       }
                   }
                }
                """);

        String json = """
                {
                    "person": {
                        "id": "%s"
                    },
                    addresses: [
                        {
                            "streetName": "Duckburg Lane %s"
                        }
                    ]
                }                
                """;

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        outputStream.writeBytes("[".getBytes());
        for (int i = 0; i < 10; i++) {
            outputStream.writeBytes(json.formatted(UUID.randomUUID(), i).getBytes());
            if (i == 9) {
                continue;
            }
            outputStream.writeBytes(",".getBytes());
        }
        outputStream.writeBytes("]".getBytes());

        Path path = testDir.resolve(Path.of("thatReadWorks.parquet"));
        File.writeJson(new ByteArrayInputStream(outputStream.toByteArray()), path, schema);

        List<String> streetNumbers = new ArrayList<>();
        try (
                RecordStream stream = RecordStream.builder(FileChannel.open(path))
                        .withFieldSelectors(Set.of("/addresses/streetName")) //Select only streetName field
                        .withFieldInterceptor((field, value) -> StringUtils.getDigits(value)) //Extract street number from streetName
                        .build()
        ) {
            Map<String, Object> next;
            while ((next = stream.read()) != null) {
                assertThat(next).containsOnlyKeys("addresses");
                streetNumbers.add((String) ((List<Map<String, Object>>) next.get("addresses")).get(0).get("streetName"));
            }
        }
        assertThat(streetNumbers).containsExactly("0", "1", "2", "3", "4", "5", "6", "7", "8", "9");
    }
}
