package no.ssb.dapla.parquet;

import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

class LongRunningFileTest {

    @Disabled("Not meant to be run on a regular basis")
    @Test
    public void writeLargeJson() throws IOException {

        String jsonTemplate = """
                {
                    "person": {
                        "name": {
                            "firstName": "Donald %d",
                            "surname": "Duck"
                        },
                        "addresses": [
                            {
                                "streetName": "Duckburg Lane 1",
                                "zipCode": "123"
                            },
                            {
                                "streetName": "Andedammen",
                                "zipCode": "321"
                            }
                        ],
                        "aliases": [
                            "Maui Mallard",
                            "Frank Duck"
                        ]
                    }
                }
                """;

        int n = 10000000;

        FileChannel channel = FileChannel.open(Path.of(String.format("/tmp/%s-%d.json", System.currentTimeMillis(), n)), StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
        channel.write(ByteBuffer.wrap("[".getBytes(StandardCharsets.UTF_8)));
        for (int i = 0; i < n; i++) {
            channel.write(ByteBuffer.wrap(String.format(jsonTemplate, i).getBytes(StandardCharsets.UTF_8)));
            if (i + 1 < n) {
                channel.write(ByteBuffer.wrap(",".getBytes(StandardCharsets.UTF_8)));
            }
        }
        channel.write(ByteBuffer.wrap("]".getBytes(StandardCharsets.UTF_8)));
    }

    @Disabled("Not meant to be run on a regular basis")
    @Test
    public void readJsonWriteParquet() throws IOException {

        String schemaString = """
                message root {
                   required group person {
                       required group name {
                            required binary firstName (STRING);
                            required binary surname (STRING);
                       }
                       optional group addresses (LIST) {
                           repeated group array {
                               required binary streetName (STRING);
                               required binary zipCode (STRING);
                           }
                       }
                       optional group aliases (LIST) {
                            repeated binary array (STRING);
                       }
                   }
                }
                """;

        MessageType schema = MessageTypeParser.parseMessageType(schemaString);

        FileInputStream inputStream = new FileInputStream("/tmp/1591961879973-10000000.json");

        File.writeJson(inputStream, Path.of(String.format("/tmp/%s.parquet", System.currentTimeMillis())), schema);
    }

    @Disabled("Not meant to be run on a regular basis")
    @Test
    public void thatReadColumnWorks() throws IOException {
        SeekableByteChannel fileChannel = FileChannel.open(Path.of("/tmp/1592205620655.parquet"));

        Instant start = Instant.now();
        List<Object> objects = File.readColumn(fileChannel, "/person/name/firstName");
        Duration duration = Duration.between(start, Instant.now());

        System.out.println("Read " + objects.size() + " columns " + duration.toMillis() + " ms");
    }
}
