package no.ssb.dapla.parquet;

import org.apache.commons.io.FileUtils;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.json.JSONException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.skyscreamer.jsonassert.JSONAssert;
import org.skyscreamer.jsonassert.JSONCompareMode;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

class FileTest {

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
    void thatWriteWorks() {
        Path path = testDir.resolve(Path.of("thatWriteWorks.parquet"));

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

        String json = """
                {
                    "person": {
                        "name": {
                            "firstName": "Donald",
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

        File.writeJson(json, path, schema);
    }

    @Disabled("Method under test isn't implemented yet")
    @Test
    void thatReadJsonWorks() throws IOException, JSONException {

        MessageType schema = MessageTypeParser.parseMessageType("""
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
                """);

        String want = """
                {
                    "person": {
                        "name": {
                            "firstName": "Donald",
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

        Path file = Path.of("readJsonWorks.parquet");

        File.writeJson(want, file, schema);

        String got = new String(File.readJson(FileChannel.open(file), schema).readAllBytes());

        JSONAssert.assertEquals(want, got, JSONCompareMode.STRICT);
    }

    @Disabled("Replace me with test utilizing Data.writeJson")
    @Test
    void thatReadColumnWorks() throws IOException {
        SeekableByteChannel fileChannel = FileChannel.open(Path.of("src", "test", "resources", "ske-freg-playground-1589549835139.parquet"));
        List<Object> objects = File.readColumn(fileChannel, "/dokumentForHendelse/hendelse/egenskapshendelse/kontaktinformasjonForDoedsbo/advokat/personnavn/fornavn");
        System.out.println("Read: " + objects.size() + " columns");
    }

    @Disabled("Replace me with test utilizing Data.writeJson")
    @Test
    void thatReadUlidColumnWorks() throws IOException {
        SeekableByteChannel fileChannel = FileChannel.open(Path.of("src", "test", "resources", "ske-freg-playground-1589549835139.parquet"));
        List<Object> objects = File.readColumn(fileChannel, "/metadata/ulid");
        System.out.println("Read: " + objects.size() + " columns");
    }

    @Disabled("Replace me with test utilizing Data.writeJson")
    @Test
    void thatReadFoedselsnummerColumnWorks() throws IOException {
        SeekableByteChannel fileChannel = FileChannel.open(Path.of("src", "test", "resources", "ske-freg-playground-1589549835139.parquet"));
        List<Object> objects = File.readColumn(fileChannel, "/dokumentForHendelse/hendelse/egenskapshendelse/identifikasjonsnummer/foedselsEllerDNummer");
        System.out.println("Read: " + objects.size() + " columns");
    }

    /**
     * optional group adresselinje (LIST) {
     * repeated binary array (STRING);
     * }
     */
    @Disabled("Replace me with test utilizing Data.writeJson")
    @Test
    void thatReadRepeatedBinaryColumnWorks() throws IOException {
        SeekableByteChannel fileChannel = FileChannel.open(Path.of("src", "test", "resources", "ske-freg-playground-1589549835139.parquet"));
        List<Object> objects = File.readColumn(fileChannel, "/folkeregisterperson/**/postadresseIFrittFormat/adresselinje");
        System.out.println("Read: " + objects.size() + " columns");
    }
}
