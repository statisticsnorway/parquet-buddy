package no.ssb.dapla.parquet;

import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Path;
import java.util.List;

class DataTest {

    @Test
    public void thatWriteWorks() {
        Path path = Path.of(String.format("/tmp/%s-test-json.parquet", System.currentTimeMillis()));

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

        Data.writeJson(json, path, schema);
    }

    @Test
    public void thatReadColumnWorks() throws IOException {
        SeekableByteChannel fileChannel = FileChannel.open(Path.of("src", "test", "resources", "ske-freg-playground-1589549835139.parquet"));
        List<Object> objects = Data.readColumn(fileChannel, "/dokumentForHendelse/hendelse/egenskapshendelse/kontaktinformasjonForDoedsbo/advokat/personnavn/fornavn");
        System.out.println("Read: " + objects.size() + " columns");
    }

    @Test
    public void thatReadUlidColumnWorks() throws IOException {
        SeekableByteChannel fileChannel = FileChannel.open(Path.of("src", "test", "resources", "ske-freg-playground-1589549835139.parquet"));
        List<Object> objects = Data.readColumn(fileChannel, "/metadata/ulid");
        System.out.println("Read: " + objects.size() + " columns");
    }

    @Test
    public void thatReadFoedselsnummerColumnWorks() throws IOException {
        SeekableByteChannel fileChannel = FileChannel.open(Path.of("src", "test", "resources", "ske-freg-playground-1589549835139.parquet"));
        List<Object> objects = Data.readColumn(fileChannel, "/dokumentForHendelse/hendelse/egenskapshendelse/identifikasjonsnummer/foedselsEllerDNummer");
        System.out.println("Read: " + objects.size() + " columns");
    }

    /**
     * optional group adresselinje (LIST) {
     * repeated binary array (STRING);
     * }
     */
    @Test
    public void thatReadRepeatedBinaryColumnWorks() throws IOException {
        SeekableByteChannel fileChannel = FileChannel.open(Path.of("src", "test", "resources", "ske-freg-playground-1589549835139.parquet"));
        List<Object> objects = Data.readColumn(fileChannel, "/folkeregisterperson/**/postadresseIFrittFormat/adresselinje");
        System.out.println("Read: " + objects.size() + " columns");
    }
}