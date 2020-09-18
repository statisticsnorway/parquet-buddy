package no.ssb.dapla.parquet;

import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RecordTest {

    @Test
    void thatNormalizeFailsIfGroupIsNull() {
        assertThatThrownBy(() -> Record.normalize(null)).isInstanceOf(NullPointerException.class);
    }

    @Test
    void thatGroupToMapWorksWithSimpleGroup() {
        MessageType schema = MessageTypeParser.parseMessageType("""
                message root {
                   optional binary name (STRING);
                }
                """);

        SimpleGroup root = new SimpleGroup(schema);
        root.add("name", "Donald");

        HashMap<String, Object> wantMap = new HashMap<>();
        wantMap.put("name", "Donald");

        Map<String, Object> gotRootMap = Record.normalize(root);
        assertThat(gotRootMap).isEqualTo(wantMap);
    }

    @Test
    void thatGroupToMapWorksWithEmptyGroup() {
        MessageType schema = MessageTypeParser.parseMessageType("""
                message root {
                   optional binary name (STRING);
                }
                """);

        Map<String, Object> gotRootMap = Record.normalize(new SimpleGroup(schema));
        assertThat(gotRootMap).isEqualTo(new HashMap<String, Object>());
    }

    @Test
    void thatGroupToMapWorksWithEmptyArray() {
        MessageType schema = MessageTypeParser.parseMessageType("""
                message root {
                   optional group aliases (LIST) {
                        repeated binary array (STRING);
                   }
                }
                """);

        GroupType aliasesType = (GroupType) schema.getType("aliases");

        SimpleGroup aliases = new SimpleGroup(aliasesType);

        SimpleGroup root = new SimpleGroup(schema);
        root.add("aliases", aliases);

        List<Object> aliasesList = new ArrayList<>();

        Map<String, Object> wantRootMap = new HashMap<>();
        wantRootMap.put("aliases", aliasesList);

        Map<String, Object> gotRootMap = Record.normalize(root);
        assertThat(gotRootMap).isEqualTo(wantRootMap);
    }

    @Test
    void thatGroupToMapWorksWithArray() {
        MessageType schema = MessageTypeParser.parseMessageType("""
                message root {
                   optional group aliases (LIST) {
                        repeated binary array (STRING);
                   }
                }
                """);

        GroupType aliasesType = (GroupType) schema.getType("aliases");

        SimpleGroup aliases = new SimpleGroup(aliasesType);
        aliases.add("array", "Maui Mallard");
        aliases.add("array", "Frank Duck");

        SimpleGroup root = new SimpleGroup(schema);
        root.add("aliases", aliases);

        List<Object> aliasesList = new ArrayList<>();
        aliasesList.add("Maui Mallard");
        aliasesList.add("Frank Duck");

        Map<String, Object> wantRootMap = new HashMap<>();
        wantRootMap.put("aliases", aliasesList);

        Map<String, Object> gotRootMap = Record.normalize(root);
        assertThat(gotRootMap).isEqualTo(wantRootMap);
    }

    @Test
    void thatInterceptorWorks() {
        MessageType schema = MessageTypeParser.parseMessageType("""
                message root {
                   required group person {
                       required group name {
                            required binary firstName (STRING);
                            required binary surname (STRING);
                       }
                   }
                }
                """);

        GroupType personType = (GroupType) schema.getType("person");
        GroupType nameType = (GroupType) personType.getType("name");

        SimpleGroup name = new SimpleGroup(nameType);
        name.add("firstName", Base64.getEncoder().encodeToString("Donald".getBytes()));
        name.add("surname", Base64.getEncoder().encodeToString("Duck".getBytes()));

        SimpleGroup person = new SimpleGroup(personType);
        person.add("name", name);

        SimpleGroup root = new SimpleGroup(schema);
        root.add("person", person);

        Map<String, Object> nameMap = new HashMap<>();
        nameMap.put("firstName", "Donald");
        nameMap.put("surname", "Duck");

        Map<String, Object> personMap = new HashMap<>();
        personMap.put("name", nameMap);

        Map<String, Object> wantRootMap = new HashMap<>();
        wantRootMap.put("person", personMap);

        Map<String, Object> gotRootMap = Record.normalize(root, (field, value) -> new String(Base64.getDecoder().decode(value)));
        assertThat(gotRootMap).isEqualTo(wantRootMap);
    }

    @Test
    void thatGroupToMapWorks() {
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

        GroupType personType = (GroupType) schema.getType("person");
        GroupType nameType = (GroupType) personType.getType("name");
        GroupType addressesType = (GroupType) personType.getType("addresses");
        GroupType addressesArrayType = (GroupType) addressesType.getType("array");
        GroupType aliasesType = (GroupType) personType.getType("aliases");

        SimpleGroup name = new SimpleGroup(nameType);
        name.add("firstName", "Donald");
        name.add("surname", "Duck");

        SimpleGroup firstAddress = new SimpleGroup(addressesArrayType);
        firstAddress.add("streetName", "Duckburg Lane 1");
        firstAddress.add("zipCode", "123");

        SimpleGroup secondAddress = new SimpleGroup(addressesArrayType);
        secondAddress.add("streetName", "Andedammen");
        secondAddress.add("zipCode", "321");

        SimpleGroup addresses = new SimpleGroup(addressesType);
        addresses.add("array", firstAddress);
        addresses.add("array", secondAddress);

        SimpleGroup aliases = new SimpleGroup(aliasesType);
        aliases.add("array", "Maui Mallard");
        aliases.add("array", "Frank Duck");

        SimpleGroup person = new SimpleGroup(personType);
        person.add("name", name);
        person.add("addresses", addresses);
        person.add("aliases", aliases);

        SimpleGroup root = new SimpleGroup(schema);
        root.add("person", person);

        String groupString = """
                person
                  name
                    firstName: Donald
                    surname: Duck
                  addresses
                    array
                      streetName: Duckburg Lane 1
                      zipCode: 123
                    array
                      streetName: Andedammen
                      zipCode: 321
                  aliases
                    array: Maui Mallard
                    array: Frank Duck
                """;

        assertThat(root.toString()).isEqualTo(groupString);

        Map<String, Object> nameMap = new HashMap<>();
        nameMap.put("firstName", "Donald");
        nameMap.put("surname", "Duck");

        Map<String, Object> firstAddressMap = new HashMap<>();
        firstAddressMap.put("streetName", "Duckburg Lane 1");
        firstAddressMap.put("zipCode", "123");

        Map<String, Object> secondAddressMap = new HashMap<>();
        secondAddressMap.put("streetName", "Andedammen");
        secondAddressMap.put("zipCode", "321");

        List<Object> addressesList = new ArrayList<>();
        addressesList.add(firstAddressMap);
        addressesList.add(secondAddressMap);

        List<Object> aliasesList = new ArrayList<>();
        aliasesList.add("Maui Mallard");
        aliasesList.add("Frank Duck");

        Map<String, Object> personMap = new HashMap<>();
        personMap.put("name", nameMap);
        personMap.put("addresses", addressesList);
        personMap.put("aliases", aliasesList);

        Map<String, Object> wantRootMap = new HashMap<>();
        wantRootMap.put("person", personMap);

        Map<String, Object> gotRootMap = Record.normalize(root);
        assertThat(gotRootMap).isEqualTo(wantRootMap);
    }
}
