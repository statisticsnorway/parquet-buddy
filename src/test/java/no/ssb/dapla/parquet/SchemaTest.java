package no.ssb.dapla.parquet;

import org.apache.parquet.schema.MessageType;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SchemaTest {

    @Test
    public void thatCreateProjectionWorks() {

        String input = """
                message root {
                    required group person {
                        required group name {
                            required binary firstname (STRING);
                            optional binary middlename (STRING);
                            required binary surname (STRING);                        
                        }
                        optional group address (LIST) {
                            repeated group array {
                                required group windows (LIST) {
                                    repeated binary array (STRING);
                                }
                                required group building {
                                    required binary number (STRING);
                                    required binary floors (STRING);
                                }
                            }
                        }     
                        required binary gender (STRING);
                    }
                    required group meta {
                        required binary id (STRING);
                    }
                }
                """;

        Set<String> globs = Set.of("**/firstname", "/person/gender");

        String want = """
                message root {
                    required group person {
                        required group name {
                            required binary firstname (STRING);
                        }
                        required binary gender (STRING);
                    }
                }                
                """;

        String output = Schema.createProjection(input, globs).toString();
        assertThat(output).isEqualToIgnoringWhitespace(want);
    }

    @Test
    public void thatCreateProjectionWorksWhenGlobsAreEmpty() {
        String input = """
                message root {
                    required group person {
                        required group name {
                            required binary firstname (STRING);
                            optional binary middlename (STRING);
                            required binary surname (STRING);                        
                        }
                    }
                }
                """;
        MessageType output = Schema.createProjection(input, new HashSet<>());
        assertThat(output).isNull();
    }

    @Test
    public void thatCreateProjectionWorksWhenNoneOfTheGlobsMatch() {
        String input = """
                message root {
                    required group person {
                        required group name {
                            required binary firstname (STRING);
                            optional binary middlename (STRING);
                            required binary surname (STRING);                        
                        }
                    }
                }
                """;
        Set<String> globs = Set.of("/person/name/lastname");
        MessageType output = Schema.createProjection(input, globs);
        assertThat(output).isNull();
    }

    @Test
    public void thatCreateProjectionWorksWhenGlobMatchesEverything() {
        String input = """
                message root {
                    required group person {
                        required group name {
                            required binary firstname (STRING);
                            optional binary middlename (STRING);
                            required binary surname (STRING);                        
                        }
                    }
                }
                """;

        Set<String> globs = Set.of("**");

        String want = """
                message root {
                    required group person {
                        required group name {
                            required binary firstname (STRING);
                            optional binary middlename (STRING);
                            required binary surname (STRING);                        
                        }
                    }
                }
                """;

        String output = Schema.createProjection(input, globs).toString();
        assertThat(output).isEqualToIgnoringWhitespace(want);
    }

    @Test
    public void thatCreateProjectionFailsWhenGlobsIsNull() {
        String input = """
                message root {
                    required group person {
                        required group name {
                            required binary firstname (STRING);
                            optional binary middlename (STRING);
                            required binary surname (STRING);                        
                        }
                    }
                }
                """;
        assertThatThrownBy(() -> Schema.createProjection(input, null)).isInstanceOf(NullPointerException.class);
    }

    @Test
    public void thatCreateProjectionFailsWhenSchemaIsInvalid() {
        assertThatThrownBy(() -> Schema.createProjection("foo", Set.of(""))).isInstanceOf(IllegalArgumentException.class);
    }
}
