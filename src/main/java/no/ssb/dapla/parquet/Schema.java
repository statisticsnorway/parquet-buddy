package no.ssb.dapla.parquet;

import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Type.Repetition;
import org.apache.parquet.schema.Types;

import java.nio.file.FileSystems;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Schema provides utility methods for working with parquet schemas ({@link MessageType})
 */
public class Schema {

    /**
     * createProjection takes in a parquet schema ({@link MessageType}) and creates a subset of that schema by comparing
     * all columns against a given set of glob patterns. Only columns that match at least one glob pattern will be in
     * the projection. For glob syntax see: https://docs.oracle.com/javase/tutorial/essential/io/fileOps.html#glob
     * <p></p>
     * E.g. Given the following schema:
     * <pre>
     * message root {
     *      required group person {
     *          required group name {
     *               required binary firstName (STRING);
     *               optional binary middleName (STRING);
     *               required binary surname (STRING);
     *          }
     *          optional group address (LIST) {
     *              repeated group array {
     *                  required binary streetName (STRING);
     *                  required binary zipCode (STRING);
     *              }
     *          }
     *      }
     * }
     * </pre>
     * <p>
     * That results in these column paths:
     * <pre>
     * /person/name/firstName
     * /person/name/middleName
     * /person/name/surName
     * /person/address/streetName
     * /person/address/zipCode
     * </pre>
     * <p>
     * These globs:
     * <pre>
     * {@literal "**}/firstName", "/person/address/streetName"
     * </pre>
     * <p>
     * Would result in the following projection:
     * <pre>
     * message root {
     *      required group person {
     *          required group name {
     *               required binary firstName (STRING);
     *          }
     *          optional group address (LIST) {
     *              repeated group array {
     *                  required binary streetName (STRING);
     *              }
     *          }
     *      }
     * }
     * </pre>
     *
     * @param schema       The schema as a {@link MessageType}
     * @param globPatterns The globs to match against
     * @return The projection schema, or null if no matches were found
     */
    public static MessageType createProjection(MessageType schema, Set<String> globPatterns) {
        Objects.requireNonNull(globPatterns);

        Set<PathMatcher> pathMatchers = new HashSet<>();
        for (String glob : globPatterns) {
            pathMatchers.add(FileSystems.getDefault().getPathMatcher("glob:" + glob));
        }
        return (MessageType) processGroupType(schema, pathMatchers, "");
    }

    /**
     * createProjection takes in a parquet schema ({@link MessageType}) and creates a subset of that schema by comparing
     * all columns against a given set of glob patterns. Only columns that match at least one glob pattern will be in
     * the projection. For glob syntax see: https://docs.oracle.com/javase/tutorial/essential/io/fileOps.html#glob
     * <p></p>
     * E.g. Given the following schema:
     * <pre>
     * message root {
     *      required group person {
     *          required group name {
     *               required binary firstName (STRING);
     *               optional binary middleName (STRING);
     *               required binary surname (STRING);
     *          }
     *          optional group address (LIST) {
     *              repeated group array {
     *                  required binary streetName (STRING);
     *                  required binary zipCode (STRING);
     *              }
     *          }
     *      }
     * }
     * </pre>
     * <p>
     * That results in these column paths:
     * <pre>
     * /person/name/firstName
     * /person/name/middleName
     * /person/name/surName
     * /person/address/streetName
     * /person/address/zipCode
     * </pre>
     * <p>
     * These globs:
     * <pre>
     * {@literal "**}/firstName", "/person/address/streetName"
     * </pre>
     * <p>
     * Would result in the following projection:
     * <pre>
     * message root {
     *      required group person {
     *          required group name {
     *               required binary firstName (STRING);
     *          }
     *          optional group address (LIST) {
     *              repeated group array {
     *                  required binary streetName (STRING);
     *              }
     *          }
     *      }
     * }
     * </pre>
     *
     * @param schema       The schema as a String
     * @param globPatterns The globs to match against
     * @return The projection schema, or null if no matches were found
     */
    public static MessageType createProjection(String schema, Set<String> globPatterns) {
        return createProjection(MessageTypeParser.parseMessageType(schema), globPatterns);
    }

    /**
     * Recursively traverse the fields originating from a GroupType and return a new GroupType with the fields that
     * match at least one pathMatcher, or null if none of the fields match.
     */
    private static GroupType processGroupType(GroupType groupType, Set<PathMatcher> pathMatchers, String parentPath) {

        String path = String.format("%s/%s", parentPath, groupType.getName());

        if (groupType.getRepetition() == Repetition.REPEATED) { //'Array' or 'Map' should not be part of path string
            path = parentPath;
        }

        // Fields that should be part of projection
        List<Type> fields = new ArrayList<>();

        for (Type type : groupType.getFields()) {
            if (type instanceof GroupType) {
                GroupType gt = processGroupType((GroupType) type, pathMatchers, path);
                if (gt != null) {
                    fields.add(gt);
                }
            } else if (type instanceof PrimitiveType) {
                PrimitiveType pt = processPrimitiveType((PrimitiveType) type, pathMatchers, path);
                if (pt != null) {
                    fields.add(pt);
                }
            } else {
                throw new RuntimeException("Got unexpected type: " + type.getClass());
            }
        }

        GroupType gt = null;
        if (fields.size() > 0) {
            if (groupType instanceof MessageType) { //We're at the root
                gt = Types.buildMessage().addFields(fields.toArray(new Type[0])).named(groupType.getName());
            } else {
                gt = groupType.withNewFields(fields);
            }
        }
        return gt;
    }

    /**
     * Determine if this PrimitiveType matches any of the path matchers. Return a copy of the PrimitiveType if a match
     * is found, null otherwise.
     */
    private static PrimitiveType processPrimitiveType(PrimitiveType primitiveType, Set<PathMatcher> pathMatchers, String parentPath) {

        String path = String.format("%s/%s", parentPath, primitiveType.getName());

        if (primitiveType.getRepetition() == Repetition.REPEATED) { //'Array' or 'Map' should not be part of path string
            path = parentPath;
        }

        PrimitiveType pt = null;
        for (PathMatcher pathMatcher : pathMatchers) {
            if (pathMatcher.matches(Paths.get(path))) {
                pt = Types
                        .primitive(primitiveType.getPrimitiveTypeName(), primitiveType.getRepetition())
                        .as(primitiveType.getLogicalTypeAnnotation())
                        .length(primitiveType.getTypeLength())
                        .named(primitiveType.getName());
                break;
            }
        }
        return pt;
    }
}
