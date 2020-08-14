package no.ssb.dapla.parquet;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Utility functions for working with parquet records.
 *
 * @see GroupType
 * @see Group
 */
public class Record {

    /**
     * Normalize a parquet record. This function re-constructs a parquet record using plain java constructs, namely
     * {@link HashMap}, {@link ArrayList} and {@link String}.
     *
     * @param group the record to normalize
     * @return the normalized record
     */
    static Map<String, Object> normalize(Group group) {
        return (Map<String, Object>) processGroup(Objects.requireNonNull(group));
    }

    private static Object processGroup(Group group) {

        GroupType type = group.getType();

        //TODO: always a single field named 'array' when we encounter an array?
        if ("array".equals(type.getFields().get(0).getName())) {
            Type arrayType = type.getType("array");
            int arraySize = group.getFieldRepetitionCount("array");
            List<Object> array = new ArrayList<>(arraySize);
            for (int i = 0; i < arraySize; i++) {
                if (arrayType instanceof GroupType) {
                    array.add(processGroup(group.getGroup("array", i)));
                } else {
                    array.add(group.getString("array", i));
                }
            }
            return array;
        } else {
            Map<String, Object> map = new HashMap<>();
            for (Type fieldType : type.getFields()) {
                String fieldName = fieldType.getName();
                if (group.getFieldRepetitionCount(fieldName) < 1) {
                    continue;
                }
                if (fieldType instanceof GroupType) {
                    //TODO: safe to always use index=0 here?
                    map.put(fieldName, processGroup(group.getGroup(fieldName, 0)));
                } else if (fieldType instanceof PrimitiveType) {
                    map.put(fieldName, group.getString(fieldName, 0));
                }
            }
            return map;
        }
    }
}
