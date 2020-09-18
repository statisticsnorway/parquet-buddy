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
        return normalize(group, FieldInterceptor.noOp());
    }

    static Map<String, Object> normalize(Group group, FieldInterceptor fieldInterceptor) {
        return (Map<String, Object>) processGroup(Objects.requireNonNull(group), fieldInterceptor, "");
    }

    private static Object processGroup(Group group, FieldInterceptor fieldInterceptor, String parentPath) {

        GroupType type = group.getType();

        //Assume always a single field named 'array' when we encounter an array
        if ("array".equals(type.getFields().get(0).getName())) {
            Type arrayType = type.getType("array");
            int arraySize = group.getFieldRepetitionCount("array");
            List<Object> array = new ArrayList<>(arraySize);
            for (int i = 0; i < arraySize; i++) {
                if (arrayType instanceof GroupType) {
                    array.add(processGroup(group.getGroup("array", i), fieldInterceptor, parentPath));
                } else {
                    array.add(fieldInterceptor.intercept(parentPath, group.getString("array", i)));
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
                String fieldPath = "%s/%s".formatted(parentPath, fieldName);
                if (fieldType instanceof GroupType) {
                    //Assume it's safe to always use index=0 here
                    map.put(fieldName, processGroup(group.getGroup(fieldName, 0), fieldInterceptor, fieldPath));
                } else if (fieldType instanceof PrimitiveType) {
                    map.put(fieldName, fieldInterceptor.intercept(fieldPath, group.getString(fieldName, 0)));
                }
            }
            return map;
        }
    }
}
