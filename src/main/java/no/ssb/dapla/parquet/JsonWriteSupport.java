package no.ssb.dapla.parquet;

import com.google.gson.Gson;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.ValidatingRecordConsumer;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JsonWriteSupport extends WriteSupport<String> {

    private ValidatingRecordConsumer recordConsumer;
    private MessageType schema;

    public JsonWriteSupport(MessageType schema) {
        this.schema = schema;
    }

    @Override
    public WriteContext init(Configuration configuration) {
        return new WriteContext(schema, new HashMap<>());
    }

    @Override
    public void prepareForWrite(RecordConsumer recordConsumer) {
        this.recordConsumer = new ValidatingRecordConsumer(recordConsumer, schema);
    }

    @Override
    public void write(String json) {
        recordConsumer.startMessage();
        process(new Gson().fromJson(json, Map.class), recordConsumer);
        recordConsumer.endMessage();
    }

    private static void process(Map<String, Object> map, RecordConsumer recordConsumer) {

        int i = 0;

        for (Map.Entry<String, Object> entry : map.entrySet()) {

            String k = entry.getKey();
            Object v = entry.getValue();

            recordConsumer.startField(k, i);

            if (v instanceof Map) {
                processJsonObject((Map) v, recordConsumer);
            } else if (v instanceof List) {
                processJsonArray((List) v, recordConsumer);
            } else {
                recordConsumer.addBinary(Binary.fromString((String) v));
            }

            recordConsumer.endField(k, i++);
        }
    }

    private static void processJsonObject(Map<String, Object> map, RecordConsumer recordConsumer) {
        recordConsumer.startGroup();
        process(map, recordConsumer);
        recordConsumer.endGroup();
    }

    private static void processJsonArray(List list, RecordConsumer recordConsumer) {
        recordConsumer.startGroup();
        recordConsumer.startField("array", 0);
        for (Object o : list) {
            if (o instanceof Map) {
                processJsonObject((Map) o, recordConsumer);
            } else {
                recordConsumer.addBinary(Binary.fromString((String) o));
            }
        }
        recordConsumer.endField("array", 0);
        recordConsumer.endGroup();
    }
}
