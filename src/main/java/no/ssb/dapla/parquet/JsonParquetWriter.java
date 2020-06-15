package no.ssb.dapla.parquet;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;

public class JsonParquetWriter extends ParquetWriter<String> {
    public JsonParquetWriter(java.nio.file.Path path, MessageType schema) throws IOException {
        this(new Path(path.toUri()), schema, true, CompressionCodecName.SNAPPY);
    }

    public JsonParquetWriter(Path file, MessageType schema, boolean enableDictionary, CompressionCodecName codecName) throws IOException {
        super(file, new JsonWriteSupport(schema), codecName, DEFAULT_BLOCK_SIZE, DEFAULT_PAGE_SIZE, enableDictionary, true);
    }
}
