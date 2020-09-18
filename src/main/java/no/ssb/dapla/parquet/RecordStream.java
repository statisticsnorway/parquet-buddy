package no.ssb.dapla.parquet;

import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * This class represents a continuous stream of records from a parquet file.
 */
public class RecordStream implements AutoCloseable {

    private final DataStreamRowGroupReader rowGroupReader;
    private final FieldInterceptor fieldInterceptor;

    private RecordStream(SeekableByteChannel file, Set<String> fieldSelectors, FieldInterceptor fieldInterceptor) throws IOException {
        this.rowGroupReader = new DataStreamRowGroupReader(file, fieldSelectors);
        this.fieldInterceptor = fieldInterceptor;
    }

    /**
     * Read the next record from this stream of records.
     *
     * @return the next record in the stream, or null if the end of the stream has been reached.
     * @throws IOException if an error occurs while reading.
     */
    public Map<String, Object> read() throws IOException {
        Group next = rowGroupReader.next();
        if (next == null) {
            return null;
        }
        return Record.normalize(next, fieldInterceptor);
    }

    @Override
    public void close() {
        try {
            rowGroupReader.close();
        } catch (Exception e) {
            throw new RuntimeException("Error when closing RecordStream reader", e);
        }
    }

    public static Builder builder(SeekableByteChannel file) {
        return new Builder(file);
    }

    public static class Builder {
        private FieldInterceptor fieldInterceptor = FieldInterceptor.noOp();
        private Set<String> fieldSelectors = new HashSet<>();
        private final SeekableByteChannel file;

        public Builder(SeekableByteChannel file) {
            this.file = file;
        }

        public Builder withFieldSelectors(Set<String> globPatterns) {
            this.fieldSelectors = globPatterns;
            return this;
        }

        public Builder withFieldInterceptor(FieldInterceptor fieldInterceptor) {
            this.fieldInterceptor = fieldInterceptor;
            return this;
        }

        public RecordStream build() throws IOException {
            return new RecordStream(file, fieldSelectors, fieldInterceptor);
        }
    }

    static class DataStreamRowGroupReader implements AutoCloseable {

        private final ParquetFileReader fileReader;
        private final MessageColumnIO columnIO;
        private final MessageType schema;
        private RecordReader<Group> groupReader;
        private long groupsRemaining;

        DataStreamRowGroupReader(SeekableByteChannel file, Set<String> fieldSelectors) throws IOException {
            this.fileReader = ParquetFileReader.open(new SeekableByteChannelInputFile(file));
            PageReadStore rowGroup = this.fileReader.readNextRowGroup();
            if (rowGroup == null) {
                throw new RuntimeException("No row groups found in file");
            }
            MessageType schema = this.fileReader.getFileMetaData().getSchema();
            if (!fieldSelectors.isEmpty()) {
                schema = Schema.createProjection(schema, fieldSelectors);
            }
            this.schema = schema;
            this.columnIO = new ColumnIOFactory().getColumnIO(schema);
            this.groupReader = columnIO.getRecordReader(rowGroup, new GroupRecordConverter(schema));
            this.groupsRemaining = rowGroup.getRowCount();
        }

        Group next() throws IOException {
            if (groupsRemaining < 1) {
                PageReadStore rowGroup = fileReader.readNextRowGroup();
                if (rowGroup == null) {
                    return null;
                }
                groupReader = columnIO.getRecordReader(rowGroup, new GroupRecordConverter(schema));
                groupsRemaining = rowGroup.getRowCount();
                if (groupsRemaining < 1) {
                    return null;
                }
            }
            Group next = groupReader.read();
            groupsRemaining--;
            return next;
        }

        @Override
        public void close() throws Exception {
            this.fileReader.close();
        }
    }
}
