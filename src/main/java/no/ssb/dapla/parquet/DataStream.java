package no.ssb.dapla.parquet;

import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.util.Map;

public class DataStream implements AutoCloseable {

    private final DataStreamRowGroupReader rowGroupReader;

    DataStream(SeekableByteChannel data, MessageType schema) throws IOException {
        this.rowGroupReader = new DataStreamRowGroupReader(data, schema);
    }

    public Map<String, Object> read() throws IOException {
        Group next = rowGroupReader.next();
        System.out.println(next);




        return null;
    }

    @Override
    public void close() throws Exception {
        rowGroupReader.close();
    }

    static class DataStreamRowGroupReader implements AutoCloseable {

        private final ParquetFileReader fileReader;
        private final MessageColumnIO columnIO;
        private final MessageType schema;
        private RecordReader<Group> groupReader;
        private long groupsRemaining;

        DataStreamRowGroupReader(SeekableByteChannel data, MessageType schema) throws IOException {

            this.fileReader = ParquetFileReader.open(new SeekableByteChannelInputFile(data));

            PageReadStore rowGroup = this.fileReader.readNextRowGroup();
            if (rowGroup == null) {
                throw new RuntimeException("No row groups found in file");
            }

            this.columnIO = new ColumnIOFactory().getColumnIO(schema);
            this.schema = schema;
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
