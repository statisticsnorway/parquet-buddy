package no.ssb.dapla.parquet;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.stream.JsonReader;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Data provides methods for reading and writing parquet.
 */
public class Data {

    /**
     * readColumn returns all values in a given column, specified by a glob. For glob syntax see:
     * https://docs.oracle.com/javase/tutorial/essential/io/fileOps.html#glob. To avoid having to scan the entire
     * parquet file, a projection schema is created to allow reading only the requested column.
     * <p></p>
     * E.g. Given the following parquet schema:
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
     * To read all values in the column streetName (root -> person -> address -> array -> streetName) create a glob that
     * matches the path to that column, e.g. {@literal "**}/streetName" or "person/address/streetName". Note: 'root' and
     * 'array' are considered as part of the path to a column.
     *
     * @param data              a parquet file as a {@link SeekableByteChannel}
     * @param columnGlobPattern a glob that should match a single column
     * @return a list of all the column values
     */
    public static List<Object> readColumn(SeekableByteChannel data, String columnGlobPattern) {

        InputFile inputFile = new SeekableByteChannelInputFile(data);

        List<Object> columnValues = new ArrayList<>();
        try (ParquetFileReader reader = ParquetFileReader.open(inputFile)) {

            MessageType schema = reader.getFooter().getFileMetaData().getSchema();

            MessageType schemaProjection = Schema.createProjection(schema, Set.of(columnGlobPattern));
            if (schemaProjection == null) {
                throw new RuntimeException("Column glob pattern doesn't match any columns. Pattern: " + columnGlobPattern);
            }

            List<ColumnDescriptor> columns = schemaProjection.getColumns();
            if (columns.size() > 1) {
                throw new RuntimeException("Column glob pattern matches several columns. Pattern: " + columnGlobPattern + ". Matches: " + columns.size());
            }

            reader.setRequestedSchema(schemaProjection);

            String[] columnPath = columns.get(0).getPath();

            PageReadStore rowGroup;
            while ((rowGroup = reader.readNextRowGroup()) != null) {
                long rows = rowGroup.getRowCount();

                MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schemaProjection);
                RecordReader<Group> groupReader = columnIO.getRecordReader(rowGroup, new GroupRecordConverter(schemaProjection));

                for (int i = 0; i < rows; i++) {
                    Group group = groupReader.read();
                    List<Object> values = findColumnValues(group, columnPath, 0);
                    columnValues.addAll(values);
                }
            }

        } catch (IOException e) {
            throw new RuntimeException("Got error while reading", e);
        }

        return columnValues;
    }

    /**
     * Recursively traverse the hierarchy until we find the column we're interested in. Return the column values as a
     * list (column could be in a collection).
     */
    private static List<Object> findColumnValues(Group group, String[] columnPath, int index) {

        List<Object> values = new ArrayList<>();

        String field = columnPath[index];
        int fieldRepetitionCount = group.getFieldRepetitionCount(field);

        if (columnPath.length == index + 1) { //Found target column field
            for (int i = 0; i < fieldRepetitionCount; i++) {
                String value = group.getString(field, i);
                values.add(value);
            }
        } else {
            for (int i = 0; i < fieldRepetitionCount; i++) {
                Group nextGroup = group.getGroup(field, i);
                List<Object> groupValues = findColumnValues(nextGroup, columnPath, index + 1);
                values.addAll(groupValues);
            }
        }

        return values;
    }

    /**
     * writeJson writes an array of json objects to file, as parquet.
     *
     * @param json   an array of json objects as an {@link InputStream}
     * @param path   the file to write to
     * @param schema the parquet schema ({@link MessageType}) of the file
     */
    public static void writeJson(InputStream json, Path path, MessageType schema) {
        try (JsonParquetWriter writer = new JsonParquetWriter(path, schema)) {

            Gson gson = new GsonBuilder().setPrettyPrinting().create();

            try (JsonReader reader = new JsonReader(new InputStreamReader(json))) {

                reader.beginArray();

                while (reader.hasNext()) {
                    Object o = gson.fromJson(reader, Object.class);
                    writer.write(gson.toJson(o));
                }

                reader.endArray();
            }

        } catch (IOException e) {
            throw new RuntimeException(String.format("Error while writing json. Path: %s", path), e);
        }
    }

    /**
     * writeJson writes a json object to file, as parquet.
     *
     * @param json   a single json object as a String
     * @param path   the file to write to
     * @param schema the parquet schema ({@link MessageType}) of the file
     */
    public static void writeJson(String json, Path path, MessageType schema) {
        try (JsonParquetWriter writer = new JsonParquetWriter(path, schema)) {
            writer.write(json);
        } catch (IOException e) {
            throw new RuntimeException(String.format("Error while writing json. Path: %s", path), e);
        }
    }
}
