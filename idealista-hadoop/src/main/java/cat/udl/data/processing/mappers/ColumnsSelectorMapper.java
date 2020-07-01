package cat.udl.data.processing.mappers;

import cat.udl.data.processing.Utils;
import cat.udl.data.processing.writables.CsvRecordWritable;
import lombok.val;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toMap;

public class ColumnsSelectorMapper<K> extends Mapper<K, Text, Text, CsvRecordWritable> {
    public final static String COLUMNS = "columns.selector.names";
    public final static String SELECTOR = "columns.selector.fields";
    public final static String SEPARATOR = "columns.selector.separator";
    public final static String SKIP_HEADER = "columns.selector.skipheader";

    private List<String> columnsName;
    private List<Integer> keyIndices;
    private List<Integer> valueIndices;
    private String separator;
    private Boolean skipHeader;

    /**
     * Parses a part of the selector.
     * @param selector Fields selector, for example: "propertyType,province-price"
     * @return Returns a list of indices corresponding to the selected columns
     */
    private List<Integer> parseKeyValueSelector(String selector) {
        val result = new ArrayList<Integer>();
        val parts = selector.split(",");
        if (parts.length == 0)
            throw new RuntimeException(selector + " is not well formatted");

        for(val part : parts) {
            if (part.contains("-")) {
                val slicedPart = part.split("-");
                val start = this.columnsName.indexOf(slicedPart[0]);
                val end = this.columnsName.indexOf(slicedPart[1]);
                result.addAll(Utils.range(start, end + 1));
            } else {
                result.add(this.columnsName.indexOf(part));
            }
        }

        return result;
    }

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        columnsName = Arrays.asList(
                context.getConfiguration().get(COLUMNS).split(","));
        skipHeader = Boolean.parseBoolean(context.getConfiguration().get(SKIP_HEADER, "true"));

        val selector = context.getConfiguration().get(SELECTOR);
        val slicedSelector = selector.split(":");
        if (slicedSelector.length != 2)
            throw new RuntimeException("Selector is not well formatted");

        keyIndices = parseKeyValueSelector(slicedSelector[0]);
        valueIndices = parseKeyValueSelector(slicedSelector[1]);

        separator = context.getConfiguration().get(SEPARATOR, ",");
    }

    public void map(K key, Text value, Context context) throws InterruptedException, IOException {
        // Split row by separator, but we use a regex to ignore
        // the separator if it is between quotes
        val slicedRow = value.toString().split(
                this.separator + "(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");

        // If user configures the mapper to skip the header, we check that the key is equal
        // to zero
        if (skipHeader && key.toString().equals("0"))
            return;

        // Select keys
        val newKey = this.keyIndices
                .stream()
                .map(i -> slicedRow[i])
                .collect(joining(","));

        // Select values
        val columnsMap = this.valueIndices
                .stream()
                .collect(toMap(i -> this.columnsName.get(i), i -> slicedRow[i]));

        context.write(new Text(newKey), new CsvRecordWritable(columnsMap));
    }
}
