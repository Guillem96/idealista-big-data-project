package cat.udl.data.processing.mappers;

import cat.udl.data.processing.Utils;
import lombok.val;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.util.stream.Collectors.joining;

public class ColumnsSelectorMapper<K> extends Mapper<K, Text, Text, Text> {
    public final static String COLUMNS = "columns.selector.names";
    public final static String SELECTOR = "columns.selector.fields";
    public final static String SEPARATOR = "columns.selector.separator";

    private List<String> columnsName;
    private List<Integer> keyIndices;
    private List<Integer> valueIndices;
    private String separator;

    /**
     * Parses a part of the selector.
     * @param selector Fields selector, for example: "propertyType,province-price"
     * @return Returns a list of indices corresponding to the selected columns
     */
    private List<Integer> parseKeyValueSelector(String selector) {
        val result = new ArrayList<Integer>();

        for(val part : selector.split(",")) {
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

        val selector = context.getConfiguration().get(SELECTOR);
        val slicedSelector = selector.split(":");
        keyIndices = parseKeyValueSelector(slicedSelector[0]);
        valueIndices = parseKeyValueSelector(slicedSelector[1]);

        separator = context.getConfiguration().get(SEPARATOR, ",");
    }

    public void map(K key, Text value, Context context) throws InterruptedException, IOException {
        val slicedRow = value.toString().split(this.separator + "(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");

        val newKey = this.keyIndices
                .stream()
                .map(i -> slicedRow[i])
                .collect(joining(","));

        val newValue = this.valueIndices
                .stream()
                .map(i -> slicedRow[i])
                .collect(joining(","));

        context.write(new Text(newKey), new Text(newValue));
    }
}
