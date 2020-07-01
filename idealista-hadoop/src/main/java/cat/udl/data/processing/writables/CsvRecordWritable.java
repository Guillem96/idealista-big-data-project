package cat.udl.data.processing.writables;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.val;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class CsvRecordWritable implements Writable {
    private MapWritable row = new MapWritable();

    public CsvRecordWritable(Map<String, String> other) {
        this.row = new MapWritable();
        for(val entry: other.entrySet())
            this.row.put(new Text(entry.getKey()), new Text(entry.getValue()));
    }

    public String get(String key) {
        return this.row.get(new Text(key)).toString();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        this.row.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.row.readFields(dataInput);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("CsvRecord(");

        val iter = row.entrySet().iterator();
        while (iter.hasNext()) {
            val entry = iter.next();
            sb.append(entry.getKey());
            sb.append('=').append('"');
            sb.append(entry.getValue());
            sb.append('"');
            if (iter.hasNext())
                sb.append(',').append(' ');
        }
        sb.append(")");
        return sb.toString();
    }
}
