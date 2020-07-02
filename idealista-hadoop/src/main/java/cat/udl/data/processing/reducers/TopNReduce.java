package cat.udl.data.processing.reducers;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

public class TopNReduce extends Reducer<Text, LongWritable, Text, LongWritable> {

    private TreeMap<Integer, Text> topN = new TreeMap<>();

    @Override
    public void reduce(Text key, Iterable<LongWritable> values, Context context) {
        int count = 0;

        Configuration conf = context.getConfiguration();
        int N = conf.getInt("N", 10);

        for (LongWritable value : values) {
            count += value.get();
        }

        topN.put(count, new Text(key.toString()));

        if (topN.size() > N)
            topN.remove(topN.firstKey());
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        for (Map.Entry<Integer, Text> entry : topN.descendingMap().entrySet()) {
            context.write(new Text(entry.getValue()), new LongWritable(entry.getKey()));
        }
    }
}

