package cat.udl.data.processing.mappers;

import cat.udl.data.processing.writables.CsvRecordWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class HouseCount extends Mapper<Text, CsvRecordWritable, Text, LongWritable> {
    private final static LongWritable ONE = new LongWritable(1);

    @Override
    public void map(Text key, CsvRecordWritable value, Context context) throws IOException, InterruptedException {
        context.write(new Text(key.toString()), ONE);
    }
}
