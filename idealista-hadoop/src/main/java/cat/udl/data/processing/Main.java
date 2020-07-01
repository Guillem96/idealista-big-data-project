package cat.udl.data.processing;

import cat.udl.data.processing.mappers.ColumnsSelectorMapper;
import cat.udl.data.processing.writables.CsvRecordWritable;
import lombok.val;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Main extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Main(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {
        val conf = new JobConf(getConf(), Main.class);

        val parsedArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        val job = Job.getInstance(conf, "Idealista");
        job.setJarByClass(Main.class);

        val csvParserConf = new JobConf(false);
        csvParserConf.set(ColumnsSelectorMapper.SEPARATOR, ",");
        csvParserConf.set(ColumnsSelectorMapper.COLUMNS, "propertyCode,thumbnail,numPhotos,floor," +
                "price,propertyType,operation,size,exterior,rooms,bathrooms,address,province,municipality," +
                "district,country,neighborhood,latitude,longitude,showAddress,url,distance,hasVideo,status," +
                "newDevelopment,hasLift,priceByArea,hasPlan,has3DTour,has360,topNewDevelopment," +
                "detailedType_typology,suggestedTexts_subtitle,suggestedTexts_title," +
                "parkingSpace_hasParkingSpace,parkingSpace_isParkingSpaceIncludedInPrice," +
                "detailedType_subTypology,parkingSpace_parkingSpacePrice,spain_state");

        csvParserConf.set(ColumnsSelectorMapper.SELECTOR,
                "propertyCode:price,priceByArea,numPhotos,hasLift,propertyType,spain_state");

        ChainMapper.addMapper(
                job, ColumnsSelectorMapper.class,
                Object.class, Text.class, Text.class, CsvRecordWritable.class, csvParserConf);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(CsvRecordWritable.class);

        FileInputFormat.addInputPath(job, new Path(parsedArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(parsedArgs[1]));

        return (job.waitForCompletion(true) ? 0 : 1);
    }
}
