package cat.udl.data.processing.mappers;

import cat.udl.data.processing.writables.CsvRecordWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FilterRowsMapper extends Mapper<Text, CsvRecordWritable, Text, CsvRecordWritable> {
    public final static String MAX_PRICE = "filter.rows.maxPrice";
    public final static String MIN_ROOMS = "filter.rows.minRooms";
    public final static String HAS_PHOTOS = "filter.rows.hasPhotos";
    public final static String PROPERTY_TYPE = "filter.rows.type";

    private Double maxPrice;
    private Integer minRooms;
    private Boolean hasPhotos;
    private String type;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        maxPrice = context.getConfiguration().getDouble(MAX_PRICE, Double.MAX_VALUE);
        minRooms = context.getConfiguration().getInt(MIN_ROOMS, 0);
        type = context.getConfiguration().get(PROPERTY_TYPE, "flat");
        hasPhotos = Boolean.parseBoolean(context.getConfiguration().get(HAS_PHOTOS, "true"));
    }

    public void map(Text key, CsvRecordWritable value, Context context) throws InterruptedException, IOException {
        if (Double.parseDouble(value.get("price")) <= maxPrice &&
                Integer.parseInt(value.get("rooms")) >= minRooms &&
                value.get("propertyType").equals(type) &&
                (!hasPhotos || Integer.parseInt(value.get("numPhotos")) >= 0))
            context.write(key, value);

    }
}
