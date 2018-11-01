import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Map is passed a single line at a time, it splits the line based on space
 * and generated the pairs<key,value> which are output by map with value as one to be consumed by reduce class
 */
public class MapClass extends Mapper<LongWritable, Text, EventsWritableComparable, IntWritable> {

    private final static int CITY_ID_COLUMN = 7;   // Column number with city ID
    private final static int PRICE_COLUMN_END = 5;      // The number of the column with the price from the end
    private final static int MIN_PRICE = 250;       // Minimal bidding price
    private final static Pattern regex = Pattern.compile("\\((.*?)\\)");  // Regex for OS
    private final static IntWritable one = new IntWritable(1);

    /**
     * Mapper method
     * Parsing string from input and processing it into useful data (pair <(cityID, osInfo); 1>)
     */
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] tokens = line.split("\t");
        if (tokens.length != 24)
        {
            Logger log = Logger.getLogger(MapClass.class.getName());
            log.warning("Invalid size of line");
            return;
        }

        try {
            int price = Integer.parseInt(tokens[tokens.length - PRICE_COLUMN_END]);
            if (price <= MIN_PRICE)
                return;

            int cityID = Integer.parseInt(tokens[CITY_ID_COLUMN]);

            String osInfo = null;
            Matcher matcher = regex.matcher(line);
            osInfo = matcher.find() ? matcher.group() : "";

            context.write(new EventsWritableComparable(cityID, osInfo), one);

        } catch (NumberFormatException ex) {
            // Wrong string
            Logger log = Logger.getLogger(MapClass.class.getName());
            log.warning("Wrong string");
        }
    }
}
