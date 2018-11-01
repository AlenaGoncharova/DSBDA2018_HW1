import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.StringTokenizer;
import java.util.logging.Logger;

/**
 * Reduce class which is executed after the map class and takes key(EventsWritableComparable - info about event)
 * and corresponding value equal 1, sums all the values
 * and write the event along with the corresponding total occurances in the output
 */
public class ReduceClass extends Reducer<EventsWritableComparable, IntWritable, Text, IntWritable> {

    private HashMap<Integer, String> citiesMap = new HashMap <>();

    @Override
    protected void setup(Context context) {
        try{
            URI[] filesURI = context.getCacheFiles();
            if(filesURI != null && filesURI.length > 0) {
                for(URI fileURI : filesURI) {
                    BufferedReader bufferedReader =
                            new BufferedReader(
                                    new InputStreamReader(
                                            FileSystem.get(context.getConfiguration())
                                                    .open(new Path(fileURI))));
                    String line;
                    while((line = bufferedReader.readLine()) != null) {
                        StringTokenizer itr = new StringTokenizer(line);
                        citiesMap.put(Integer.parseInt(itr.nextToken()), itr.nextToken());
                    }
                }
            }
        } catch(IOException ex) {
            Logger log = Logger.getLogger(ReduceClass.class.getName());
            log.warning("Exception in reducer setup: " + ex.getMessage());
        }
    }

    /**
     * Method which performs the reduce operation and sums
     * all the occurrences of the event before passing it to be stored in output
     */
    @Override
    public void reduce(EventsWritableComparable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }

        Text text = null;
        if (citiesMap.containsKey(key.getCityID())) {
            text = new Text(citiesMap.get(key.getCityID()));
            context.write(text, new IntWritable(sum));
        }
        else {
            Logger log = Logger.getLogger(ReduceClass.class.getName());
            log.warning("No city found with this ID" + key.getCityID());
        }
    }
}
