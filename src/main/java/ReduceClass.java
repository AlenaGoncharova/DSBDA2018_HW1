import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.StringTokenizer;

/**
 * Reduce class which is executed after the map class and takes key(EventsWritableComparable - info about event)
 * and corresponding value equal 1, sums all the values
 * and write the event along with the corresponding total occurances in the output
 */
public class ReduceClass extends Reducer<EventsWritableComparable, IntWritable, Text, IntWritable> {

    private HashMap<Integer, String> citiesMap = new HashMap <Integer, String>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        try{
            URI[] filesURI = context.getCacheFiles();
            if(filesURI != null && filesURI.length > 0) {
                for(URI fileURI : filesURI) {
                    readFileWriteToCitiesMap(fileURI);
                }
            }
        } catch(IOException ex) {
            System.err.println("Exception in mapper setup: " + ex.getMessage());
        }
    }

    public void readFileWriteToCitiesMap(URI filePath) {
        try{
            BufferedReader bufferedReader = new BufferedReader(new FileReader(filePath.toString()));
            String line = null;
            while((line = bufferedReader.readLine()) != null) {
                StringTokenizer itr = new StringTokenizer(line);
                citiesMap.put(Integer.parseInt(itr.nextToken()),itr.nextToken());
            }

//            List<String> lines = Files.readAllLines(Paths.get(filePath.getPath()), StandardCharsets.UTF_8);
//            for (String line: lines) {
//                StringTokenizer itr = new StringTokenizer(line);
//                citiesMap.put(Integer.parseInt(itr.nextToken()),itr.nextToken());
//            }
        } catch(IOException ex) {
            System.err.println("Exception while reading stop words file: " + ex.getMessage());
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
        }
        else {
            text = new Text("City ID - #" + key.getCityID());
        }
        context.write(text, new IntWritable(sum));
    }
}
