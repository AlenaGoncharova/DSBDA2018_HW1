import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Custom Partitioner
 */
public class ByOSPartitioner extends Partitioner<EventsWritableComparable, IntWritable> {

    /**
     * This is the custom Partitioner class, which will divide the dataset into three partitions -
     * one with OS as Windows, second partition with other OSs
     */
    @Override
    public int getPartition(EventsWritableComparable key, IntWritable value, int numReduceTasks){
        if (numReduceTasks <= 1)
            return 0;
        if (key.getOsInfo().indexOf("Windows") != -1)
            return 0;
        else
            return 1;
    }

}
