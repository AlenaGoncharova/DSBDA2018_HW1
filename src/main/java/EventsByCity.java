import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.logging.Logger;


public class EventsByCity extends Configured implements Tool {

    /**
     * Main function which calls the run method and passes the args using ToolRunner
     * @param args Three arguments - input, output and cache file paths
     * @throws Exception
     */
    public static void main(final String[] args) throws Exception {
        int exitCode = ToolRunner.run(new EventsByCity(), args);
        System.exit(exitCode);
    }

    /**
     * Run method which schedules the Hadoop Job
     * @param args Arguments passed in main function
     */
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        if (args.length != 3) {
            Logger log = Logger.getLogger(EventsByCity.class.getName());
            log.warning("Invalid number of arguments");
            throw new Exception("Invalid number of arguments\"");
        }

        String hdfsInputFilesOrDirectory = args[0];
        String hdfsOutputFolder = args[1];
        String hdfsCacheFilesOrDirectory = args[2];

        //Initialize the Hadoop job and set the jar as well as the name of the Job
        Job job = Job.getInstance(conf);
        job.setJarByClass(getClass());
        job.setJobName(getClass().getName());

        //Add input and output file paths to job based on the arguments passed
        FileInputFormat.addInputPath(job, new Path(hdfsInputFilesOrDirectory));
        FileOutputFormat.setOutputPath(job, new Path(hdfsOutputFolder));

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

//        job.setOutputFormatClass(SequenceFileOutputFormat.class);
//        FileOutputFormat.setCompressOutput(job, true);
//        FileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);
//        SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);


        //Set the MapClass and ReduceClass in the job
        job.setMapperClass(MapClass.class);
        job.setMapOutputKeyClass(EventsWritableComparable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setReducerClass(ReduceClass.class);

        //Set Partitioner Class
        job.setPartitionerClass(ByOSPartitioner.class);

        //Set number of reducer tasks
        job.setNumReduceTasks(2);

        job.addCacheFile(new Path(hdfsCacheFilesOrDirectory).toUri());

        //Wait for the job to complete and print if the job was successful or not
        int returnValue = job.waitForCompletion(true) ? 0 : 1;

        if(job.isSuccessful()) {
            System.out.println("Job was successful");
        } else if(!job.isSuccessful()) {
            System.out.println("Job was not successful");
        }

        return returnValue;
    }

}
