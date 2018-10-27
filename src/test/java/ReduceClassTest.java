import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * ReduceClassTest.java
 * Unit tests for ReduceClass class.
 */
public class ReduceClassTest {

    private ReduceDriver<EventsWritableComparable, IntWritable, Text, IntWritable> reduceDriver;
    List<IntWritable> values;
    /**
     * Creates reduceDriver before each try.
     */
    @Before
    public void setUp() {
        reduceDriver = ReduceDriver.newReduceDriver(new ReduceClass());
        values = new ArrayList<IntWritable>();
        values.add(new IntWritable(10));
        values.add(new IntWritable(6));
//        reduceDriver.addCacheFile("./dataCityID/city.en.txt");
    }

    /**
     * Correct Reducer test with cityId from city names file.
     * @throws IOException
     */
//    @Test
//    public void sumAndCorrectCityIdReducerTest() throws IOException {
//        reduceDriver.withInput(
//                new EventsWritableComparable(217, "Windows"),
//                values
//        );
//        reduceDriver.withOutput(
//                new Text("guangzhou"),
//                new IntWritable(16)
//        );
//        reduceDriver.runTest();
//    }

    /**
     * Correct Reducer test without cityID in city names file.
     * @throws IOException
     */
    @Test
    public void sumAndIncorrectCityIdReducerTest() throws IOException {
        reduceDriver.withInput(
                new EventsWritableComparable(216, "OS X"),
                values
        );
        reduceDriver.withOutput(
                new Text("City ID - #216"),
                new IntWritable(16));
        reduceDriver.runTest();
    }
}
