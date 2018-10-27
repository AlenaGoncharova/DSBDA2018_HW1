import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * MapClassTest.java
 * Unit tests for MapClass class.
 */
public class MapClassTest {

    private MapDriver<LongWritable, Text, EventsWritableComparable, IntWritable> mapDriver;

    /**
     * Creates mapDriver before each try.
     */
    @Before
    public void setUp() {
        mapDriver = MapDriver.newMapDriver(new MapClass());
    }

    /**
     * Correct Mapper test. All fields are described correctly.
     * @throws IOException
     */
    @Test
    public void correctMapperTest() throws IOException {
        mapDriver.withInput(
                new LongWritable(),
                new Text("dd5c2a9324de82aa07af8ce0cf4e348\t20131020043201565\t1\tCANGkv9Beql\tMozilla/4.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0; QQBrowser/7.4.14018.400)\t121.12.0.*\t216\t223\t3\tdd4270481b753dde29898e27c7c03920\td2bdfdce12b2a3545cd25628d87c7f3b\tnull\tEnt_F_Width1\t1000\t90\tNa\tNa\t70\t7336\t294\t70\tnull\t2259\t10057,10059,10077,10075,10083,10006,10111,10126,10131,13403,10063,10116")
        );
        mapDriver.withOutput(
                new EventsWritableComparable(223, "(compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0; QQBrowser/7.4.14018.400)"),
                new IntWritable(1)

        );
        mapDriver.runTest();
    }

    /**
     * Incorrect cityID Mapper test. The value in the city identifier column is not correct.
     * @throws IOException
     */
    @Test
    public void incorrectCityIDMapperTest() throws IOException {
        mapDriver.withInput(
                new LongWritable(),
                new Text("dd5c2a9324de82aa07af8ce0cf4e348\t20131020043201565\t1\tCANGkv9Beql\tMozilla/4.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0; QQBrowser/7.4.14018.400)\t121.12.0.*\t216\tIncorrectCityID\t3\tdd4270481b753dde29898e27c7c03920\td2bdfdce12b2a3545cd25628d87c7f3b\tnull\tEnt_F_Width1\t1000\t90\tNa\tNa\t70\t7336\t294\t70\tnull\t2259\t10057,10059,10077,10075,10083,10006,10111,10126,10131,13403,10063,10116")
        );
        mapDriver.runTest();
    }

    /**
     * Lower price Mapper test. Bidding price is less than the minimum price.
     * @throws IOException
     */
    @Test
    public void lowPriceMapperTest() throws IOException {
        mapDriver.withInput(
                new LongWritable(),
                new Text("dd5c2a9324de82aa07af8ce0cf4e348\t20131020043201565\t1\tCANGkv9Beql\tMozilla/4.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0; QQBrowser/7.4.14018.400)\t121.12.0.*\t216\t223\t3\tdd4270481b753dde29898e27c7c03920\td2bdfdce12b2a3545cd25628d87c7f3b\tnull\tEnt_F_Width1\t1000\t90\tNa\tNa\t70\t7336\t244\t70\tnull\t2259\t10057,10059,10077,10075,10083,10006,10111,10126,10131,13403,10063,10116")
        );
        mapDriver.runTest();
    }

    /**
     * Incorrect price Mapper test. The value in the bidding price column is not correct.
     * @throws IOException
     */
    @Test
    public void incorrectPriceMapperTest() throws IOException {
        mapDriver.withInput(
                new LongWritable(),
                new Text("dd5c2a9324de82aa07af8ce0cf4e348\t20131020043201565\t1\tCANGkv9Beql\tMozilla/4.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0; QQBrowser/7.4.14018.400)\t121.12.0.*\t216\t223\t3\tdd4270481b753dde29898e27c7c03920\td2bdfdce12b2a3545cd25628d87c7f3b\tnull\tEnt_F_Width1\t1000\t90\tNa\tNa\t70\t7336\tIncorrectPrice\t70\tnull\t2259\t10057,10059,10077,10075,10083,10006,10111,10126,10131,13403,10063,10116")
        );
        mapDriver.runTest();
    }
}
