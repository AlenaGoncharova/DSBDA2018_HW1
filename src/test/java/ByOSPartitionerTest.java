import org.apache.hadoop.io.IntWritable;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

/**
 * ByOSPartitionerTest.java
 * Unit tests for ByOSPartitioner class.
 */
public class ByOSPartitionerTest {

    private ByOSPartitioner partitioner;
    private EventsWritableComparable event1;
    private EventsWritableComparable event2;
    private IntWritable intWritable;

    /**
     * Creates all data before each try.
     */
    @Before
    public void setUp() {
        partitioner = new ByOSPartitioner();
        event1 = new EventsWritableComparable(223, "Windows");
        event2 = new EventsWritableComparable(233, "OS X");
        intWritable = new IntWritable(1);
    }

    /**
     * Partitioner test with one reducer.
     */
    @Test
    public void oneNumReduceTasks() {
        assertEquals(
                partitioner.getPartition(event1, intWritable, 1),
                partitioner.getPartition(event2, intWritable, 1)
        );
    }

    /**
     * Partitioner test with two reducers.
     */
    @Test
    public void twoNumReduceTasks() {
        assertNotEquals(
                partitioner.getPartition(event1, intWritable, 2),
                partitioner.getPartition(event2, intWritable, 2)
        );
    }

    /**
     * Partitioner test for OS "Windows" with two reducers.
     */
    @Test
    public void reduceTaskForOSWindows() {
        assertEquals(0, partitioner.getPartition(event1, intWritable, 2));
    }

}

