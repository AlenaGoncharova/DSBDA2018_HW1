import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

/**
 * EventsWritableComparableTest.java
 * Unit tests for EventsWritableComparable class.
 */
public class EventsWritableComparableTest {

    private static final int CITY_ID = 223;
    private static final String OS_NAME = "Windows";

    /**
     * Empty constructor test.
     */
    @Test
    public void emptyConstructorTest() {
        EventsWritableComparable event = new EventsWritableComparable();
        assertEquals(-1, event.getCityID());
        assertEquals(null, event.getOsInfo());
    }

    /**
     * Full constructor test.
     */
    @Test
    public void fullConstructorTest() {
        EventsWritableComparable event = new EventsWritableComparable(CITY_ID, OS_NAME);
        assertEquals(CITY_ID, event.getCityID());
        assertEquals(OS_NAME, event.getOsInfo());
    }

    /**
     * Generation test for same hash codes.
     */
    @Test
    public void sameCashCodesTest() {
        EventsWritableComparable event1 = new EventsWritableComparable(CITY_ID, OS_NAME);
        EventsWritableComparable event2 = new EventsWritableComparable(CITY_ID, "OS X");
        assertEquals(event1.hashCode(), event2.hashCode());
    }

    /**
     * Generation test for different hash codes.
     */
    @Test
    public void differentCashCodesTest() {
        EventsWritableComparable event1 = new EventsWritableComparable(CITY_ID, OS_NAME);
        EventsWritableComparable event2 = new EventsWritableComparable(CITY_ID - 100, "Windows");
        assertNotEquals(event1.hashCode(), event2.hashCode());
    }


    /**
     * CompareTo test.
     */
    @Test
    public void compareToTest() {
        EventsWritableComparable event1 = new EventsWritableComparable(CITY_ID, OS_NAME);
        EventsWritableComparable event2 = new EventsWritableComparable(CITY_ID + 100, OS_NAME);
        EventsWritableComparable event3 = new EventsWritableComparable(CITY_ID , "OS X");
        assertEquals(-1, event1.compareTo(event2));
        assertEquals(0, event1.compareTo(event3));
        assertEquals(0, event1.compareTo(event1));
        assertEquals(1, event2.compareTo(event1));
        assertEquals(-1, event3.compareTo(event2));
    }
}