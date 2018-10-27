import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Custom WritableComparable
 */
public class EventsWritableComparable implements WritableComparable<EventsWritableComparable> {

    private int cityID;
    private String osInfo;

    public EventsWritableComparable() {
        cityID = -1;
        osInfo = null;
    }

    public EventsWritableComparable(int cityId, String osInfo) {
        this.cityID = cityId;
        this.osInfo = osInfo;
    }

    public int getCityID() {
        return cityID;
    }

    public String getOsInfo() {
        return osInfo;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(cityID);
        out.writeUTF(osInfo);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        cityID = in.readInt();
        osInfo = in.readUTF();
    }

    @Override
    public int compareTo(EventsWritableComparable o) {
        return (this.cityID < o.cityID ? -1 : (this.cityID == o.cityID ? 0 : 1));
    }

    @Override
    public int hashCode() {
        return this.cityID;
    }

    @Override
    public String toString() {
        return Integer.toString(cityID) + "; " + osInfo + ";";
    }

    @Override
    public boolean equals(Object object) {
        if ((object == null) || (!(object instanceof EventsWritableComparable)))
            return false;
        return this.cityID == ((EventsWritableComparable) object).cityID;
    }

}
