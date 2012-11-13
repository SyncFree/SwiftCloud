package swift.clocks;

import java.util.concurrent.atomic.AtomicLong;


/**
 * Timestamp generator for a given site. Always generates the consecutive
 * counter for the given site.
 * 
 * @author nmp
 * 
 */
public class IncrementalTimestampGenerator implements TimestampSource<Timestamp> {

    private String siteid;
    private AtomicLong last = new AtomicLong(0);

    public IncrementalTimestampGenerator(String siteid) {
        this(siteid, 0);
    }

    public IncrementalTimestampGenerator(String siteid, long last) {
        if (siteid == null) {
            throw new NullPointerException();
        }
        this.siteid = siteid;
        this.last = new AtomicLong(last);
    }

    @Override
    public Timestamp generateNew() {
        return new Timestamp(siteid, last.incrementAndGet() );
    }

}
