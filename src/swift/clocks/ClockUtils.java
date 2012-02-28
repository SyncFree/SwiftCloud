package swift.clocks;

import swift.clocks.CausalityClock.CMP_CLOCK;

public class ClockUtils {

    public static CMP_CLOCK combineCmpClock(CMP_CLOCK c1, CMP_CLOCK c2) {
        if (c1 == CMP_CLOCK.CMP_EQUALS) {
            return c2;
        } else if (c2 == CMP_CLOCK.CMP_EQUALS) {
            return c1;
        } else if (c1 == CMP_CLOCK.CMP_CONCURRENT) {
            return c1;
        } else if (c2 == CMP_CLOCK.CMP_CONCURRENT) {
            return c2;
        } else if (c1 == CMP_CLOCK.CMP_DOMINATES) {
            if (c2 == CMP_CLOCK.CMP_DOMINATES) {
                return CMP_CLOCK.CMP_DOMINATES;
            } else {
                return CMP_CLOCK.CMP_CONCURRENT;
            }
        } else { // c1 == CMP_CLOCK.CMP_ISDOMINATED
            if (c2 == CMP_CLOCK.CMP_ISDOMINATED) {
                return CMP_CLOCK.CMP_ISDOMINATED;
            } else {
                return CMP_CLOCK.CMP_CONCURRENT;
            }
        }

    }
}
