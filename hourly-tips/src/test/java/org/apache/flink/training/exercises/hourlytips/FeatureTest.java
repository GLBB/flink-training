package org.apache.flink.training.exercises.hourlytips;

import org.apache.flink.streaming.api.windowing.time.Time;
import org.junit.Test;

public class FeatureTest {
    
    @Test
    public void testTime() {
        Time oneHours = Time.hours(1);
        System.out.println(oneHours.toMilliseconds());
        long oneHourMsec = oneHours.toMilliseconds();
        long nowMsec = System.currentTimeMillis();
        long windownEnd = nowMsec - (nowMsec % oneHourMsec) + oneHourMsec - 1;
        System.out.println("windown end: " + windownEnd);
    }
}
