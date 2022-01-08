package org.apache.flink.training.exercises.hourlytips;


import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.training.exercises.common.datatypes.TaxiFare;
import org.apache.flink.util.Collector;

public class DriverHourTipsFunction extends ProcessWindowFunction<TaxiFare, Tuple3<Long, Long, Float>, Long, TimeWindow> {
    
    @Override
    public void process(Long driverId, Context context, Iterable<TaxiFare> taxiFares, Collector<Tuple3<Long, Long, Float>> out) throws Exception {
        float sum = 0f;
        for (TaxiFare taxiFare : taxiFares) {
            sum += taxiFare.tip;
        }
        long endTime = context.window().getEnd();
        out.collect(Tuple3.of(endTime, driverId, sum));
    }
}
