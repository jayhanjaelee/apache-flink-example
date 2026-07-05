package com.example.flinkdemo.aggregate;

import com.example.flinkdemo.model.AggregatedResult;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class WindowResultFunction
        extends ProcessWindowFunction<Tuple2<Long, Double>, AggregatedResult, String, TimeWindow> {

    @Override
    public void process(String userId, Context context, Iterable<Tuple2<Long, Double>> elements,
                         Collector<AggregatedResult> out) {
        Tuple2<Long, Double> agg = elements.iterator().next();
        TimeWindow window = context.window();
        out.collect(new AggregatedResult(userId, window.getStart(), window.getEnd(), agg.f0, agg.f1));
    }
}
