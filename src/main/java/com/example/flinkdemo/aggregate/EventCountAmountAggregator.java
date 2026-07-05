package com.example.flinkdemo.aggregate;

import com.example.flinkdemo.model.UserEvent;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class EventCountAmountAggregator
        implements AggregateFunction<UserEvent, Tuple2<Long, Double>, Tuple2<Long, Double>> {

    @Override
    public Tuple2<Long, Double> createAccumulator() {
        return Tuple2.of(0L, 0.0);
    }

    @Override
    public Tuple2<Long, Double> add(UserEvent event, Tuple2<Long, Double> acc) {
        return Tuple2.of(acc.f0 + 1, acc.f1 + event.amount);
    }

    @Override
    public Tuple2<Long, Double> getResult(Tuple2<Long, Double> acc) {
        return acc;
    }

    @Override
    public Tuple2<Long, Double> merge(Tuple2<Long, Double> a, Tuple2<Long, Double> b) {
        return Tuple2.of(a.f0 + b.f0, a.f1 + b.f1);
    }
}
