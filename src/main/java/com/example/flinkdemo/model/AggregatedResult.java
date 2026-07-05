package com.example.flinkdemo.model;

public class AggregatedResult {
    public String userId;
    public long windowStart;
    public long windowEnd;
    public long eventCount;
    public double totalAmount;

    public AggregatedResult() {}

    public AggregatedResult(String userId, long windowStart, long windowEnd,
                             long eventCount, double totalAmount) {
        this.userId = userId;
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
        this.eventCount = eventCount;
        this.totalAmount = totalAmount;
    }

    @Override
    public String toString() {
        return "AggregatedResult{userId='" + userId + "', windowStart=" + windowStart
                + ", windowEnd=" + windowEnd + ", eventCount=" + eventCount
                + ", totalAmount=" + totalAmount + '}';
    }
}
