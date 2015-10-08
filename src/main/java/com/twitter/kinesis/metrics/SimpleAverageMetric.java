package com.twitter.kinesis.metrics;

import com.timgroup.statsd.StatsDClient;

public class SimpleAverageMetric implements SimpleMetric {
    private long count;
    private long sample;
    private String name;
    private String shortName;

    SimpleAverageMetric(String name, String shortName){
        this.name = name;
        this.shortName = shortName;
    }

    @Override
    public synchronized void mark(long size) {
        count++;
        sample += size;
    }

    public synchronized long getCount() {
        return count;
    }

    public synchronized double getAvg() {
        return count == 0 ? 0 : (double) sample / (double) count;
    }

    @Override
    public synchronized void reset() {
        count = 0;
        sample = 0;
    }

    public String toString() {
        return String.format("%s : %,.3f", name, this.getAvg());
    }

    @Override
    public void recordStats(StatsDClient statsd) {
        if (statsd != null) {
            statsd.gauge(getShortName(), this.getAvg());
        }
    }

    public String getName() {
        return name;
    }

    public String getShortName() {
        return shortName;
    }
}
