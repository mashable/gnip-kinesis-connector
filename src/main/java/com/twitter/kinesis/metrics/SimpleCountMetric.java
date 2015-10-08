package com.twitter.kinesis.metrics;

import com.timgroup.statsd.StatsDClient;

public class SimpleCountMetric extends SimpleAverageMetric {
    SimpleCountMetric(String name, String shortName) {
        super(name, shortName);
    }

    @Override
    public String toString() {
        return String.format("%s : %d", this.getName(), this.getCount());
    }

    @Override
    public void recordStats(StatsDClient statsd) {
        if (statsd != null) {
            statsd.gauge(getShortName(), this.getCount());
        }
    }
}
