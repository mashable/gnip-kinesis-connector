package com.twitter.kinesis.metrics;

import com.timgroup.statsd.StatsDClient;

public interface SimpleMetric {
    void mark(long size);

    void reset();

    String getName();

    String getShortName();

    void recordStats(StatsDClient statsd);
}
