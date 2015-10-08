package com.twitter.kinesis.metrics;

import com.google.inject.Singleton;
import com.timgroup.statsd.StatsDClient;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

@Singleton
public class SimpleMetricManager {

    Map<String, SimpleMetric> map = new HashMap<String, SimpleMetric>();
    Logger logger = LoggerFactory.getLogger(SimpleMetricManager.class);
    StatsDClient statsd = null;

    public SimpleMetricManager() {
        //do nothing
    }

    public SimpleMetricManager(StatsDClient statsd) {
        this.statsd = statsd;
    }

    public synchronized void report() {
        StringBuilder buf = new StringBuilder();
        buf.append("\n=================\n");
        for (String key : map.keySet()) {
            SimpleMetric value = map.get(key);
            if (this.statsd == null || ! (value instanceof HBCStatsTrackerMetric)) {
                buf.append(value.toString());
                buf.append ("\n");
            }
            value.recordStats(this.statsd);
            value.reset();
        }
        logger.info(buf.toString());
    }

    public synchronized SimpleMetric newSimpleCountMetric(String name, String shortname) {
        SimpleMetric metric = map.get(name);
        if ( metric == null ){
            metric = new SimpleCountMetric(name, shortname);
            map.put(name, metric);
        }
        return metric;
    }

    public synchronized SimpleMetric newSimpleMetric(String name, String shortname) {
        SimpleMetric metric = map.get(name);
        if ( metric == null ){
            metric = new SimpleAverageMetric(name, shortname);
            map.put(name, metric);
        }
        return metric;
    }

    public synchronized void registerMetric (SimpleMetric metric) {
        map.put (metric.getName(), metric);
    }
}
