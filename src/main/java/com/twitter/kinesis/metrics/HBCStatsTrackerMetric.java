package com.twitter.kinesis.metrics;
import com.twitter.hbc.core.StatsReporter;
import com.timgroup.statsd.StatsDClient;

public class HBCStatsTrackerMetric implements SimpleMetric {

  private final StatsReporter.StatsTracker statsTracker;
  private double mostRecentMessageCount;
  
  public HBCStatsTrackerMetric(StatsReporter.StatsTracker statsTracker) {
    this.statsTracker = statsTracker;
  }

  @Override
  public void mark(long size) {
    // No-op
  }

  @Override
  public void reset() {
    // No-op
  }

  @Override
  public String getName() {
    return "HBC Messages Processed ";
  }

  @Override 
  public String getShortName() {
    return "hbc_messages";
  }

  public String toString() {
    String fmtString;
    double sample = getLastPeriodCountAndReset();
    if ( Double.isNaN(sample) ) {
      fmtString = String.format("%s : %s", getName(), "NaN");
    } else {
      fmtString = String.format("%s : %4.0f", getName(), sample);
    }
    return fmtString;
  }

  @Override
  public void recordStats(StatsDClient statsd) {
    if (statsd != null) {
      double sample = getLastPeriodCountAndReset();    
      if (!Double.isNaN(sample)) {
        statsd.gauge(String.format("%s.processed", getShortName()), sample);
      }
    }
  }

  // Return the count for the last period
  private double getLastPeriodCountAndReset() {
    double lastPeriodTotal = getLastMessageCount();
    double currentPeriodTotal = updateMessageCount(); // Holy side effects batman
    return (currentPeriodTotal - lastPeriodTotal);
  }

  private double getLastMessageCount() {
    return this.mostRecentMessageCount;
  }

  private double updateMessageCount() {
    this.mostRecentMessageCount = trackerMessageCount();
    return this.mostRecentMessageCount;
  }

  private double trackerMessageCount() {
    return this.statsTracker.getNumMessages();
  }
}
