package com.twitter.kinesis;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSCredentialsProviderChain;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.timgroup.statsd.StatsDClient;
import com.timgroup.statsd.NonBlockingStatsDClient;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.EnterpriseStreamingEndpoint;
import com.twitter.hbc.core.endpoint.RealTimeEnterpriseStreamingEndpoint;
import com.twitter.hbc.core.endpoint.ReplayEnterpriseStreamingEndpoint;
import com.twitter.hbc.core.processor.LineStringProcessor;
import com.twitter.hbc.httpclient.auth.BasicAuth;
import com.twitter.kinesis.metrics.HBCStatsTrackerMetric;
import com.twitter.kinesis.metrics.MetricReporter;
import com.twitter.kinesis.metrics.ShardMetricLogging;
import com.twitter.kinesis.metrics.SimpleMetricManager;
import com.twitter.kinesis.utils.Environment;
import org.apache.commons.cli.*;
import org.joda.time.LocalDateTime;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.Date;

public class ConnectorApplication {
  private static final Logger logger = LoggerFactory.getLogger(ConnectorApplication.class);
  private Client client;
  private Environment environment;
  private KinesisProducer producer;
  private SimpleMetricManager simpleMetricManager;
  private Date replayFrom;
  private Date replayTo;
  private String configFile;

  public ConnectorApplication() {
    environment = new Environment();
  }

  public static void main(String[] args) {
    Options options = createOptions();
    try {
      CommandLine cmdLine = new DefaultParser().parse(options, args);
      if (cmdLine.hasOption("h")) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp( "java target/connector-1.0-jar-with-dependencies.jar", options);
        System.exit(0);
      }

      logger.info("Starting Connector Application...");
      ConnectorApplication application = new ConnectorApplication();
      if (cmdLine.hasOption("f") && cmdLine.hasOption("t")) {
        application.setReplayDates(cmdLine.getOptionValue("f"), cmdLine.getOptionValue("t"));
      }
      if (cmdLine.hasOption("c")) {
        application.setConfigFile(cmdLine.getOptionValue("c"));
      }

      application.start();
    } catch (Exception e) {
      logger.error("Unexpected error occured", e);
    }
  }

  private static Options createOptions() {
    Options options = new Options();
    options.addOption(Option.builder("h").argName("help").hasArg(false).desc("Help").build());
    options.addOption(Option.builder("f").argName("from").hasArg().desc("Replay from (iso8601 date)").type(Date.class).build());
    options.addOption(Option.builder("t").argName("to").hasArg().desc("Replay to (iso8601 date)").type(Date.class).build());
    options.addOption(Option.builder("c").argName("file").hasArg().desc("Config property file").type(String.class).build());
    return options;
  }

  private void setReplayDates(String from, String to) {
    this.replayFrom = new LocalDateTime(new DateTime(from).toDate(), DateTimeZone.UTC).toDate();
    this.replayTo = new LocalDateTime(new DateTime(to).toDate(), DateTimeZone.UTC).toDate();
  }

  private void setConfigFile(String file) {
    this.configFile = file;
  }

  private void configure() {
    this.environment.configure(this.configFile);
    StatsDClient statsd = null;
    if (this.environment.getStatsdHost() != null) {
      statsd = new NonBlockingStatsDClient(this.environment.getStatsdPrefix(), this.environment.getStatsdHost(), 
        this.environment.getStatsdPort());
    }
    this.simpleMetricManager = new SimpleMetricManager(statsd);
    LinkedBlockingQueue<String> downstream = new LinkedBlockingQueue<String>(10000);
    ShardMetricLogging shardMetric = new ShardMetricLogging();
    this.simpleMetricManager.registerMetric(shardMetric);
    AWSCredentialsProvider credentialsProvider = new AWSCredentialsProviderChain(new InstanceProfileCredentialsProvider(), this.environment);

    this.client = new ClientBuilder()
            .name("PowerTrackClient-01")
            .hosts(Constants.ENTERPRISE_STREAM_HOST)
            .endpoint(endpoint())
            .authentication(auth())
            .processor(new LineStringProcessor(downstream))
            .build();

    AmazonKinesisClient kinesisClient = new AmazonKinesisClient(credentialsProvider);
    kinesisClient.withRegion(Regions.fromName(this.environment.kinesisStreamRegion()));
    new KinesisProducer.Builder()
            .environment(this.environment)
            .kinesisClient(kinesisClient)
            .shardCount(this.environment.shardCount())
            .streamName(this.environment.kinesisStreamName())
            .upstream(downstream)
            .simpleMetricManager(this.simpleMetricManager)
            .shardMetric(shardMetric)
            .gnipClient(this.client)
            .replayMode(this.replayFrom != null && this.replayTo != null)
            .buildAndStart();

    configureHBCStatsTrackerMetric();
  }

  private void configureHBCStatsTrackerMetric() {
    HBCStatsTrackerMetric rateTrackerMetric = new HBCStatsTrackerMetric(client.getStatsTracker());
    this.simpleMetricManager.registerMetric(rateTrackerMetric);
    MetricReporter metricReporter = new MetricReporter(this.simpleMetricManager, this.environment);
    metricReporter.start();
  }

  private BasicAuth auth() {
    return new BasicAuth(this.environment.userName(), this.environment.userPassword());
  }

  private void start() throws InterruptedException {
    configure();

    // Establish a connection
    client.connect();
  }

  private EnterpriseStreamingEndpoint endpoint() {
    String account = this.environment.accountName();
    String label = this.environment.streamLabel();
    String product = this.environment.product();
    if (this.replayFrom != null && this.replayTo != null) {
      return new ReplayEnterpriseStreamingEndpoint(account, product, label, this.replayFrom, this.replayTo);
    } else {
      return new RealTimeEnterpriseStreamingEndpoint(account, product, label);
    }
  }
}