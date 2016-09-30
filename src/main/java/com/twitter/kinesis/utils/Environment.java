package com.twitter.kinesis.utils;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;
import java.util.TreeSet;

public class Environment implements AWSCredentialsProvider {
  private static final Logger logger = LoggerFactory.getLogger(Environment.class);
  private static Properties props;

  public void configure(String configFile) {
    try {
      logger.info("loading properties from classpath");
      InputStream properties = null;
      if (configFile != null) {
        properties = new FileInputStream(configFile);
      } else {
        properties = Environment.class.getClassLoader().getResourceAsStream("config.properties");
      }
      props = new Properties();
      props.load(properties);
      logProperties();
    } catch (IOException e) {
      logger.error("Could not load properties, streams cannot be configured");
      throw new RuntimeException("Could not load properties");
    }
  }

  public void logProperties() {
    TreeSet<String> keys = new TreeSet<String>(props.stringPropertyNames());

    for (String key : keys) {
      logger.info(key + ": " + props.get(key));
    }
  }

  public String userName() {
    return props.getProperty("gnip.user.name");
  }

  public String userPassword() {
    return props.getProperty("gnip.user.password");
  }

  public String streamLabel() {
    return props.getProperty("gnip.stream.label");
  }

  public String accountName() {
    return props.getProperty("gnip.account.name");
  }

  public String product() {
    return props.getProperty("gnip.product");
  }

  public int partitionId() {
    return Integer.parseInt(props.getProperty("gnip.partition", "0"));
  }

  public int backfillMinutes() {
    return Integer.parseInt(props.getProperty("gnip.backfillMinutes", "0"));
  }

  public String publisher() {
    return props.getProperty("gnip.publisher", "twitter");
  }

  public int getProducerThreadCount() {
    return Integer.parseInt(props.getProperty("producer.thread.count", "30"));
  }

  public double getRateLimit() {
    return Double.parseDouble(props.getProperty("rate.limit", "-1"));
  }

  public int getReportInterval() {
    return Integer.parseInt(props.getProperty("metric.report.interval.seconds", "60"));
  }

  public String kinesisStreamName() {
    return props.getProperty("aws.kinesis.stream.name");
  }

  public String kinesisStreamRegion() {
    return props.getProperty("aws.kinesis.stream.region");
  }

  public int shardCount() {
    return Integer.parseInt(props.getProperty("aws.kinesis.shard.count"));
  }

  public int getMessageQueueSize() {
    return Integer.parseInt(props.getProperty("message.queue.size"));
  }

  public String getStatsdPrefix() {
    return props.getProperty("statsd.prefix", "");
  }

  public String getStatsdHost() {
    return props.getProperty("statsd.host");
  }

  public int getStatsdPort() {
    return Integer.parseInt(props.getProperty("statsd.port", "8125"));
  }

  @Override
  public AWSCredentials getCredentials() {
    AWSCredentials credentials = new AWSCredentials() {
      @Override
      public String getAWSAccessKeyId() {
        String value = props.getProperty("aws.access.key");
        return value;
      }

      @Override
      public String getAWSSecretKey() {
        String value = props.getProperty("aws.secret.key");
        return value;
      }
    };
    return credentials;
  }

  @Override
  public void refresh() {
    // No-op
  }
}
