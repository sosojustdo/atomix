/*
 * Copyright 2017-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.protocols.backup;

import io.atomix.primitive.Consistency;
import io.atomix.primitive.DistributedPrimitive;
import io.atomix.primitive.Recovery;
import io.atomix.primitive.Replication;
import io.atomix.primitive.partition.PartitionService;
import io.atomix.primitive.partition.Partitioner;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.primitive.proxy.ProxyClient;
import io.atomix.primitive.proxy.impl.DefaultProxyClient;
import io.atomix.primitive.service.ServiceConfig;
import io.atomix.primitive.session.SessionClient;
import io.atomix.protocols.backup.partition.PrimaryBackupPartition;

import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Multi-primary protocol.
 */
public class MultiPrimaryProtocol implements PrimitiveProtocol {
  public static final Type TYPE = new Type();

  /**
   * Returns a new multi-primary protocol builder.
   *
   * @return a new multi-primary protocol builder
   */
  public static Builder builder() {
    return new Builder(new Config());
  }

  /**
   * Returns a new multi-primary protocol builder for the given group.
   *
   * @param group the partition group
   * @return a new multi-primary protocol builder for the given group
   */
  public static Builder builder(String group) {
    return new Builder(new Config().setGroup(group));
  }

  /**
   * Multi-primary protocol type.
   */
  public static final class Type implements PrimitiveProtocol.Type<Config> {
    private static final String NAME = "multi-primary";

    @Override
    public String name() {
      return NAME;
    }

    @Override
    public Config newConfig() {
      return new Config();
    }

    @Override
    public PrimitiveProtocol newProtocol(Config config) {
      return new MultiPrimaryProtocol(config);
    }
  }

  protected final Config config;

  protected MultiPrimaryProtocol(Config config) {
    this.config = config;
  }

  @Override
  public PrimitiveProtocol.Type type() {
    return TYPE;
  }

  @Override
  public String group() {
    return config.getGroup();
  }

  @Override
  public <S> ProxyClient<S> newProxy(String primitiveName, DistributedPrimitive.Type primitiveType, Class<S> serviceType, ServiceConfig serviceConfig, PartitionService partitionService) {
    Collection<SessionClient> partitions = partitionService.getPartitionGroup(this)
        .getPartitions()
        .stream()
        .map(partition -> ((PrimaryBackupPartition) partition).getClient()
            .sessionBuilder(primitiveName, primitiveType, serviceConfig)
            .withConsistency(config.getConsistency())
            .withReplication(config.getReplication())
            .withRecovery(config.getRecovery())
            .withNumBackups(config.getBackups())
            .withMaxRetries(config.getMaxRetries())
            .withRetryDelay(config.getRetryDelay())
            .build())
        .collect(Collectors.toList());
    return new DefaultProxyClient<>(primitiveName, primitiveType, this, serviceType, partitions, config.getPartitioner());
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("type", type())
        .add("group", group())
        .toString();
  }

  /**
   * Multi-primary protocol configuration.
   */
  public static class Config extends PrimitiveProtocol.Config<Config> {
    private Partitioner<String> partitioner = Partitioner.MURMUR3;
    private Consistency consistency = Consistency.SEQUENTIAL;
    private Replication replication = Replication.ASYNCHRONOUS;
    private Recovery recovery = Recovery.RECOVER;
    private int backups = 1;
    private int maxRetries = 0;
    private Duration retryDelay = Duration.ofMillis(100);

    @Override
    public PrimitiveProtocol.Type getType() {
      return MultiPrimaryProtocol.TYPE;
    }

    /**
     * Returns the protocol partitioner.
     *
     * @return the protocol partitioner
     */
    public Partitioner<String> getPartitioner() {
      return partitioner;
    }

    /**
     * Sets the protocol partitioner.
     *
     * @param partitioner the protocol partitioner
     * @return the protocol configuration
     */
    public Config setPartitioner(Partitioner<String> partitioner) {
      this.partitioner = partitioner;
      return this;
    }

    /**
     * Returns the consistency level.
     *
     * @return the consistency level
     */
    public Consistency getConsistency() {
      return consistency;
    }

    /**
     * Sets the consistency level.
     *
     * @param consistency the consistency level
     * @return the protocol configuration
     */
    public Config setConsistency(Consistency consistency) {
      this.consistency = consistency;
      return this;
    }

    /**
     * Returns the replication level.
     *
     * @return the replication level
     */
    public Replication getReplication() {
      return replication;
    }

    /**
     * Sets the replication level.
     *
     * @param replication the replication level
     * @return the protocol configuration
     */
    public Config setReplication(Replication replication) {
      this.replication = replication;
      return this;
    }

    /**
     * Returns the recovery strategy.
     *
     * @return the recovery strategy
     */
    public Recovery getRecovery() {
      return recovery;
    }

    /**
     * Sets the recovery strategy.
     *
     * @param recovery the recovery strategy
     * @return the protocol configuration
     */
    public Config setRecovery(Recovery recovery) {
      this.recovery = recovery;
      return this;
    }

    /**
     * Returns the number of backups.
     *
     * @return the number of backups
     */
    public int getBackups() {
      return backups;
    }

    /**
     * Sets the number of backups.
     *
     * @param numBackups the number of backups
     * @return the protocol configuration
     */
    public Config setBackups(int numBackups) {
      this.backups = numBackups;
      return this;
    }

    /**
     * Returns the maximum allowed number of retries.
     *
     * @return the maximum allowed number of retries
     */
    public int getMaxRetries() {
      return maxRetries;
    }

    /**
     * Sets the maximum allowed number of retries.
     *
     * @param maxRetries the maximum allowed number of retries
     * @return the protocol configuration
     */
    public Config setMaxRetries(int maxRetries) {
      this.maxRetries = maxRetries;
      return this;
    }

    /**
     * Returns the retry delay.
     *
     * @return the retry delay
     */
    public Duration getRetryDelay() {
      return retryDelay;
    }

    /**
     * Sets the retry delay.
     *
     * @param retryDelayMillis the retry delay in milliseconds
     * @return the protocol configuration
     */
    public Config setRetryDelayMillis(long retryDelayMillis) {
      return setRetryDelay(Duration.ofMillis(retryDelayMillis));
    }

    /**
     * Sets the retry delay.
     *
     * @param retryDelay the retry delay
     * @return the protocol configuration
     */
    public Config setRetryDelay(Duration retryDelay) {
      this.retryDelay = retryDelay;
      return this;
    }
  }

  /**
   * Multi-primary protocol builder.
   */
  public static class Builder extends PrimitiveProtocol.Builder<Config, MultiPrimaryProtocol> {
    protected Builder(Config config) {
      super(config);
    }

    /**
     * Sets the protocol partitioner.
     *
     * @param partitioner the protocol partitioner
     * @return the protocol builder
     */
    public Builder withPartitioner(Partitioner<String> partitioner) {
      config.setPartitioner(partitioner);
      return this;
    }

    /**
     * Sets the protocol consistency model.
     *
     * @param consistency the protocol consistency model
     * @return the protocol builder
     */
    public Builder withConsistency(Consistency consistency) {
      config.setConsistency(consistency);
      return this;
    }

    /**
     * Sets the protocol replication strategy.
     *
     * @param replication the protocol replication strategy
     * @return the protocol builder
     */
    public Builder withReplication(Replication replication) {
      config.setReplication(replication);
      return this;
    }

    /**
     * Sets the protocol recovery strategy.
     *
     * @param recovery the protocol recovery strategy
     * @return the protocol builder
     */
    public Builder withRecovery(Recovery recovery) {
      config.setRecovery(recovery);
      return this;
    }

    /**
     * Sets the number of backups.
     *
     * @param numBackups the number of backups
     * @return the protocol builder
     */
    public Builder withBackups(int numBackups) {
      config.setBackups(numBackups);
      return this;
    }

    /**
     * Sets the maximum number of retries before an operation can be failed.
     *
     * @param maxRetries the maximum number of retries before an operation can be failed
     * @return the proxy builder
     */
    public Builder withMaxRetries(int maxRetries) {
      config.setMaxRetries(maxRetries);
      return this;
    }

    /**
     * Sets the operation retry delay.
     *
     * @param retryDelayMillis the delay between operation retries in milliseconds
     * @return the proxy builder
     */
    public Builder withRetryDelayMillis(long retryDelayMillis) {
      config.setRetryDelayMillis(retryDelayMillis);
      return this;
    }

    /**
     * Sets the operation retry delay.
     *
     * @param retryDelay the delay between operation retries
     * @param timeUnit   the delay time unit
     * @return the proxy builder
     * @throws NullPointerException if the time unit is null
     */
    public Builder withRetryDelay(long retryDelay, TimeUnit timeUnit) {
      return withRetryDelay(Duration.ofMillis(timeUnit.toMillis(retryDelay)));
    }

    /**
     * Sets the operation retry delay.
     *
     * @param retryDelay the delay between operation retries
     * @return the proxy builder
     * @throws NullPointerException if the delay is null
     */
    public Builder withRetryDelay(Duration retryDelay) {
      config.setRetryDelay(retryDelay);
      return this;
    }

    @Override
    public MultiPrimaryProtocol build() {
      return new MultiPrimaryProtocol(config);
    }
  }
}
