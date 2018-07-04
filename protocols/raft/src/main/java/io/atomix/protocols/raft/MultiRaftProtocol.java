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
package io.atomix.protocols.raft;

import io.atomix.primitive.DistributedPrimitive;
import io.atomix.primitive.Recovery;
import io.atomix.primitive.partition.PartitionService;
import io.atomix.primitive.partition.Partitioner;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.primitive.proxy.ProxyClient;
import io.atomix.primitive.proxy.impl.DefaultProxyClient;
import io.atomix.primitive.service.ServiceConfig;
import io.atomix.primitive.session.SessionClient;
import io.atomix.protocols.raft.partition.RaftPartition;
import io.atomix.protocols.raft.session.CommunicationStrategy;

import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Multi-Raft protocol.
 */
public class MultiRaftProtocol implements PrimitiveProtocol {
  public static final Type TYPE = new Type();

  /**
   * Returns a new multi-Raft protocol builder.
   *
   * @return a new multi-Raft protocol builder
   */
  public static Builder builder() {
    return new Builder(new Config());
  }

  /**
   * Returns a new multi-Raft protocol builder.
   *
   * @param group the partition group
   * @return the multi-Raft protocol builder
   */
  public static Builder builder(String group) {
    return new Builder(new Config().setGroup(group));
  }

  /**
   * Multi-Raft protocol type.
   */
  public static final class Type implements PrimitiveProtocol.Type<Config> {
    private static final String NAME = "multi-raft";

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
      return new MultiRaftProtocol(config);
    }
  }

  private final Config config;

  protected MultiRaftProtocol(Config config) {
    this.config = checkNotNull(config, "config cannot be null");
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
        .map(partition -> ((RaftPartition) partition).getClient()
            .sessionBuilder(primitiveName, primitiveType, serviceConfig)
            .withMinTimeout(config.getMinTimeout())
            .withMaxTimeout(config.getMaxTimeout())
            .withReadConsistency(config.getReadConsistency())
            .withCommunicationStrategy(config.getCommunicationStrategy())
            .withRecoveryStrategy(config.getRecoveryStrategy())
            .withMaxRetries(config.getMaxRetries())
            .withRetryDelay(config.getRetryDelay())
            .build())
        .collect(Collectors.toList());
    return new DefaultProxyClient<>(primitiveName, primitiveType, this, serviceType, partitions, config.getPartitioner());
  }

  /**
   * Raft protocol configuration.
   */
  public static class Config extends PrimitiveProtocol.Config<Config> {
    private Partitioner<String> partitioner = Partitioner.MURMUR3;
    private Duration minTimeout = Duration.ofMillis(250);
    private Duration maxTimeout = Duration.ofSeconds(30);
    private ReadConsistency readConsistency = ReadConsistency.SEQUENTIAL;
    private CommunicationStrategy communicationStrategy = CommunicationStrategy.LEADER;
    private Recovery recoveryStrategy = Recovery.RECOVER;
    private int maxRetries = 0;
    private Duration retryDelay = Duration.ofMillis(100);

    @Override
    public PrimitiveProtocol.Type getType() {
      return MultiRaftProtocol.TYPE;
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
     * Returns the minimum session timeout.
     *
     * @return the minimum session timeout
     */
    public Duration getMinTimeout() {
      return minTimeout;
    }

    /**
     * Sets the minimum session timeout.
     *
     * @param minTimeout the minimum session timeout
     * @return the Raft protocol configuration
     */
    public Config setMinTimeout(Duration minTimeout) {
      this.minTimeout = minTimeout;
      return this;
    }

    /**
     * Returns the maximum session timeout.
     *
     * @return the maximum session timeout
     */
    public Duration getMaxTimeout() {
      return maxTimeout;
    }

    /**
     * Sets the maximum session timeout.
     *
     * @param maxTimeout the maximum session timeout
     * @return the Raft protocol configuration
     */
    public Config setMaxTimeout(Duration maxTimeout) {
      this.maxTimeout = maxTimeout;
      return this;
    }

    /**
     * Returns the read consistency level.
     *
     * @return the read consistency level
     */
    public ReadConsistency getReadConsistency() {
      return readConsistency;
    }

    /**
     * Sets the read consistency level.
     *
     * @param readConsistency the read consistency level
     * @return the Raft protocol configuration
     */
    public Config setReadConsistency(ReadConsistency readConsistency) {
      this.readConsistency = readConsistency;
      return this;
    }

    /**
     * Returns the client communication strategy.
     *
     * @return the client communication strategy
     */
    public CommunicationStrategy getCommunicationStrategy() {
      return communicationStrategy;
    }

    /**
     * Sets the client communication strategy.
     *
     * @param communicationStrategy the client communication strategy
     * @return the Raft protocol configuration
     */
    public Config setCommunicationStrategy(CommunicationStrategy communicationStrategy) {
      this.communicationStrategy = communicationStrategy;
      return this;
    }

    /**
     * Returns the client recovery strategy.
     *
     * @return the client recovery strategy
     */
    public Recovery getRecoveryStrategy() {
      return recoveryStrategy;
    }

    /**
     * Sets the client recovery strategy.
     *
     * @param recoveryStrategy the client recovery strategy
     * @return the Raft protocol configuration
     */
    public Config setRecoveryStrategy(Recovery recoveryStrategy) {
      this.recoveryStrategy = recoveryStrategy;
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
   * Multi-Raft protocol builder.
   */
  public static class Builder extends PrimitiveProtocol.Builder<Config, MultiRaftProtocol> {
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
     * Sets the minimum session timeout.
     *
     * @param minTimeout the minimum session timeout
     * @return the Raft protocol builder
     */
    public Builder withMinTimeout(Duration minTimeout) {
      config.setMinTimeout(minTimeout);
      return this;
    }

    /**
     * Sets the maximum session timeout.
     *
     * @param maxTimeout the maximum session timeout
     * @return the Raft protocol builder
     */
    public Builder withMaxTimeout(Duration maxTimeout) {
      config.setMaxTimeout(maxTimeout);
      return this;
    }

    /**
     * Sets the read consistency level.
     *
     * @param readConsistency the read consistency level
     * @return the Raft protocol builder
     */
    public Builder withReadConsistency(ReadConsistency readConsistency) {
      config.setReadConsistency(readConsistency);
      return this;
    }

    /**
     * Sets the communication strategy.
     *
     * @param communicationStrategy the communication strategy
     * @return the Raft protocol builder
     */
    public Builder withCommunicationStrategy(CommunicationStrategy communicationStrategy) {
      config.setCommunicationStrategy(communicationStrategy);
      return this;
    }

    /**
     * Sets the recovery strategy.
     *
     * @param recoveryStrategy the recovery strategy
     * @return the Raft protocol builder
     */
    public Builder withRecoveryStrategy(Recovery recoveryStrategy) {
      config.setRecoveryStrategy(recoveryStrategy);
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
      return withRetryDelay(Duration.ofMillis(retryDelayMillis));
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
    public MultiRaftProtocol build() {
      return new MultiRaftProtocol(config);
    }
  }
}
