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
package io.atomix.protocols.raft.partition;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.atomix.cluster.Member;
import io.atomix.cluster.MemberId;
import io.atomix.primitive.Recovery;
import io.atomix.primitive.partition.ManagedPartitionGroup;
import io.atomix.primitive.partition.Partition;
import io.atomix.primitive.partition.PartitionGroup;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.partition.PartitionManagementService;
import io.atomix.primitive.partition.PartitionMetadata;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.protocols.raft.MultiRaftProtocol;
import io.atomix.storage.StorageLevel;
import io.atomix.utils.memory.MemorySize;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Raft partition group.
 */
public class RaftPartitionGroup implements ManagedPartitionGroup {
  public static final Type TYPE = new Type();

  /**
   * Returns a new Raft partition group builder.
   *
   * @param name the partition group name
   * @return a new partition group builder
   */
  public static Builder builder(String name) {
    return new Builder(new Config().setName(name));
  }

  /**
   * Raft partition group type.
   */
  public static class Type implements PartitionGroup.Type<Config> {
    private static final String NAME = "raft";

    @Override
    public String name() {
      return NAME;
    }

    @Override
    public Config newConfig() {
      return new Config();
    }

    @Override
    public ManagedPartitionGroup newPartitionGroup(Config config) {
      return new RaftPartitionGroup(config);
    }
  }

  /**
   * Raft partition group configuration.
   */
  public static class Config extends PartitionGroup.Config<Config> {
    private static final int DEFAULT_PARTITIONS = 7;
    private static final String DATA_PREFIX = ".data";

    private Set<String> members = new HashSet<>();
    private int partitionSize;
    private String storageLevel = StorageLevel.MAPPED.name();
    private long segmentSize = 1024 * 1024 * 32;
    private boolean flushOnCommit = true;
    private String dataDirectory;

    @Override
    public PartitionGroup.Type getType() {
      return RaftPartitionGroup.TYPE;
    }

    @Override
    protected int getDefaultPartitions() {
      return DEFAULT_PARTITIONS;
    }

    /**
     * Returns the set of members in the partition group.
     *
     * @return the set of members in the partition group
     */
    public Set<String> getMembers() {
      return members;
    }

    /**
     * Sets the set of members in the partition group.
     *
     * @param members the set of members in the partition group
     * @return the Raft partition group configuration
     */
    public Config setMembers(Set<String> members) {
      this.members = members;
      return this;
    }

    /**
     * Returns the partition size.
     *
     * @return the partition size
     */
    public int getPartitionSize() {
      return partitionSize;
    }

    /**
     * Sets the partition size.
     *
     * @param partitionSize the partition size
     * @return the Raft partition group configuration
     */
    public Config setPartitionSize(int partitionSize) {
      this.partitionSize = partitionSize;
      return this;
    }

    /**
     * Returns the partition storage level.
     *
     * @return the partition storage level
     */
    public String getStorageLevel() {
      return storageLevel;
    }

    /**
     * Sets the partition storage level.
     *
     * @param storageLevel the partition storage level
     * @return the Raft partition group configuration
     */
    public Config setStorageLevel(String storageLevel) {
      StorageLevel.valueOf(storageLevel.toUpperCase());
      this.storageLevel = storageLevel;
      return this;
    }

    /**
     * Returns the Raft log segment size.
     *
     * @return the Raft log segment size
     */
    public MemorySize getSegmentSize() {
      return MemorySize.from(segmentSize);
    }

    /**
     * Sets the Raft log segment size.
     *
     * @param segmentSize the Raft log segment size
     * @return the partition group configuration
     */
    public Config setSegmentSize(MemorySize segmentSize) {
      this.segmentSize = segmentSize.bytes();
      return this;
    }

    /**
     * Returns whether to flush logs to disk on commit.
     *
     * @return whether to flush logs to disk on commit
     */
    public boolean isFlushOnCommit() {
      return flushOnCommit;
    }

    /**
     * Sets whether to flush logs to disk on commit.
     *
     * @param flushOnCommit whether to flush logs to disk on commit
     * @return the Raft partition group configuration
     */
    public Config setFlushOnCommit(boolean flushOnCommit) {
      this.flushOnCommit = flushOnCommit;
      return this;
    }

    /**
     * Returns the partition data directory.
     *
     * @return the partition data directory
     */
    public String getDataDirectory() {
      return dataDirectory != null ? dataDirectory : DATA_PREFIX + "/" + getName();
    }

    /**
     * Sets the partition data directory.
     *
     * @param dataDirectory the partition data directory
     * @return the Raft partition group configuration
     */
    public Config setDataDirectory(String dataDirectory) {
      this.dataDirectory = dataDirectory;
      return this;
    }
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(RaftPartitionGroup.class);

  private static Collection<RaftPartition> buildPartitions(Config config) {
    File partitionsDir = new File(config.getDataDirectory(), "partitions");
    List<RaftPartition> partitions = new ArrayList<>(config.getPartitions());
    for (int i = 0; i < config.getPartitions(); i++) {
      partitions.add(new RaftPartition(
          PartitionId.from(config.getName(), i + 1),
          StorageLevel.valueOf(config.getStorageLevel().toUpperCase()),
          config.getSegmentSize().bytes(),
          config.isFlushOnCommit(),
          new File(partitionsDir, String.valueOf(i + 1))));
    }
    return partitions;
  }

  private final String name;
  private final Config config;
  private final int partitionSize;
  private final Map<PartitionId, RaftPartition> partitions = Maps.newConcurrentMap();
  private final List<PartitionId> sortedPartitionIds = Lists.newCopyOnWriteArrayList();
  private Collection<PartitionMetadata> metadata;

  public RaftPartitionGroup(Config config) {
    this.name = config.getName();
    this.config = config;
    this.partitionSize = config.getPartitionSize();
    buildPartitions(config).forEach(p -> {
      this.partitions.put(p.id(), p);
      this.sortedPartitionIds.add(p.id());
    });
    Collections.sort(sortedPartitionIds);
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public PartitionGroup.Type type() {
    return TYPE;
  }

  @Override
  public PrimitiveProtocol.Type protocol() {
    return MultiRaftProtocol.TYPE;
  }

  @Override
  public Config config() {
    return config;
  }

  @Override
  public PrimitiveProtocol newProtocol() {
    return MultiRaftProtocol.builder(name)
        .withRecoveryStrategy(Recovery.RECOVER)
        .withMaxRetries(5)
        .build();
  }

  @Override
  public RaftPartition getPartition(PartitionId partitionId) {
    return partitions.get(partitionId);
  }

  @Override
  @SuppressWarnings("unchecked")
  public Collection<Partition> getPartitions() {
    return (Collection) partitions.values();
  }

  @Override
  public List<PartitionId> getPartitionIds() {
    return sortedPartitionIds;
  }

  @Override
  public CompletableFuture<ManagedPartitionGroup> join(PartitionManagementService managementService) {
    this.metadata = buildPartitions();
    List<CompletableFuture<Partition>> futures = metadata.stream()
        .map(metadata -> {
          RaftPartition partition = partitions.get(metadata.id());
          return partition.open(metadata, managementService);
        })
        .collect(Collectors.toList());
    return CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()])).thenApply(v -> {
      LOGGER.info("Started");
      return this;
    });
  }

  @Override
  public CompletableFuture<ManagedPartitionGroup> connect(PartitionManagementService managementService) {
    return join(managementService);
  }

  private Collection<PartitionMetadata> buildPartitions() {
    List<MemberId> sorted = new ArrayList<>(config.getMembers().stream()
        .map(MemberId::from)
        .collect(Collectors.toSet()));
    Collections.sort(sorted);

    int partitionSize = this.partitionSize;
    if (partitionSize == 0) {
      partitionSize = sorted.size();
    }

    int length = sorted.size();
    int count = Math.min(partitionSize, length);

    Set<PartitionMetadata> metadata = Sets.newHashSet();
    for (int i = 0; i < partitions.size(); i++) {
      PartitionId partitionId = sortedPartitionIds.get(i);
      Set<MemberId> set = new HashSet<>(count);
      for (int j = 0; j < count; j++) {
        set.add(sorted.get((i + j) % length));
      }
      metadata.add(new PartitionMetadata(partitionId, set));
    }
    return metadata;
  }

  @Override
  public CompletableFuture<Void> close() {
    List<CompletableFuture<Void>> futures = partitions.values().stream()
        .map(RaftPartition::close)
        .collect(Collectors.toList());
    return CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()])).thenRun(() -> {
      LOGGER.info("Stopped");
    });
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("name", name)
        .add("partitions", partitions)
        .toString();
  }

  /**
   * Raft partition group builder.
   */
  public static class Builder extends PartitionGroup.Builder<Config> {
    protected Builder(Config config) {
      super(config);
    }

    /**
     * Sets the Raft partition group members.
     *
     * @param members the Raft partition group members
     * @return the Raft partition group builder
     * @throws NullPointerException if the members are null
     */
    public Builder withMembers(String... members) {
      return withMembers(Arrays.asList(members));
    }

    /**
     * Sets the Raft partition group members.
     *
     * @param members the Raft partition group members
     * @return the Raft partition group builder
     * @throws NullPointerException if the members are null
     */
    public Builder withMembers(MemberId... members) {
      return withMembers(Stream.of(members).map(nodeId -> nodeId.id()).collect(Collectors.toList()));
    }

    /**
     * Sets the Raft partition group members.
     *
     * @param members the Raft partition group members
     * @return the Raft partition group builder
     * @throws NullPointerException if the members are null
     */
    public Builder withMembers(Member... members) {
      return withMembers(Stream.of(members).map(node -> node.id().id()).collect(Collectors.toList()));
    }

    /**
     * Sets the Raft partition group members.
     *
     * @param members the Raft partition group members
     * @return the Raft partition group builder
     * @throws NullPointerException if the members are null
     */
    public Builder withMembers(Collection<String> members) {
      config.setMembers(Sets.newHashSet(checkNotNull(members, "members cannot be null")));
      return this;
    }

    /**
     * Sets the number of partitions.
     *
     * @param numPartitions the number of partitions
     * @return the cluster metadata builder
     * @throws IllegalArgumentException if the number of partitions is not positive
     */
    public Builder withNumPartitions(int numPartitions) {
      config.setPartitions(numPartitions);
      return this;
    }

    /**
     * Sets the partition size.
     *
     * @param partitionSize the partition size
     * @return the cluster metadata builder
     * @throws IllegalArgumentException if the partition size is not positive
     */
    public Builder withPartitionSize(int partitionSize) {
      config.setPartitionSize(partitionSize);
      return this;
    }

    /**
     * Sets the storage level.
     *
     * @param storageLevel the storage level
     * @return the Raft partition group builder
     */
    public Builder withStorageLevel(StorageLevel storageLevel) {
      config.setStorageLevel(storageLevel.name());
      return this;
    }

    /**
     * Sets the path to the data directory.
     *
     * @param dataDir the path to the replica's data directory
     * @return the replica builder
     */
    public Builder withDataDirectory(File dataDir) {
      config.setDataDirectory(new File("user.dir").toURI().relativize(dataDir.toURI()).getPath());
      return this;
    }

    @Override
    public RaftPartitionGroup build() {
      return new RaftPartitionGroup(config);
    }
  }
}
