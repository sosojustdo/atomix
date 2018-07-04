/*
 * Copyright 2016-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.core.leadership;

import io.atomix.cluster.MemberId;
import io.atomix.primitive.DistributedPrimitive;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.SyncPrimitive;
import io.atomix.utils.serializer.Namespace;
import io.atomix.utils.serializer.NamespaceConfig;
import io.atomix.utils.serializer.Namespaces;
import io.atomix.utils.serializer.Serializer;

import java.util.Map;

/**
 * {@code LeaderElector} provides the same functionality as {@link AsyncLeaderElector} with
 * the only difference that all its methods block until the corresponding operation completes.
 */
public interface LeaderElector<T> extends SyncPrimitive {

  /**
   * Attempts to become leader for a topic.
   *
   * @param topic      leadership topic
   * @param identifier instance identifier of the candidate
   * @return current Leadership state of the topic
   */
  Leadership<T> run(String topic, T identifier);

  /**
   * Withdraws from leadership race for a topic.
   *
   * @param topic      leadership topic
   * @param identifier instance identifier of the candidate to withdraw
   */
  void withdraw(String topic, T identifier);

  /**
   * Attempts to promote a node to leadership displacing the current leader.
   *
   * @param topic      leadership topic
   * @param identifier instance identifier of the node to anoint
   * @return {@code true} if leadership transfer was successfully executed; {@code false} if it failed.
   * This operation can return {@code false} if the node to be made new leader is not registered to
   * run for election for the topic.
   */
  boolean anoint(String topic, T identifier);

  /**
   * Attempts to promote a node to top of candidate list.
   *
   * @param topic      leadership topic
   * @param identifier instance identifier of the node to promote
   * @return {@code true} if node is now the top candidate. This operation can fail (i.e. return
   * {@code false}) if the node is not registered to run for election for the topic.
   */
  boolean promote(String topic, T identifier);

  /**
   * Attempts to evict a node from all leadership elections it is registered for.
   * <p>
   * If the node the current leader for a topic, this call will force the next candidate (if one exists)
   * to be promoted to leadership.
   *
   * @param identifier instance identifier
   */
  void evict(T identifier);

  /**
   * Returns the {@link Leadership} for the specified topic.
   *
   * @param topic leadership topic
   * @return current Leadership state of the topic
   */
  Leadership<T> getLeadership(String topic);

  /**
   * Returns the current {@link Leadership}s for all topics.
   *
   * @return topic name to Leadership mapping
   */
  Map<String, Leadership<T>> getLeaderships();

  /**
   * Registers a listener to be notified of Leadership changes for all topics.
   *
   * @param listener listener to notify
   */
  void addListener(LeadershipEventListener<T> listener);

  /**
   * Unregisters a previously registered change notification listener.
   * <p>
   * If the specified listener was not previously registered, this operation will be a noop.
   *
   * @param listener listener to remove
   */
  void removeListener(LeadershipEventListener<T> listener);

  /**
   * Registers a listener to be notified of Leadership changes for all topics.
   *
   * @param topic    leadership topic
   * @param listener listener to notify
   */
  void addListener(String topic, LeadershipEventListener<T> listener);

  /**
   * Unregisters a previously registered change notification listener.
   * <p>
   * If the specified listener was not previously registered, this operation will be a noop.
   *
   * @param topic    leadership topic
   * @param listener listener to remove
   */
  void removeListener(String topic, LeadershipEventListener<T> listener);

  @Override
  AsyncLeaderElector<T> async();

  /**
   * Builder for constructing new {@link AsyncLeaderElector} instances.
   */
  abstract class Builder<T> extends DistributedPrimitive.Builder<Builder<T>, LeaderElectorConfig, LeaderElector<T>> {
    public Builder(String name, LeaderElectorConfig config, PrimitiveManagementService managementService) {
      super(LeaderElectorType.instance(), name, config, managementService);
    }

    @Override
    public Serializer serializer() {
      Serializer serializer = this.serializer;
      if (serializer == null) {
        NamespaceConfig config = this.config.getNamespaceConfig();
        if (config == null) {
          serializer = Serializer.using(Namespace.builder()
              .register(Namespaces.BASIC)
              .register(MemberId.class)
              .build());
        } else {
          serializer = Serializer.using(Namespace.builder()
              .register(Namespaces.BASIC)
              .register(MemberId.class)
              .register(new Namespace(config))
              .build());
        }
      }
      return serializer;
    }
  }
}
