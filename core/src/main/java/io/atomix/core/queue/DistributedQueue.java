/*
 * Copyright 2018-present Open Networking Foundation
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
package io.atomix.core.queue;

import io.atomix.core.collection.DistributedCollection;
import io.atomix.primitive.DistributedPrimitive;
import io.atomix.primitive.PrimitiveManagementService;

import java.util.Queue;

/**
 * Distributed queue.
 */
public interface DistributedQueue<E> extends DistributedCollection<E>, Queue<E> {

  /**
   * Distributed queue configuration.
   */
  class Config extends DistributedPrimitive.Config<Config> {
    @Override
    public DistributedPrimitive.Type getType() {
      return DistributedQueueType.instance();
    }
  }

  /**
   * Builder for distributed queue.
   *
   * @param <E> queue element type
   */
  abstract class Builder<E> extends DistributedPrimitive.Builder<Builder<E>, Config, DistributedQueue<E>> {
    protected Builder(String name, Config config, PrimitiveManagementService managementService) {
      super(DistributedQueueType.instance(), name, config, managementService);
    }
  }
}
