/*
 * Copyright 2015-present Open Networking Foundation
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
package io.atomix.core.set;

import io.atomix.core.collection.DistributedCollection;
import io.atomix.primitive.DistributedPrimitive;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.PrimitiveType;

import java.util.Set;

/**
 * A distributed collection designed for holding unique elements.
 *
 * @param <E> set entry type
 */
public interface DistributedSet<E> extends DistributedCollection<E>, Set<E> {
  @Override
  AsyncDistributedSet<E> async();

  /**
   * Distributed set configuration.
   */
  class Config extends DistributedPrimitive.Config<Config> {
    @Override
    public PrimitiveType getType() {
      return DistributedSetType.instance();
    }
  }

  /**
   * Builder for distributed set.
   *
   * @param <E> type set elements.
   */
  abstract class Builder<E> extends DistributedPrimitive.Builder<Builder<E>, Config, DistributedSet<E>> {
    protected Builder(String name, Config config, PrimitiveManagementService managementService) {
      super(DistributedSetType.instance(), name, config, managementService);
    }
  }
}
