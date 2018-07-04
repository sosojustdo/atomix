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
package io.atomix.core.transaction;

import io.atomix.primitive.DistributedPrimitive;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.SyncPrimitive;
import io.atomix.primitive.protocol.PrimitiveProtocol;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Transaction primitive.
 */
public interface Transaction extends SyncPrimitive {

  /**
   * Returns the transaction identifier.
   *
   * @return transaction id
   */
  TransactionId transactionId();

  /**
   * Returns the transaction isolation level.
   *
   * @return the transaction isolation level
   */
  Isolation isolation();

  /**
   * Returns if this transaction context is open.
   *
   * @return true if open, false otherwise
   */
  boolean isOpen();

  /**
   * Starts a new transaction.
   */
  void begin();

  /**
   * Commits a transaction that was previously started thereby making its changes permanent
   * and externally visible.
   *
   * @return indicates whether the transaction was successful
   */
  CommitStatus commit();

  /**
   * Aborts any changes made in this transaction context and discarding all locally cached updates.
   */
  void abort();

  /**
   * Returns a new transactional map builder.
   *
   * @param name the map name
   * @param <K>  the key type
   * @param <V>  the value type
   * @return the transactional map builder
   */
  <K, V> TransactionalMap.Builder<K, V> mapBuilder(String name);

  /**
   * Returns a new transactional map builder.
   *
   * @param name     the map name
   * @param protocol the primitive protocol
   * @param <K>      the key type
   * @param <V>      the value type
   * @return the transactional map builder
   */
  default <K, V> TransactionalMap.Builder<K, V> mapBuilder(String name, PrimitiveProtocol protocol) {
    return this.<K, V>mapBuilder(name).withProtocol(protocol);
  }

  /**
   * Returns a new transactional set builder.
   *
   * @param name the set name
   * @param <E>  the set element type
   * @return the transactional set builder
   */
  <E> TransactionalSet.Builder<E> setBuilder(String name);

  /**
   * Returns a new transactional set builder.
   *
   * @param name     the set name
   * @param protocol the primitive protocol
   * @param <E>      the set element type
   * @return the transactional set builder
   */
  default <E> TransactionalSet.Builder<E> setBuilder(String name, PrimitiveProtocol protocol) {
    return this.<E>setBuilder(name).withProtocol(protocol);
  }

  @Override
  AsyncTransaction async();

  /**
   * Transaction configuration.
   */
  class Config extends DistributedPrimitive.Config<Config> {
    private Isolation isolation = Isolation.READ_COMMITTED;

    @Override
    public DistributedPrimitive.Type getType() {
      return TransactionType.instance();
    }

    /**
     * Sets the transaction isolation level.
     *
     * @param isolation the transaction isolation level
     * @return the transaction configuration
     */
    public Config setIsolation(Isolation isolation) {
      this.isolation = checkNotNull(isolation, "isolation cannot be null");
      return this;
    }

    /**
     * Returns the transaction isolation level.
     *
     * @return the transaction isolation level
     */
    public Isolation getIsolation() {
      return isolation;
    }
  }

  /**
   * Transaction builder.
   */
  abstract class Builder extends DistributedPrimitive.Builder<Builder, Config, Transaction> {
    protected Builder(String name, Config config, PrimitiveManagementService managementService) {
      super(TransactionType.instance(), name, config, managementService);
    }

    /**
     * Sets the transaction isolation level.
     *
     * @param isolation the transaction isolation level
     * @return the transaction builder
     */
    public Builder withIsolation(Isolation isolation) {
      config.setIsolation(isolation);
      return this;
    }
  }
}
