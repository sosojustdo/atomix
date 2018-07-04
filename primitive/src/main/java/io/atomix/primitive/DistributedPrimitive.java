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
package io.atomix.primitive;

import com.google.common.base.Joiner;
import io.atomix.primitive.partition.PartitionGroup;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.primitive.resource.PrimitiveResource;
import io.atomix.primitive.service.PrimitiveService;
import io.atomix.primitive.service.ServiceConfig;
import io.atomix.utils.ConfiguredType;
import io.atomix.utils.config.ConfigurationException;
import io.atomix.utils.config.NamedConfig;
import io.atomix.utils.config.TypedConfig;
import io.atomix.utils.serializer.Namespace;
import io.atomix.utils.serializer.NamespaceConfig;
import io.atomix.utils.serializer.Namespaces;
import io.atomix.utils.serializer.Serializer;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Interface for all distributed primitives.
 */
public interface DistributedPrimitive {

  /**
   * Primitive type.
   */
  interface Type<B extends DistributedPrimitive.Builder, C extends DistributedPrimitive.Config, P extends DistributedPrimitive> extends ConfiguredType<C> {

    /**
     * Returns the primitive type namespace.
     *
     * @return the primitive type namespace
     */
    default Namespace namespace() {
      return Namespace.builder()
          .register(Namespaces.BASIC)
          .register(ServiceConfig.class)
          .build();
    }

    /**
     * Returns a new instance of the primitive configuration.
     *
     * @return a new instance of the primitive configuration
     */
    @Override
    C newConfig();

    /**
     * Returns a new primitive builder.
     *
     * @param primitiveName     the primitive name
     * @param config            the primitive configuration
     * @param managementService the primitive management service
     * @return a new primitive builder
     */
    B newBuilder(String primitiveName, C config, PrimitiveManagementService managementService);

    /**
     * Creates a new service instance from the given configuration.
     *
     * @param config the service configuration
     * @return the service instance
     */
    PrimitiveService newService(ServiceConfig config);

    /**
     * Creates a new resource for the given primitive.
     *
     * @param primitive the primitive instance
     * @return a new resource for the given primitive instance
     */
    default PrimitiveResource newResource(P primitive) {
      return null;
    }
  }

  /**
   * Default timeout for primitive operations.
   */
  long DEFAULT_OPERATION_TIMEOUT_MILLIS = 5000L;

  /**
   * Returns the name of this primitive.
   *
   * @return name
   */
  String name();

  /**
   * Returns the type of primitive.
   *
   * @return primitive type
   */
  Type type();

  /**
   * Returns the primitive protocol.
   *
   * @return the primitive protocol
   */
  PrimitiveProtocol protocol();

  /**
   * Registers a listener to be called when the primitive's state changes.
   *
   * @param listener The listener to be called when the state changes.
   */
  default void addStateChangeListener(Consumer<PrimitiveState> listener) {
  }

  /**
   * Unregisters a previously registered listener to be called when the primitive's state changes.
   *
   * @param listener The listener to unregister
   */
  default void removeStateChangeListener(Consumer<PrimitiveState> listener) {
  }

  /**
   * Primitive configuration.
   */
  abstract class Config<C extends Config<C>> implements TypedConfig<Type>, NamedConfig<C> {
    private static final int DEFAULT_CACHE_SIZE = 1000;

    private String name;
    private NamespaceConfig namespaceConfig;
    private PrimitiveProtocol.Config protocolConfig;
    private boolean cacheEnabled = false;
    private int cacheSize = DEFAULT_CACHE_SIZE;
    private boolean readOnly = false;

    @Override
    public String getName() {
      return name;
    }

    @Override
    @SuppressWarnings("unchecked")
    public C setName(String name) {
      this.name = name;
      return (C) this;
    }

    /**
     * Returns the serializer configuration.
     *
     * @return the serializer configuration
     */
    public NamespaceConfig getNamespaceConfig() {
      return namespaceConfig;
    }

    /**
     * Sets the serializer configuration.
     *
     * @param namespaceConfig the serializer configuration
     * @return the primitive configuration
     */
    @SuppressWarnings("unchecked")
    public C setNamespaceConfig(NamespaceConfig namespaceConfig) {
      this.namespaceConfig = namespaceConfig;
      return (C) this;
    }

    /**
     * Returns the protocol configuration.
     *
     * @return the protocol configuration
     */
    public PrimitiveProtocol.Config getProtocolConfig() {
      return protocolConfig;
    }

    /**
     * Sets the protocol configuration.
     *
     * @param protocolConfig the protocol configuration
     * @return the primitive configuration
     */
    @SuppressWarnings("unchecked")
    public C setProtocolConfig(PrimitiveProtocol.Config protocolConfig) {
      this.protocolConfig = protocolConfig;
      return (C) this;
    }

    /**
     * Enables caching for the primitive.
     *
     * @return the primitive configuration
     */
    public C setCacheEnabled() {
      return setCacheEnabled(true);
    }

    /**
     * Sets whether caching is enabled.
     *
     * @param cacheEnabled whether caching is enabled
     * @return the primitive configuration
     */
    @SuppressWarnings("unchecked")
    public C setCacheEnabled(boolean cacheEnabled) {
      this.cacheEnabled = cacheEnabled;
      return (C) this;
    }

    /**
     * Returns whether caching is enabled.
     *
     * @return whether caching is enabled
     */
    public boolean isCacheEnabled() {
      return cacheEnabled;
    }

    /**
     * Sets the cache size.
     *
     * @param cacheSize the cache size
     * @return the primitive configuration
     */
    @SuppressWarnings("unchecked")
    public C setCacheSize(int cacheSize) {
      this.cacheSize = cacheSize;
      return (C) this;
    }

    /**
     * Returns the cache size.
     *
     * @return the cache size
     */
    public int getCacheSize() {
      return cacheSize;
    }

    /**
     * Sets the primitive to read-only.
     *
     * @return the primitive configuration
     */
    public C setReadOnly() {
      return setReadOnly(true);
    }

    /**
     * Sets whether the primitive is read-only.
     *
     * @param readOnly whether the primitive is read-only
     * @return the primitive configuration
     */
    @SuppressWarnings("unchecked")
    public C setReadOnly(boolean readOnly) {
      this.readOnly = readOnly;
      return (C) this;
    }

    /**
     * Returns whether the primitive is read-only.
     *
     * @return whether the primitive is read-only
     */
    public boolean isReadOnly() {
      return readOnly;
    }
  }

  /**
   * Abstract builder for distributed primitives.
   *
   * @param <B> builder type
   * @param <P> primitive type
   */
  abstract class Builder<B extends Builder<B, C, P>, C extends Config, P extends DistributedPrimitive> implements io.atomix.utils.Builder<P> {
    protected final Type type;
    protected final String name;
    protected final C config;
    protected Serializer serializer;
    protected PrimitiveProtocol protocol;
    protected final PrimitiveManagementService managementService;

    protected Builder(Type type, String name, C config, PrimitiveManagementService managementService) {
      this.type = checkNotNull(type, "type cannot be null");
      this.name = checkNotNull(name, "name cannot be null");
      this.config = checkNotNull(config, "config cannot be null");
      this.managementService = checkNotNull(managementService, "managementService cannot be null");
    }

    /**
     * Sets the serializer to use for transcoding info held in the primitive.
     *
     * @param serializer serializer
     * @return this builder
     */
    @SuppressWarnings("unchecked")
    public B withSerializer(Serializer serializer) {
      this.serializer = serializer;
      return (B) this;
    }

    /**
     * Sets the primitive protocol.
     *
     * @param protocol the primitive protocol
     * @return the primitive builder
     */
    @SuppressWarnings("unchecked")
    public B withProtocol(PrimitiveProtocol protocol) {
      this.protocol = protocol;
      return (B) this;
    }

    /**
     * Enables caching for the primitive.
     *
     * @return the primitive builder
     */
    @SuppressWarnings("unchecked")
    public B withCacheEnabled() {
      config.setCacheEnabled();
      return (B) this;
    }

    /**
     * Sets whether caching is enabled.
     *
     * @param cacheEnabled whether caching is enabled
     * @return the primitive builder
     */
    @SuppressWarnings("unchecked")
    public B withCacheEnabled(boolean cacheEnabled) {
      config.setCacheEnabled(cacheEnabled);
      return (B) this;
    }

    /**
     * Sets the cache size.
     *
     * @param cacheSize the cache size
     * @return the primitive builder
     */
    @SuppressWarnings("unchecked")
    public B withCacheSize(int cacheSize) {
      config.setCacheSize(cacheSize);
      return (B) this;
    }

    /**
     * Sets the primitive to read-only.
     *
     * @return the primitive builder
     */
    @SuppressWarnings("unchecked")
    public B withReadOnly() {
      config.setReadOnly();
      return (B) this;
    }

    /**
     * Sets whether the primitive is read-only.
     *
     * @param readOnly whether the primitive is read-only
     * @return the primitive builder
     */
    @SuppressWarnings("unchecked")
    public B withReadOnly(boolean readOnly) {
      config.setReadOnly(readOnly);
      return (B) this;
    }

    /**
     * Returns the name of the primitive.
     *
     * @return primitive name
     */
    public String name() {
      return name;
    }

    /**
     * Returns the primitive type.
     *
     * @return primitive type
     */
    public Type primitiveType() {
      return type;
    }

    /**
     * Returns the primitive protocol.
     *
     * @return the primitive protocol
     */
    public PrimitiveProtocol protocol() {
      PrimitiveProtocol protocol = this.protocol;
      if (protocol == null) {
        PrimitiveProtocol.Config<?> protocolConfig = config.getProtocolConfig();
        if (protocolConfig == null) {
          Collection<PartitionGroup> partitionGroups = managementService.getPartitionService().getPartitionGroups();
          if (partitionGroups.size() == 1) {
            protocol = partitionGroups.iterator().next().newProtocol();
          } else {
            String groups = Joiner.on(", ").join(partitionGroups.stream()
                .map(group -> group.name())
                .collect(Collectors.toList()));
            throw new ConfigurationException(String.format("Primitive protocol is ambiguous: %d partition groups found (%s)", partitionGroups.size(), groups));
          }
        } else {
          protocol = protocolConfig.getType().newProtocol(protocolConfig);
        }
      }
      return protocol;
    }

    /**
     * Returns the serializer.
     *
     * @return serializer
     */
    public Serializer serializer() {
      Serializer serializer = this.serializer;
      if (serializer == null) {
        NamespaceConfig config = this.config.getNamespaceConfig();
        if (config == null) {
          serializer = Serializer.using(Namespaces.BASIC);
        } else {
          serializer = Serializer.using(Namespace.builder()
              .register(Namespaces.BASIC)
              .register(new Namespace(config))
              .build());
        }
      }
      return serializer;
    }

    /**
     * Constructs an instance of the distributed primitive.
     *
     * @return distributed primitive
     */
    @Override
    public P build() {
      return buildAsync().join();
    }

    /**
     * Constructs an instance of the asynchronous primitive.
     *
     * @return asynchronous distributed primitive
     */
    public abstract CompletableFuture<P> buildAsync();
  }
}
