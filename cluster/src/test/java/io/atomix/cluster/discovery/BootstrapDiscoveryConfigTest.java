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
package io.atomix.cluster.discovery;

import io.atomix.cluster.NodeConfig;
import io.atomix.utils.config.ConfigMapper;
import org.junit.Test;

import java.io.File;
import java.time.Duration;
import java.util.Iterator;

import static org.junit.Assert.assertEquals;

/**
 * Bootstrap discovery configuration class.
 */
public class BootstrapDiscoveryConfigTest {
  @Test
  public void testConfigDefaults() throws Exception {
    BootstrapDiscoveryConfig config = new BootstrapDiscoveryConfig();
    assertEquals(Duration.ofSeconds(1), config.getHeartbeatInterval());
    assertEquals(Duration.ofSeconds(10), config.getFailureTimeout());
    assertEquals(10, config.getFailureThreshold());
  }

  @Test
  public void testLoadConfig() throws Exception {
    File file = new File(getClass().getResource("/bootstrap-test.conf").getFile());
    BootstrapDiscoveryConfig config = new ConfigMapper().loadFile(BootstrapDiscoveryConfig.class, file);
    assertEquals(BootstrapDiscoveryProvider.TYPE, config.getType());
    assertEquals(3, config.getNodes().size());

    Iterator<NodeConfig> iterator = config.getNodes().iterator();
    assertEquals("node1", iterator.next().getId().id());
    assertEquals("node2", iterator.next().getId().id());
    assertEquals("node3", iterator.next().getId().id());
  }
}
