/*
 * Copyright 2012 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.dempsy.config;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import net.dempsy.lifecycle.annotations.Evictable;
import net.dempsy.lifecycle.annotations.MessageHandler;
import net.dempsy.lifecycle.annotations.MessageKey;
import net.dempsy.lifecycle.annotations.MessageProcessor;
import net.dempsy.lifecycle.annotations.MessageProcessor.CdBuild;
import net.dempsy.messages.Adaptor;
import net.dempsy.messages.Dispatcher;
import net.dempsy.messages.KeySource;
import net.dempsy.lifecycle.annotations.Mp;
import net.dempsy.lifecycle.annotations.Start;

public class TestConfig {

    public static class GoodMessage {
        @MessageKey
        public String key() {
            return "Hello";
        }
    }

    @Mp
    public static class GoodTestMp implements Cloneable {
        @MessageHandler
        public void handle(final GoodMessage string) {}

        @Start
        public void startMethod() {}

        @Evictable
        public boolean evict() {
            return false;
        }

        @Override
        public GoodTestMp clone() throws CloneNotSupportedException {
            return (GoodTestMp) super.clone();
        }
    }

    @Mp
    public static class MultiStartTestMp {
        @MessageHandler
        public void handle(final GoodMessage string) {}

        @Start
        public void startMethod() {}

        @Start
        public void extraStartMethod() {}

    }

    public static class GoodAdaptor implements Adaptor {
        @Override
        public void setDispatcher(final Dispatcher dispatcher) {}

        @Override
        public void start() {}

        @Override
        public void stop() {}

    }

    @Test
    public void testSimpleConfig() throws Throwable {
        final ApplicationDefinition app = new ApplicationDefinition("test");
        final ClusterDefinition cd = new ClusterDefinition("test-slot");
        cd.setMessageProcessor(new MessageProcessor(new GoodTestMp()));
        app.add(cd);

        // if we get to here without an error we should be okay
        app.validate(); // this throws if there's a problem.

        assertNull(app.getSerializer());

        assertNull(app.getRoutingStrategy());

        assertNull(cd.getStatsCollectorFactory());
        assertNull(app.getStatsCollectorFactory());
    }

    @Test
    public void testSimpleConfigTopology() throws Throwable {
        final ApplicationDefinition app = new Topology("test").add("test-slot", new MessageProcessor(new GoodTestMp())).app();

        // if we get to here without an error we should be okay
        app.validate(); // this throws if there's a problem.

        assertNull(app.getSerializer());

        assertNull(app.getRoutingStrategy());

        assertNull(app.getClusterDefinition("test-slot").getStatsCollectorFactory());
        assertNull(app.getStatsCollectorFactory());
    }

    @Test
    public void testConfig() throws Throwable {
        final List<ClusterDefinition> clusterDefs = new ArrayList<ClusterDefinition>();

        Object appSer;
        Object appRs;
        Object appScf;
        final ApplicationDefinition app = new ApplicationDefinition("test").setSerializer(appSer = new Object())
                .setRoutingStrategy(appRs = new Object())
                .setStatsCollectorFactory(appScf = new Object());

        ClusterDefinition cd = new ClusterDefinition("test-slot1").setAdaptor(new GoodAdaptor());
        clusterDefs.add(cd);

        cd = new ClusterDefinition("test-slot2", new MessageProcessor(new GoodTestMp()))
                .setDestinations(new ClusterId(new ClusterId("test", "test-slot3")));
        clusterDefs.add(cd);

        cd = new ClusterDefinition("test-slot3");
        cd.setMessageProcessor(new MessageProcessor(new GoodTestMp()));
        cd.setDestinations(new ClusterId[] { new ClusterId("test", "test-slot4"), new ClusterId("test", "test-slot5") });
        clusterDefs.add(cd);

        cd = new ClusterDefinition("test-slot4").setMessageProcessor(new MessageProcessor(new GoodTestMp()));
        clusterDefs.add(cd);

        cd = new ClusterDefinition("test-slot5").setMessageProcessor(new MessageProcessor(new GoodTestMp()));
        clusterDefs.add(cd);

        Object clusScf;
        cd = new ClusterDefinition("test-slot6").setMessageProcessor(new MessageProcessor(new GoodTestMp()))
                .setStatsCollectorFactory(clusScf = new Object());
        clusterDefs.add(cd);

        cd = new ClusterDefinition("test-slot1.5", new GoodAdaptor());
        assertNotNull(cd.getAdaptor());
        clusterDefs.add(cd);

        app.setClusterDefinitions(clusterDefs);

        // if we get to here without an error we should be okay
        app.validate(); // this throws if there's a problem.

        assertTrue(app.getClusterDefinitions().get(0).isRouteAdaptorType());
        assertEquals(new ClusterId("test", "test-slot2"), app.getClusterDefinitions().get(1).getClusterId());
        assertEquals("test", app.getClusterDefinitions().get(1).getClusterId().applicationName);
        assertEquals("test-slot2", app.getClusterDefinitions().get(1).getClusterId().clusterName);
        assertEquals(new ClusterId("test", "test-slot2").hashCode(), app.getClusterDefinitions().get(1).getClusterId().hashCode());
        assertFalse(new ClusterId("test", "test-slot3").equals(new Object()));
        assertFalse(new ClusterId("test", "test-slot3").equals(null));

        assertEquals(appSer, app.getSerializer());

        assertEquals(appRs, app.getRoutingStrategy());

        assertEquals(appScf, app.getClusterDefinitions().get(0).getStatsCollectorFactory());
        assertEquals(app.getStatsCollectorFactory(), app.getClusterDefinitions().get(1).getStatsCollectorFactory());
        assertEquals(clusScf, app.getClusterDefinitions().get(5).getStatsCollectorFactory());

        assertTrue(app == app.getClusterDefinitions().get(4).getParentApplicationDefinition());

        assertEquals(new ClusterId("test", "test-slot1"), app.getClusterDefinitions().get(0).getClusterId());
    }

    @Test
    public void testConfigTopology() throws Throwable {

        Object appSer;
        final Object appRs;
        Object appScf;
        Object clusScf;
        final ApplicationDefinition app = new Topology("test")
                .serializer(appSer = new Object())
                .routing(appRs = new Object())
                .statsCollector(appScf = new Object())
                .add("test-slot1", new GoodAdaptor())
                .add(new CdBuild("test-slot2").prototype(new GoodTestMp()).downstream("test-slot3").cd())
                .add(new CdBuild("test-slot3").prototype(new GoodTestMp()).downstream("test-slot4", "test-slot5").cd())
                .add(new CdBuild("test-slot4", new GoodTestMp()).cd())
                .add(new CdBuild("test-slot5", new GoodTestMp()).cd())
                .add(new CdBuild("test-slot6", new GoodTestMp()).statsCollector(clusScf = new Object()).cd())
                .add("test-slot1.5", new GoodAdaptor())
                .app();

        assertNotNull(app.getClusterDefinition("test-slot1.5").getAdaptor());

        // if we get to here without an error we should be okay
        app.validate(); // this throws if there's a problem.

        assertTrue(app.getClusterDefinitions().get(0).isRouteAdaptorType());
        assertEquals(new ClusterId("test", "test-slot2"), app.getClusterDefinitions().get(1).getClusterId());
        assertEquals("test", app.getClusterDefinitions().get(1).getClusterId().applicationName);
        assertEquals("test-slot2", app.getClusterDefinitions().get(1).getClusterId().clusterName);
        assertEquals(new ClusterId("test", "test-slot2").hashCode(), app.getClusterDefinitions().get(1).getClusterId().hashCode());
        assertFalse(new ClusterId("test", "test-slot3").equals(new Object()));
        assertFalse(new ClusterId("test", "test-slot3").equals(null));

        assertEquals(appSer, app.getSerializer());

        assertEquals(appRs, app.getRoutingStrategy());

        assertEquals(appScf, app.getClusterDefinitions().get(0).getStatsCollectorFactory());
        assertEquals(app.getStatsCollectorFactory(), app.getClusterDefinitions().get(1).getStatsCollectorFactory());
        assertEquals(clusScf, app.getClusterDefinitions().get(5).getStatsCollectorFactory());

        assertTrue(app == app.getClusterDefinitions().get(4).getParentApplicationDefinition());

        assertEquals(new ClusterId("test", "test-slot1"), app.getClusterDefinitions().get(0).getClusterId());
    }

    @Test(expected = IllegalStateException.class)
    public void testFailNoPrototypeOrAdaptor() throws Throwable {
        final ApplicationDefinition app = new ApplicationDefinition("test");
        final ClusterDefinition cd = new ClusterDefinition("test-slot1");
        app.add(cd); // no prototype or adaptor
        app.validate();
    }

    @Test(expected = IllegalStateException.class)
    public void testFailNoPrototypeOrAdaptorTopology() throws Throwable {
        new Topology("test").add(new CdBuild("test-slot1").cd()).app();
    }

    @Test(expected = IllegalStateException.class)
    public void testFailBadPrototype() throws Throwable {
        final ApplicationDefinition app = new ApplicationDefinition("test");
        final ClusterDefinition cd = new ClusterDefinition("test-slot1");
        cd.setMessageProcessor(new MessageProcessor(new Object())); // has no annotated methods
        app.add(cd);
    }

    @Test(expected = IllegalStateException.class)
    public void testFailBadPrototypeTopology() throws Throwable {
        new Topology("test").add(new CdBuild("test-slot1", new Object()).cd()).app();
    }

    @Test(expected = IllegalStateException.class)
    public void testFailBothPrototypeAndAdaptor() throws Throwable {
        final ApplicationDefinition app = new ApplicationDefinition("test");
        final ClusterDefinition cd = new ClusterDefinition("test-slot1");
        cd.setMessageProcessor(new MessageProcessor(new GoodTestMp()));
        cd.setAdaptor(new GoodAdaptor());
        app.add(cd);
        app.validate();
    }

    @Test(expected = IllegalStateException.class)
    public void testFailBothPrototypeAndAdaptorTopology() throws Throwable {
        new Topology("test").add(new CdBuild("test-slot1").prototype(new GoodTestMp()).adaptor(new GoodAdaptor()).cd()).app();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFailNullClusterDefinition() throws Throwable {
        new ApplicationDefinition("test").add(
                new ClusterDefinition("test-slot1").setMessageProcessor(new MessageProcessor(new GoodTestMp())),
                null,
                new ClusterDefinition("test-slot2").setMessageProcessor(new MessageProcessor(new GoodTestMp())));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFailNullClusterDefinitionTopology() throws Throwable {
        new Topology("test").add("slot1", new MessageProcessor(new GoodTestMp()))
                .add(null)
                .add(new CdBuild("slot2", new GoodTestMp()).cd()).app();
    }

    @Test(expected = IllegalStateException.class)
    public void testFailNoParent() throws Throwable {
        new ClusterDefinition("test-slot1").setMessageProcessor(new MessageProcessor(new GoodTestMp())).validate();
    }

    @Test(expected = IllegalStateException.class)
    public void testFailNoParentTopology() throws Throwable {
        new CdBuild("jnk", new GoodTestMp()).cd().validate();
    }

    @Test(expected = IllegalStateException.class)
    public void testDupCluster() throws Throwable {
        final ApplicationDefinition app = new ApplicationDefinition("test-tooMuchWine-needMore").add(
                new ClusterDefinition("notTheSame").setAdaptor(new GoodAdaptor()),
                new ClusterDefinition("mp-stage1").setMessageProcessor(new MessageProcessor(new GoodTestMp())),
                new ClusterDefinition("mp-stage2-dupped").setMessageProcessor(new MessageProcessor(new GoodTestMp())),
                new ClusterDefinition("mp-stage2-dupped").setMessageProcessor(new MessageProcessor(new GoodTestMp())),
                new ClusterDefinition("mp-stage3").setMessageProcessor(new MessageProcessor(new GoodTestMp())));
        app.validate();
    }

    @Test(expected = IllegalStateException.class)
    public void testDupClusterTopology() throws Throwable {
        new Topology("test-tooMuchWine-needMore").add("notTheSame", new GoodAdaptor())
                .add(new CdBuild("mp-stage1", new GoodTestMp()).cd())
                .add(new CdBuild("mp-stage2-dupped", new GoodTestMp()).cd())
                .add(new CdBuild("mp-stage2-dupped", new GoodTestMp()).cd())
                .add(new CdBuild("mp-stage3", new GoodTestMp()).cd()).app();
    }

    @Test(expected = IllegalStateException.class)
    public void testMultipleStartMethodsDisallowed() throws Throwable {
        final ApplicationDefinition app = new ApplicationDefinition("test-multiple-starts").add(
                new ClusterDefinition("adaptor").setAdaptor(new GoodAdaptor()),
                new ClusterDefinition("good-mp").setMessageProcessor(new MessageProcessor(new GoodTestMp())),
                new ClusterDefinition("bad-mp").setMessageProcessor(new MessageProcessor(new MultiStartTestMp())));
        app.validate();
    }

    @Test(expected = IllegalStateException.class)
    public void testMultipleStartMethodsDisallowedTopology() throws Throwable {
        new Topology("test-multiple-starts").add("adaptor", new GoodAdaptor())
                .add(new CdBuild("good-mp", new GoodTestMp()).cd())
                .add(new CdBuild("bad-mp", new MultiStartTestMp()).cd()).app();
    }

    @Test
    public void testSimpleConfigWithKeyStore() throws Throwable {
        final ApplicationDefinition app = new ApplicationDefinition("test");
        final ClusterDefinition cd = new ClusterDefinition("test-slot");
        cd.setMessageProcessor(new MessageProcessor(new GoodTestMp()));
        cd.setKeySource(new KeySource<Object>() {
            @Override
            public Iterable<Object> getAllPossibleKeys() {
                return null;
            }
        });
        app.add(cd);
        app.validate();
    }

    @Test
    public void testSimpleConfigWithKeyStoreTopology() throws Throwable {
        new Topology("test").add(new CdBuild("test-slot", new GoodTestMp())
                .keySource(new KeySource<Object>() {
                    @Override
                    public Iterable<Object> getAllPossibleKeys() {
                        return null;
                    }
                }).cd()).app();
    }

    @Test(expected = IllegalStateException.class)
    public void testConfigAdaptorWithKeyStore() throws Throwable {
        final ApplicationDefinition app = new ApplicationDefinition("test");
        final ClusterDefinition cd = new ClusterDefinition("test-slot");
        cd.setAdaptor(new Adaptor() {
            @Override
            public void stop() {}

            @Override
            public void start() {}

            @Override
            public void setDispatcher(final Dispatcher dispatcher) {}
        })
                .setKeySource(new KeySource<Object>() {
                    @Override
                    public Iterable<Object> getAllPossibleKeys() {
                        return null;
                    }
                });
        app.add(cd);
        app.validate();
    }

    @Test(expected = IllegalStateException.class)
    public void testConfigAdaptorWithKeyStoreTopology() throws Throwable {
        new Topology("test")
                .add(new CdBuild("test-slot").adaptor(new Adaptor() {
                    @Override
                    public void stop() {}

                    @Override
                    public void start() {}

                    @Override
                    public void setDispatcher(final Dispatcher dispatcher) {}
                }).keySource(new KeySource<Object>() {
                    @Override
                    public Iterable<Object> getAllPossibleKeys() {
                        return null;
                    }
                }).cd()).app();
    }

    @Test(expected = IllegalStateException.class)
    public void testConfigMpWithMultipleEvict() throws Throwable {
        final ApplicationDefinition app = new ApplicationDefinition("test");
        final ClusterDefinition cd = new ClusterDefinition("test-slot");

        @Mp
        class mp implements Cloneable {
            @MessageHandler
            public void handle(final GoodMessage string) {}

            @Start
            public void startMethod() {}

            @Evictable
            public boolean evict2() {
                return false;
            }

            @Evictable
            public boolean evict1() {
                return false;
            }

            @Override
            public Object clone() throws CloneNotSupportedException {
                return super.clone();
            }

        }

        cd.setMessageProcessor(new MessageProcessor(new mp()));
        app.add(cd);
        app.validate();
    }

    @Test(expected = IllegalStateException.class)
    public void testConfigMpWithMultipleEvictTopology() throws Throwable {
        @Mp
        class mp implements Cloneable {
            @MessageHandler
            public void handle(final GoodMessage string) {}

            @Start
            public void startMethod() {}

            @Evictable
            public boolean evict2() {
                return false;
            }

            @Evictable
            public boolean evict1() {
                return false;
            }

            @Override
            public Object clone() throws CloneNotSupportedException {
                return super.clone();
            }
        }

        new Topology("test").add(new CdBuild("slot", new mp()).cd()).app();
    }

    @Test(expected = IllegalStateException.class)
    public void testConfigMpWithWrongReturnTypeEvict1() throws Throwable {
        final ApplicationDefinition app = new ApplicationDefinition("test");
        final ClusterDefinition cd = new ClusterDefinition("test-slot");

        @Mp
        class mp implements Cloneable {
            @MessageHandler
            public void handle(final GoodMessage string) {}

            @Start
            public void startMethod() {}

            @Evictable
            public void evict1() {}

            @Override
            public Object clone() throws CloneNotSupportedException {
                return super.clone();
            }
        }

        cd.setMessageProcessor(new MessageProcessor(new mp()));
        app.add(cd);
        app.validate();
    }

    @Test(expected = IllegalStateException.class)
    public void testConfigMpWithWrongReturnTypeEvict1Topology() throws Throwable {
        @Mp
        class mp implements Cloneable {
            @MessageHandler
            public void handle(final GoodMessage string) {}

            @Start
            public void startMethod() {}

            @Evictable
            public void evict1() {}

            @Override
            public Object clone() throws CloneNotSupportedException {
                return super.clone();
            }
        }
        new Topology("test").add(new CdBuild("slot", new mp()).cd()).app();
    }

    @Test(expected = IllegalStateException.class)
    public void testConfigMpWithWrongReturnTypeEvict2() throws Throwable {
        final ApplicationDefinition app = new ApplicationDefinition("test");
        final ClusterDefinition cd = new ClusterDefinition("test-slot");

        @Mp
        class mp implements Cloneable {
            @MessageHandler
            public void handle(final GoodMessage string) {}

            @Start
            public void startMethod() {}

            @Evictable
            public Object evict1() {
                return null;
            }

            @Override
            public Object clone() throws CloneNotSupportedException {
                return super.clone();
            }
        }

        cd.setMessageProcessor(new MessageProcessor(new mp()));
        app.add(cd);
        app.validate();
    }

    @Test(expected = IllegalStateException.class)
    public void testConfigMpWithWrongReturnTypeEvict2Topology() throws Throwable {
        @Mp
        class mp implements Cloneable {
            @MessageHandler
            public void handle(final GoodMessage string) {}

            @Start
            public void startMethod() {}

            @Evictable
            public Object evict1() {
                return null;
            }

            @Override
            public Object clone() throws CloneNotSupportedException {
                return super.clone();
            }
        }
        new Topology("test").add(new CdBuild("slot", new mp()).cd()).app();
    }

    @Test
    public void testConfigMpWithGoodMPEvict() throws Throwable {
        final ApplicationDefinition app = new ApplicationDefinition("test");
        final ClusterDefinition cd = new ClusterDefinition("test-slot");

        @Mp
        class mp implements Cloneable {
            @MessageHandler
            public void handle(final GoodMessage string) {}

            @Start
            public void startMethod() {}

            @Evictable
            public boolean evict1(final Object arg) {
                return false;
            }

            @Override
            public Object clone() throws CloneNotSupportedException {
                return super.clone();
            }
        }

        cd.setMessageProcessor(new MessageProcessor(new mp()));
        app.add(cd);
        app.validate();
    }

    @Test
    public void testConfigMpWithGoodMPEvictTopology() throws Throwable {

        @Mp
        class mp implements Cloneable {
            @MessageHandler
            public void handle(final GoodMessage string) {}

            @Start
            public void startMethod() {}

            @Evictable
            public boolean evict1(final Object arg) {
                return false;
            }

            @Override
            public Object clone() throws CloneNotSupportedException {
                return super.clone();
            }
        }

        new Topology("test").add(new CdBuild("slot", new mp()).cd()).app();

    }

}
