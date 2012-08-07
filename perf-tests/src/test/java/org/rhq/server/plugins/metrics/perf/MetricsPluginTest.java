package org.rhq.server.plugins.metrics.perf;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import org.rhq.enterprise.server.plugin.pc.metrics.MetricsServerPluginFacet;
import org.rhq.enterprise.server.plugin.pc.metrics.MetricsServerPluginTestDelegate;

/**
 * @author John Sanda
 */
public class MetricsPluginTest {

    private MetricsServerPluginFacet  metricsServer;

    private MetricsServerPluginTestDelegate testDelegate;

    @BeforeClass
    public void initClass() throws Exception {
        Class<?> pluginClass = Class.forName(System.getProperty("plugin.component"));
        Class<?> testDelegateClass = Class.forName(System.getProperty("plugin.component.test.delegate"));

        metricsServer = (MetricsServerPluginFacet) pluginClass.newInstance();
        testDelegate = (MetricsServerPluginTestDelegate) testDelegateClass.newInstance();

        metricsServer.initialize(testDelegate.createTestContext());
    }

    @Test
    public void insertRawMetrics() {

    }

}
