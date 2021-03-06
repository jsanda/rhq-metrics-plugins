package org.rhq.server.plugins.metrics.perf;

import static org.joda.time.DateTime.now;

import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import org.rhq.core.domain.measurement.MeasurementDataNumeric;
import org.rhq.enterprise.server.plugin.pc.metrics.MetricsServerPluginFacet;
import org.rhq.enterprise.server.plugin.pc.metrics.MetricsServerPluginTestDelegate;

/**
 * @author John Sanda
 */
public class MetricsPluginTest {

    private Logger log = Logger.getLogger(MetricsPluginTest.class);

    private MetricsServerPluginFacet  metricsServer;

    private MetricsServerPluginTestDelegate testDelegate;

    @BeforeClass
    public void initClass() throws Exception {
        Class<?> pluginClass = Class.forName(System.getProperty("plugin.component"));
        Class<?> testDelegateClass = Class.forName(System.getProperty("plugin.component.test.delegate"));

        metricsServer = (MetricsServerPluginFacet) pluginClass.newInstance();
        testDelegate = (MetricsServerPluginTestDelegate) testDelegateClass.newInstance();

        metricsServer.initialize(testDelegate.createTestContext());

        log.debug("Initialized " + metricsServer.getClass().getName());
    }

    @Test
    public void insertRawMetrics() {
        log.debug("starting insertRawMetrics()");

        int numReports = 10;
        int numSchedulesPerReport = 10;

        DateTime startTime = now().minusDays(1);
        DateTime collectionTime = startTime;

        long start = System.currentTimeMillis();

        for (int i = 0; i < numReports; ++i) {
            Set<MeasurementDataNumeric> data = new HashSet<MeasurementDataNumeric>();
            for (int j = 0; j < numSchedulesPerReport; ++j) {
                collectionTime = collectionTime.plusMillis(2);
                data.add(new MeasurementDataNumeric(collectionTime.getMillis(), j, 1.1));
            }
            collectionTime = collectionTime.plusSeconds(10);
            metricsServer.addNumericData(data);
        }

        long end = System.currentTimeMillis();
        log.debug("Finished inserting raw metrics in " + (end - start) + " ms");
    }

}
