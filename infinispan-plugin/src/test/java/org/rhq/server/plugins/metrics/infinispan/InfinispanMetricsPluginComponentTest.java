package org.rhq.server.plugins.metrics.infinispan;

import static org.joda.time.DateTime.now;
import static org.rhq.server.plugins.metrics.infinispan.InfinispanMetricsPluginComponent.RAW_AGGREGATES_CACHE;
import static org.rhq.server.plugins.metrics.infinispan.InfinispanMetricsPluginComponent.RAW_DATA_CACHE;
import static org.rhq.test.AssertUtils.assertPropertiesMatch;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.util.HashSet;
import java.util.Set;

import org.infinispan.Cache;
import org.infinispan.manager.EmbeddedCacheManager;
import org.joda.time.DateTime;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.rhq.core.domain.configuration.Configuration;
import org.rhq.core.domain.measurement.DataType;
import org.rhq.core.domain.measurement.MeasurementDataNumeric;
import org.rhq.core.domain.measurement.MeasurementScheduleRequest;
import org.rhq.enterprise.server.plugin.pc.ServerPluginContext;

/**
 * @author John Sanda
 */
public class InfinispanMetricsPluginComponentTest {

    private final long SECOND = 1000;

    private final long MINUTE = 60 * SECOND;

    private InfinispanMetricsPluginComponent metricsServer;

    @BeforeMethod
    public void initServer() throws Exception {
        metricsServer = new InfinispanMetricsPluginComponent();
        metricsServer.initialize(createTestContext());
    }

    private ServerPluginContext createTestContext() {
        Configuration config = new Configuration();
        return new ServerPluginContext(null, null, null, config, null);
    }

    @Test
    public void insertRawData() throws Exception {
        int scheduleId = 123;

        DateTime hour0 = now().hourOfDay().roundFloorCopy().minusHours(now().hourOfDay().get());
        DateTime hour4 = hour0.plusHours(4);
        DateTime now = hour4.plusMinutes(18);
        DateTime threeMinutesAgo = now.minusMinutes(3);
        DateTime twoMinutesAgo = now.minusMinutes(2);
        DateTime oneMinuteAgo = now.minusMinutes(1);

        String scheduleName = getClass().getName() + "_SCHEDULE";
        long interval = MINUTE * 10;
        boolean enabled = true;
        DataType dataType = DataType.MEASUREMENT;
        MeasurementScheduleRequest request = new MeasurementScheduleRequest(scheduleId, scheduleName, interval,
            enabled, dataType);

        Set<MeasurementDataNumeric> data = new HashSet<MeasurementDataNumeric>();
        data.add(new MeasurementDataNumeric(threeMinutesAgo.getMillis(), request, 3.2));
        data.add(new MeasurementDataNumeric(twoMinutesAgo.getMillis(), request, 3.9));
        data.add(new MeasurementDataNumeric(oneMinuteAgo.getMillis(), request, 2.6));

        metricsServer.addNumericData(data);

        // verify that raw data inserted into raw data cache
        EmbeddedCacheManager cacheManager = metricsServer.getCacheManager();
        Cache<String, Double> rawDataCache = cacheManager.getCache(RAW_DATA_CACHE);

        assertEquals(rawDataCache.get(scheduleId + ":" + threeMinutesAgo.getMillis()), 3.2, "Failed to store raw data");
        assertEquals(rawDataCache.get(scheduleId + ":" + twoMinutesAgo.getMillis()), 3.9, "Failed to store raw data");
        assertEquals(rawDataCache.get(scheduleId + ":" + oneMinuteAgo.getMillis()), 2.6, "Failed to store raw data");

        // verify that raw aggregates upserted (i.e., inserted or updated)
        Cache<String, Set<RawData>> rawAggregates = cacheManager.getCache(RAW_AGGREGATES_CACHE);

        Set<RawData> expected = asSet(
            new RawData(threeMinutesAgo.getMillis(), 3.2),
            new RawData(twoMinutesAgo.getMillis(), 3.9),
            new RawData(oneMinuteAgo.getMillis(), 2.6)
        );

        Set<RawData> actual = rawAggregates.get(scheduleId + ":" + hour4.getMillis());

        assertRawDataEquals(actual, expected, "Failed to find raw aggregates");
    }

    private void assertRawDataEquals(Set<RawData> actual, Set<RawData> expected, String msg) {
        assertNotNull(actual, msg);
        assertEquals(actual.size(), expected.size(), msg + " -- " + "The number of raw data aggregates do not match");

        RawData[] actualRaws = actual.toArray(new RawData[actual.size()]);
        RawData[] expectedRaws = actual.toArray(new RawData[actual.size()]);

        for (int i = 0; i < actualRaws.length; ++i) {
            assertPropertiesMatch(expectedRaws[i], actualRaws[i], msg);
        }
    }

    private <T> Set<T> asSet(T... objs) {
        HashSet<T> set = new HashSet<T>();
        for (T obj : objs) {
            set.add(obj);
        }
        return set;
    }

}
