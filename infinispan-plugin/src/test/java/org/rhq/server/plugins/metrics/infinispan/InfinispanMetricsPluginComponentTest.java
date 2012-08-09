package org.rhq.server.plugins.metrics.infinispan;

import static org.joda.time.DateTime.now;
import static org.rhq.server.plugins.metrics.infinispan.InfinispanMetricsPluginComponent.HOUR4_DATA_INDEX_CACHE;
import static org.rhq.server.plugins.metrics.infinispan.InfinispanMetricsPluginComponent.RAW_BATCHES_CACHE;
import static org.rhq.server.plugins.metrics.infinispan.InfinispanMetricsPluginComponent.RAW_DATA_CACHE;
import static org.rhq.test.AssertUtils.assertPropertiesMatch;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.infinispan.Cache;
import org.infinispan.manager.EmbeddedCacheManager;
import org.joda.time.DateTime;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.rhq.core.domain.configuration.Configuration;
import org.rhq.core.domain.configuration.PropertySimple;
import org.rhq.core.domain.measurement.DataType;
import org.rhq.core.domain.measurement.MeasurementDataNumeric;
import org.rhq.core.domain.measurement.MeasurementSchedule;
import org.rhq.core.domain.measurement.MeasurementScheduleRequest;
import org.rhq.core.domain.measurement.composite.MeasurementDataNumericHighLowComposite;
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
        config.put(new PropertySimple("cache.config.file", "infinispan-test.xml"));

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
        Cache<MetricKey, Double> rawDataCache = cacheManager.getCache(RAW_DATA_CACHE);

        assertEquals(rawDataCache.get(new MetricKey(scheduleId, threeMinutesAgo.getMillis())), 3.2,
            "Failed to store raw data.");
        assertEquals(rawDataCache.get(new MetricKey(scheduleId, twoMinutesAgo.getMillis())), 3.9,
            "Failed to store raw data.");
        assertEquals(rawDataCache.get(new MetricKey(scheduleId, oneMinuteAgo.getMillis())), 2.6,
            "Failed to store raw data.");

        // verify that raw aggregates upserted (i.e., inserted or updated)
        Cache<MetricKey, RawDataBatch> rawBatches = cacheManager.getCache(RAW_BATCHES_CACHE);

        RawDataBatch expected = new RawDataBatch();
        expected.addRawData(new RawData(threeMinutesAgo.getMillis(), 3.2));
        expected.addRawData(new RawData(twoMinutesAgo.getMillis(), 3.9));
        expected.addRawData(new RawData(oneMinuteAgo.getMillis(), 2.6));

        RawDataBatch actual = rawBatches.get(new MetricKey(scheduleId, hour4.getMillis()));

        assertRawDataBatchEquals(actual, expected, "Failed to find raw batch.");

        // verify that the hour data index is updated
        Cache<MetricKey, Boolean> indexCache = cacheManager.getCache(HOUR4_DATA_INDEX_CACHE);
        MetricKey expectedKey = new MetricKey(scheduleId, hour4.getMillis());
        assertNotNull(indexCache.get(expectedKey), "Expected to find " + expectedKey + " in " + HOUR4_DATA_INDEX_CACHE);
    }

    @Test
    public void findRawDataComposites() {
        int scheduleId = 123;
        MeasurementSchedule schedule = new MeasurementSchedule();
        schedule.setId(scheduleId);

        DateTime hour0 = now().hourOfDay().roundFloorCopy().minusHours(now().hourOfDay().get());
        DateTime beginTime = hour0.plusHours(4);
        DateTime endTime = hour0.plusHours(8);

        Buckets buckets = new Buckets(beginTime, endTime);

        Set<MeasurementDataNumeric> data = new HashSet<MeasurementDataNumeric>();
        data.add(new MeasurementDataNumeric(buckets.get(0).getStartTime() + 10, scheduleId, 1.1));
        data.add(new MeasurementDataNumeric(buckets.get(0).getStartTime() + 20, scheduleId, 2.2));
        data.add(new MeasurementDataNumeric(buckets.get(0).getStartTime() + 30, scheduleId, 3.3));
        data.add(new MeasurementDataNumeric(buckets.get(59).getStartTime() + 10, scheduleId, 4.4));
        data.add(new MeasurementDataNumeric(buckets.get(59).getStartTime() + 20, scheduleId, 5.5));
        data.add(new MeasurementDataNumeric(buckets.get(59).getStartTime() + 30, scheduleId, 6.6));

        // add some data outside the range
        data.add(new MeasurementDataNumeric(buckets.get(0).getStartTime() - 100, scheduleId, 1.23));
        data.add(new MeasurementDataNumeric(buckets.get(59).getStartTime() + buckets.getInterval() + 50, scheduleId,
            4.56));

        metricsServer.addNumericData(data);
        List<MeasurementDataNumericHighLowComposite> actualData = metricsServer.findDataForContext(null, null,
            schedule, beginTime.getMillis(), endTime.getMillis());

        assertEquals(actualData.size(), buckets.getNumDataPoints(), "Expected to get back 60 data points.");

        MeasurementDataNumericHighLowComposite expectedBucket0Data = new MeasurementDataNumericHighLowComposite(
            buckets.get(0).getStartTime(), (1.1 + 2.2 + 3.3) / 3, 3.3, 1.1);
        MeasurementDataNumericHighLowComposite expectedBucket59Data = new MeasurementDataNumericHighLowComposite(
            buckets.get(59).getStartTime(), (4.4 + 5.5 + 6.6) / 3, 6.6, 4.4);
        MeasurementDataNumericHighLowComposite expectedBucket29Data = new MeasurementDataNumericHighLowComposite(
            buckets.get(29).getStartTime(), Double.NaN, Double.NaN, Double.NaN);

        assertPropertiesMatch("The data for bucket 0 does not match the expected values.", expectedBucket0Data,
            actualData.get(0));
        assertPropertiesMatch("The data for bucket 59 does not match the expected values.", expectedBucket59Data,
            actualData.get(59));
        assertPropertiesMatch("The data for bucket 29 does not match the expected values.", expectedBucket29Data,
            actualData.get(29));
    }

    private void assertRawDataBatchEquals(RawDataBatch actual, RawDataBatch expected, String msg) {
        assertNotNull(actual, msg);
        assertEquals(actual.getRawData().size(), expected.getRawData().size(), msg +
            " -- The number of raw data in the batches do not match.");

        RawData[] actualRaws = actual.getRawData().toArray(new RawData[actual.getRawData().size()]);
        RawData[] expectedRaws = expected.getRawData().toArray(new RawData[expected.getRawData().size()]);

        for (int i = 0; i < actualRaws.length; ++i) {
            assertPropertiesMatch(expectedRaws[i], actualRaws[i], msg);
        }

        assertPropertiesMatch("Computed hourly aggregate values do not match.", actual, expected, "rawData");
    }

    private <T> Set<T> asSet(T... objs) {
        HashSet<T> set = new HashSet<T>();
        for (T obj : objs) {
            set.add(obj);
        }
        return set;
    }

}
