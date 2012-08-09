package org.rhq.server.plugins.metrics.infinispan;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.infinispan.Cache;
import org.infinispan.distexec.DefaultExecutorService;
import org.infinispan.distexec.DistributedCallable;
import org.infinispan.distexec.DistributedExecutorService;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.joda.time.DateTime;

import org.rhq.core.domain.auth.Subject;
import org.rhq.core.domain.common.EntityContext;
import org.rhq.core.domain.configuration.Configuration;
import org.rhq.core.domain.criteria.TraitMeasurementCriteria;
import org.rhq.core.domain.measurement.MeasurementDataNumeric;
import org.rhq.core.domain.measurement.MeasurementDataTrait;
import org.rhq.core.domain.measurement.MeasurementSchedule;
import org.rhq.core.domain.measurement.TraitMeasurement;
import org.rhq.core.domain.measurement.calltime.CallTimeData;
import org.rhq.core.domain.measurement.composite.MeasurementDataNumericHighLowComposite;
import org.rhq.core.domain.util.PageList;
import org.rhq.enterprise.server.plugin.pc.ServerPluginComponent;
import org.rhq.enterprise.server.plugin.pc.ServerPluginContext;
import org.rhq.enterprise.server.plugin.pc.metrics.MetricsServerPluginFacet;

/**
 * @author John Sanda
 */
public class InfinispanMetricsPluginComponent implements MetricsServerPluginFacet, ServerPluginComponent {

    public static final String RAW_DATA_CACHE = "RawData";

    public static final String RAW_BATCHES_CACHE = "RawBatches";

    public static final String HOUR0_DATA_INDEX_CACHE = "Hour0DataIndex";
    public static final String HOUR1_DATA_INDEX_CACHE = "Hour1DataIndex";
    public static final String HOUR2_DATA_INDEX_CACHE = "Hour2DataIndex";
    public static final String HOUR3_DATA_INDEX_CACHE = "Hour3DataIndex";
    public static final String HOUR4_DATA_INDEX_CACHE = "Hour4DataIndex";
    public static final String HOUR5_DATA_INDEX_CACHE = "Hour5DataIndex";
    public static final String HOUR6_DATA_INDEX_CACHE = "Hour6DataIndex";
    public static final String HOUR7_DATA_INDEX_CACHE = "Hour7DataIndex";
    public static final String HOUR8_DATA_INDEX_CACHE = "Hour8DataIndex";
    public static final String HOUR9_DATA_INDEX_CACHE = "Hour9DataIndex";
    public static final String HOUR10_DATA_INDEX_CACHE = "Hour10DataIndex";
    public static final String HOUR11_DATA_INDEX_CACHE = "Hour11DataIndex";
    public static final String HOUR12_DATA_INDEX_CACHE = "Hour12DataIndex";

    private EmbeddedCacheManager cacheManager;

    private Map<Integer, String> hourlyIndexCaches = new HashMap<Integer, String>();

    @Override
    public void initialize(ServerPluginContext serverPluginContext) throws Exception {
        Configuration pluginConfig = serverPluginContext.getPluginConfiguration();
        String cacheConfig = pluginConfig.getSimpleValue("cache.config.file");

        cacheManager = new DefaultCacheManager(cacheConfig, true);

        hourlyIndexCaches.put(0, HOUR0_DATA_INDEX_CACHE);
        hourlyIndexCaches.put(1, HOUR1_DATA_INDEX_CACHE);
        hourlyIndexCaches.put(2, HOUR2_DATA_INDEX_CACHE);
        hourlyIndexCaches.put(3, HOUR3_DATA_INDEX_CACHE);
        hourlyIndexCaches.put(4, HOUR4_DATA_INDEX_CACHE);
        hourlyIndexCaches.put(5, HOUR5_DATA_INDEX_CACHE);
        hourlyIndexCaches.put(6, HOUR6_DATA_INDEX_CACHE);
        hourlyIndexCaches.put(7, HOUR7_DATA_INDEX_CACHE);
        hourlyIndexCaches.put(8, HOUR8_DATA_INDEX_CACHE);
        hourlyIndexCaches.put(9, HOUR9_DATA_INDEX_CACHE);
        hourlyIndexCaches.put(10, HOUR10_DATA_INDEX_CACHE);
        hourlyIndexCaches.put(11, HOUR11_DATA_INDEX_CACHE);
        hourlyIndexCaches.put(12, HOUR12_DATA_INDEX_CACHE);
    }

    @Override
    public void start() {
    }

    @Override
    public void stop() {
    }

    @Override
    public void shutdown() {
    }

    @Override
    public void addNumericData(Set<MeasurementDataNumeric> dataSet) {
        Cache<MetricKey, Double> cache = cacheManager.getCache(RAW_DATA_CACHE, true);
        DistributedExecutorService executorService = new DefaultExecutorService(cache);

        Map<MetricKey, Double> rawData = new HashMap<MetricKey, Double>();
        Set<MetricKey> indexUpdates = new HashSet<MetricKey>();

        for (MeasurementDataNumeric data : dataSet) {
            MetricKey key = new MetricKey(data.getScheduleId(), data.getTimestamp());
            rawData.put(key, data.getValue());
            indexUpdates.add(new MetricKey(data.getScheduleId(),
                new DateTime(data.getTimestamp()).hourOfDay().roundFloorCopy().getMillis()));
//            cache.put(key, data.getValue());
            //executorService.submit(new AggregateRawData(), key);
        }
        cache.putAllAsync(rawData);
        Set<MetricKey> keys = rawData.keySet();
        executorService.submit(new CreateRawDataBatches(), keys.toArray(new MetricKey[keys.size()]));
        //executorService.submitEverywhere(new AggregateRawData(), keys.toArray(new MetricKey[keys.size()]));

        for (MetricKey key : indexUpdates) {
            int hour = new DateTime(key.getTimestamp()).hourOfDay().get();
            String index = hourlyIndexCaches.get(hour);
            Cache<MetricKey, Boolean> indexCache = cacheManager.getCache(index, true);
            indexCache.putAsync(key, true);
        }
    }

    @Override
    public void addTraitData(Set<MeasurementDataTrait> measurementDataTraits) {
    }

    @Override
    public void addCallTimeData(Set<CallTimeData> callTimeDatas) {
    }

    @Override
    public void calculateAggregates() {
    }

    @Override
    public List<MeasurementDataNumericHighLowComposite> findDataForContext(Subject subject, EntityContext entityContext,
        MeasurementSchedule schedule, long beginTime, long endTime) {

        Cache<MetricKey, Set<RawData>> rawBatchesCache = cacheManager.getCache(RAW_BATCHES_CACHE);

        // First, determine the keys on which to operate
        Set<MetricKey> keys = new HashSet<MetricKey>();
        DateTime start = new DateTime(beginTime).hourOfDay().roundFloorCopy();
        DateTime end = new DateTime(endTime).hourOfDay().roundFloorCopy();
        DateTime theTime = start;

        while (theTime.isBefore(end.plusHours(1))) {
            keys.add(new MetricKey(schedule.getId(), theTime.getMillis()));
            theTime = theTime.plusHours(1);
        }
//
//        Buckets buckets = new Buckets(start, end);
//
//        // execute map reduce to unroll the batched raw data. The output of the map reduce
//        // job is a map of timestamps to a list of raw values. Each timestamp corresponds
//        // to a bucket.
//        Map<Long, List<Double>> dataPoints =
//            new MapReduceTask<MetricKey, RawDataBach, Long, List<Double>>(rawBatchesCache)
//            .mappedWith(new DataPointsMapper(buckets))
//            .reducedWith(new DataPointsReducer(buckets))
//            .onKeys(keys.toArray(new MetricKey[keys.size()]))
//            .execute();
//
//        for (Long timestamp : dataPoints.keySet()) {
//            List<Double> values = dataPoints.get(timestamp);
//            for (Double value : values) {
//                buckets.insert(timestamp, value);
//            }
//        }
//
//        List<MeasurementDataNumericHighLowComposite> data = new ArrayList<MeasurementDataNumericHighLowComposite>();
//        for (int i = 0; i < buckets.getNumDataPoints(); ++i) {
//            Buckets.Bucket bucket = buckets.get(i);
//            data.add(new MeasurementDataNumericHighLowComposite(bucket.getStartTime(), bucket.getAvg(),
//                bucket.getMax(), bucket.getMin()));
//        }
//
//        return data;

        DistributedExecutorService executorService = new DefaultExecutorService(rawBatchesCache);
        Future<List<MeasurementDataNumericHighLowComposite>> results = executorService.submit(
            new GenerateDataPoints(start.getMillis(), end.getMillis()), keys.toArray(new MetricKey[keys.size()]));

        while (!results.isDone()) {
            try {
                Thread.sleep(10L);
            } catch (InterruptedException e) {
                e.printStackTrace();
                return null;
            }
        }

        try {
            return results.get();
        } catch (InterruptedException e) {
            e.printStackTrace();
            return null;
        } catch (ExecutionException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public List<MeasurementDataNumeric> findRawData(Subject subject, int i, long l, long l1) {

        return Collections.emptyList();
    }

    @Override
    public PageList<? extends TraitMeasurement> findTraitsByCriteria(Subject subject,
        TraitMeasurementCriteria traitMeasurementCriteria) {

        return new PageList();
    }

    EmbeddedCacheManager getCacheManager() {
        return cacheManager;
    }

    private class CreateRawDataBatches implements DistributedCallable<MetricKey, Double, String>, Serializable {
        private static final long serialVersionUID = 1L;

        private Cache<MetricKey, Double> rawDataCache;

        private Set<MetricKey> keys;

        @Override
        public void setEnvironment(Cache<MetricKey, Double> cache, Set<MetricKey> inputKeys) {
            rawDataCache = cache;
            keys = inputKeys;
        }

        @Override
        public String call() throws Exception {
            Cache<MetricKey, RawDataBatch> rawBatchesCache = cacheManager.getCache(RAW_BATCHES_CACHE);
            for (MetricKey key : keys) {
                long theHour = new DateTime(key.getTimestamp()).hourOfDay().roundFloorCopy().getMillis();

                MetricKey batchKey = new MetricKey(key.getScheduleId(), theHour);
                RawDataBatch batch = rawBatchesCache.get(batchKey);

                if (batch == null) {
                    batch = new RawDataBatch();
                }
                batch.addRawData(new RawData(key.getTimestamp(), rawDataCache.get(key)));
                rawBatchesCache.put(batchKey, batch);
            }

            return null;
        }

    }

    private class GenerateDataPoints implements Serializable,
        DistributedCallable<MetricKey, RawDataBatch, List<MeasurementDataNumericHighLowComposite>> {

        private static final long serialVersionUID = 1L;

        private Buckets buckets;

        private Set<MetricKey> keys;

        public GenerateDataPoints(long startTime, long endTime) {
            buckets = new Buckets(startTime, endTime);
        }

        @Override
        public void setEnvironment(Cache<MetricKey, RawDataBatch> cache, Set<MetricKey> inputKeys) {
            keys = inputKeys;
        }

        @Override
        public List<MeasurementDataNumericHighLowComposite> call() throws Exception {
            Cache<MetricKey, RawDataBatch> rawBatchesCache = cacheManager.getCache(RAW_BATCHES_CACHE);
            for (MetricKey key : keys) {
                RawDataBatch batch = rawBatchesCache.get(key);
                if (batch == null) {
                    continue;
                }
                for (RawData datum : batch.getRawData()) {
                    buckets.insert(datum.getTimestamp(), datum.getValue());
                }
            }

            List<MeasurementDataNumericHighLowComposite> data = new ArrayList<MeasurementDataNumericHighLowComposite>();
            for (int i = 0; i < buckets.getNumDataPoints(); ++i) {
                Buckets.Bucket bucket = buckets.get(i);
                data.add(new MeasurementDataNumericHighLowComposite(bucket.getStartTime(), bucket.getAvg(),
                    bucket.getMax(), bucket.getMin()));
            }

            return data;
        }
    }

}
