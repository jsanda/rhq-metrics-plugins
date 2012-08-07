package org.rhq.server.plugins.metrics.infinispan;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.infinispan.Cache;
import org.infinispan.distexec.DefaultExecutorService;
import org.infinispan.distexec.DistributedCallable;
import org.infinispan.distexec.DistributedExecutorService;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.joda.time.DateTime;

import org.rhq.core.domain.auth.Subject;
import org.rhq.core.domain.common.EntityContext;
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

    public static final String RAW_AGGREGATES_CACHE = "RawAggregates";

    private EmbeddedCacheManager cacheManager;

    @Override
    public void initialize(ServerPluginContext serverPluginContext) throws Exception {
        cacheManager = new DefaultCacheManager("infinispan.xml", true);

        // possible dummy code to get the plugin to do something
        /*
        Cache c = cacheManager.getCache();
        c.put("TestKey", new Integer(1));
        */
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

        for (MeasurementDataNumeric data : dataSet) {
            MetricKey key = new MetricKey(data.getScheduleId(), data.getTimestamp());
            cache.put(key, data.getValue());
            executorService.submit(new AggregateRawData(), key);
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
    public List<MeasurementDataNumericHighLowComposite> findDataForContext(Subject subject,
        EntityContext entityContext, MeasurementSchedule measurementSchedule, long l, long l1) {

        List<MeasurementDataNumericHighLowComposite> result = Collections.emptyList();

        // possible dummy code to get the plugin to do something
        /*
        Cache<String, Integer> c = new DefaultCacheManager().getCache();
        Integer v = c.get("testKey");
        List<MeasurementDataNumericHighLowComposite> result = new ArrayList<MeasurementDataNumericHighLowComposite>();
        for (long i = 0, time = System.currentTimeMillis(); i < 60; ++i, time -= 60000L) {
            result.add(new MeasurementDataNumericHighLowComposite(time, v, v, v));
        }
        */

        return result;
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

    private class AggregateRawData implements DistributedCallable<MetricKey, Double, String> {

        private Cache<MetricKey, Double> rawDataCache;

        private Set<MetricKey> keys;

        @Override
        public void setEnvironment(Cache<MetricKey, Double> cache, Set<MetricKey> inputKeys) {
            rawDataCache = cache;
            keys = inputKeys;
        }

        @Override
        public String call() throws Exception {
            Cache<MetricKey, Set<RawData>> rawAggregatesCache = cacheManager.getCache(RAW_AGGREGATES_CACHE);
            for (MetricKey key : keys) {
                long theHour = new DateTime(key.getTimestamp()).hourOfDay().roundFloorCopy().getMillis();

                MetricKey aggregatesKey = new MetricKey(key.getScheduleId(), theHour);
                Set<RawData> rawData = rawAggregatesCache.get(aggregatesKey);

                if (rawData == null) {
                    rawData = new HashSet<RawData>();
                }

                rawData.add(new RawData(key.getTimestamp(), rawDataCache.get(key)));
                rawAggregatesCache.put(aggregatesKey    , rawData);
            }

            return null;
        }

    }

}
