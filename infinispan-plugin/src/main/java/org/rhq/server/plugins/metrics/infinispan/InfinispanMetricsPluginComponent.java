package org.rhq.server.plugins.metrics.infinispan;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;

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

    @Override
    public void initialize(ServerPluginContext serverPluginContext) throws Exception {
        EmbeddedCacheManager cacheManager = new DefaultCacheManager(GlobalConfigurationBuilder
            .defaultClusteredBuilder().transport().addProperty("configurationFile", "jgroups.xml").build(),
            new ConfigurationBuilder().clustering().cacheMode(CacheMode.DIST_SYNC).hash().numOwners(2).build());

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
    public void addNumericData(Set<MeasurementDataNumeric> measurementDataNumerics) {
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

}
