package org.rhq.server.plugins.metrics.cassandra;

import java.util.List;

import org.rhq.core.domain.auth.Subject;
import org.rhq.core.domain.configuration.Configuration;
import org.rhq.core.domain.configuration.PropertySimple;
import org.rhq.enterprise.server.plugin.pc.ServerPluginContext;
import org.rhq.enterprise.server.plugin.pc.metrics.AggregateTestData;
import org.rhq.enterprise.server.plugin.pc.metrics.MetricsServerPluginTestDelegate;

/**
 * @author John Sanda
 */
public class CassandraMetricsTestDelegate implements MetricsServerPluginTestDelegate {

    private final String RAW_METRIC_DATA_CF = "raw_metrics";

    private final String ONE_HOUR_METRIC_DATA_CF = "one_hour_metric_data";

    private final String SIX_HOUR_METRIC_DATA_CF = "six_hour_metric_data";

    private final String TWENTY_FOUR_HOUR_METRIC_DATA_CF = "twenty_four_hour_metric_data";

    private final String METRICS_WORK_QUEUE_CF = "metrics_work_queue";

    private final String TRAITS_CF = "traits";

    private final String RESOURCE_TRAITS_CF = "resource_traits";

    @Override
    public ServerPluginContext createTestContext() {
        Configuration configuration = new Configuration();
        configuration.put(new PropertySimple("clusterName", "rhq"));
        configuration.put(new PropertySimple("hostIP", "localhost:9160"));
        configuration.put(new PropertySimple("keyspace", "rhq"));
        configuration.put(new PropertySimple("rawMetricsColumnFamily", RAW_METRIC_DATA_CF));
        configuration.put(new PropertySimple("oneHourMetricsColumnFamily", ONE_HOUR_METRIC_DATA_CF));
        configuration.put(new PropertySimple("sixHourMetricsColumnFamily", SIX_HOUR_METRIC_DATA_CF));
        configuration.put(new PropertySimple("twentyFourHourMetricsColumnFamily", TWENTY_FOUR_HOUR_METRIC_DATA_CF));
        configuration.put(new PropertySimple("metricsQueueColumnFamily", METRICS_WORK_QUEUE_CF));
        configuration.put(new PropertySimple("traitsColumnFamily", TRAITS_CF));
        configuration.put(new PropertySimple("resourceTraitsColumnFamily", RESOURCE_TRAITS_CF));

        return new ServerPluginContext(null, null, null, configuration, null);
    }

    @Override
    public void purgeRawData() {

    }

    @Override
    public void purge1HourData() {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void purge6HourData() {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void purge24HourData() {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void insert1HourData(List<AggregateTestData> aggregateTestDatas) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public List<AggregateTestData> find1HourData(Subject subject, int i, long l, long l1) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public List<AggregateTestData> find6HourData(Subject subject, int i, long l, long l1) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }
}
