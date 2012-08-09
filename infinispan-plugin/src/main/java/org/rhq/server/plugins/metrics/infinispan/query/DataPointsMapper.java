package org.rhq.server.plugins.metrics.infinispan.query;

import static java.util.Arrays.asList;

import java.util.List;

import org.infinispan.distexec.mapreduce.Collector;
import org.infinispan.distexec.mapreduce.Mapper;

import org.rhq.server.plugins.metrics.infinispan.Buckets;
import org.rhq.server.plugins.metrics.infinispan.MetricData;
import org.rhq.server.plugins.metrics.infinispan.MetricDataBatch;
import org.rhq.server.plugins.metrics.infinispan.MetricKey;

/**
 * @author John Sanda
 */
public class DataPointsMapper implements Mapper<MetricKey, MetricDataBatch, Long, List<Double>> {

    private Buckets buckets;

    public DataPointsMapper(Buckets buckets) {
        this.buckets = buckets;
    }

    @Override
    public void map(MetricKey key, MetricDataBatch value, Collector<Long, List<Double>> collector) {
        if (value == null) {
            return;
        }

        for (MetricData datum : value.getData()) {
            Buckets.Bucket bucket = buckets.find(datum.getTimestamp());
            if (bucket != null) {
                collector.emit(bucket.getStartTime(), asList(datum.getValue()));
            }
        }
    }

}
