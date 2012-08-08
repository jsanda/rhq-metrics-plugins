package org.rhq.server.plugins.metrics.infinispan.query;

import static java.util.Arrays.asList;

import java.util.List;
import java.util.Set;

import org.infinispan.distexec.mapreduce.Collector;
import org.infinispan.distexec.mapreduce.Mapper;

import org.rhq.server.plugins.metrics.infinispan.Buckets;
import org.rhq.server.plugins.metrics.infinispan.MetricKey;
import org.rhq.server.plugins.metrics.infinispan.RawData;

/**
 * @author John Sanda
 */
public class DataPointsMapper implements Mapper<MetricKey, Set<RawData>, Long, List<Double>> {

    private Buckets buckets;

    public DataPointsMapper(Buckets buckets) {
        this.buckets = buckets;
    }

    @Override
    public void map(MetricKey key, Set<RawData> value, Collector<Long, List<Double>> collector) {
        if (value == null) {
            return;
        }

        for (RawData rawData : value) {
            Buckets.Bucket bucket = buckets.find(rawData.getTimestamp());
            if (bucket != null) {
                collector.emit(bucket.getStartTime(), asList(rawData.getValue()));
            }
        }
    }

}
