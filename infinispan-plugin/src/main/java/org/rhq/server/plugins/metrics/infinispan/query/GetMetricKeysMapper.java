package org.rhq.server.plugins.metrics.infinispan.query;

import static java.util.Arrays.asList;

import java.util.List;

import org.infinispan.distexec.mapreduce.Collector;
import org.infinispan.distexec.mapreduce.Mapper;

import org.rhq.server.plugins.metrics.infinispan.MetricKey;

/**
 * @author John Sanda
 */
public class GetMetricKeysMapper implements Mapper<MetricKey, Boolean, Long, List<MetricKey>> {

    private long timestamp;

    public GetMetricKeysMapper(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public void map(MetricKey key, Boolean value, Collector<Long, List<MetricKey>> collector) {
        if (key.getTimestamp() == timestamp) {
            collector.emit(timestamp, asList(key));
        }
    }
}
