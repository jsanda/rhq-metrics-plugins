package org.rhq.server.plugins.metrics.infinispan.query;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.infinispan.distexec.mapreduce.Reducer;

import org.rhq.server.plugins.metrics.infinispan.MetricKey;

/**
 * @author John Sanda
 */
public class GetMetricKeysReducer implements Reducer<Long, List<MetricKey>> {

    @Override
    public List<MetricKey> reduce(Long reducedKey, Iterator<List<MetricKey>> iterator) {
        List<MetricKey> keys = new ArrayList<MetricKey>();
        while (iterator.hasNext()) {
            keys.addAll(iterator.next());
        }
        return keys;
    }
}
