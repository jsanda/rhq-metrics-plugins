package org.rhq.server.plugins.metrics.infinispan.query;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.infinispan.distexec.mapreduce.Reducer;

import org.rhq.server.plugins.metrics.infinispan.Buckets;

/**
 * @author John Sanda
 */
public class DataPointsReducer implements Reducer<Long, List<Double>> {

    private Buckets buckets;

    public DataPointsReducer(Buckets buckets) {
        this.buckets = buckets;
    }

    @Override
    public List<Double> reduce(Long reducedKey, Iterator<List<Double>> iterator) {
        List<Double> values = new ArrayList<Double>();

        while (iterator.hasNext()) {
            values.addAll(iterator.next());
        }

        return values;
    }

}
