package org.rhq.server.plugins.metrics.infinispan;

import java.io.Serializable;
import java.util.Comparator;
import java.util.Set;
import java.util.TreeSet;

/**
 * @author John Sanda
 */
public class RawDataBatch implements Serializable {

    private static final long serialVersionUID = 1L;

    private Set<RawData> rawData = new TreeSet<RawData>(new Comparator<RawData>() {
        @Override
        public int compare(RawData r1, RawData r2) {
            if (r1.getTimestamp() < r2.getTimestamp()) {
                return -1;
            } else if (r1.getTimestamp() > r2.getTimestamp()) {
                return 1;
            } else {
                return 0;
            }
        }
    });

    private double max;

    private double min;

    private double sum = 0;

    public void addRawData(RawData data) {
        rawData.add(data);
        sum += data.getValue();
        if (rawData.size() == 1) {
            max = data.getValue();
            min = data.getValue();
            sum = data.getValue();
        } else {
            if (data.getValue() > max) {
                max = data.getValue();
            }

            if (data.getValue() < min) {
                min = data.getValue();
            }
        }
    }

    public Set<RawData> getRawData() {
        return rawData;
    }

    public double getMax() {
        return max;
    }

    public double getMin() {
        return min;
    }

    public double getAvg() {
        return sum / rawData.size();
    }

}
