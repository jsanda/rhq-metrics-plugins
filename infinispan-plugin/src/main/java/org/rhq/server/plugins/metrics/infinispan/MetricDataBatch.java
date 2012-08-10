package org.rhq.server.plugins.metrics.infinispan;

import java.io.Serializable;
import java.util.Comparator;
import java.util.Set;
import java.util.TreeSet;

/**
 * @author John Sanda
 */
public class MetricDataBatch implements Serializable {

    private static final long serialVersionUID = 1L;

    private Set<MetricData> data = new TreeSet<MetricData>(new Comparator<MetricData>() {
        @Override
        public int compare(MetricData d1, MetricData d2) {
            if (d1.getTimestamp() < d2.getTimestamp()) {
                return -1;
            } else if (d1.getTimestamp() > d2.getTimestamp()) {
                return 1;
            } else {
                return 0;
            }
        }
    });

    private Double max;

    private Double min;

    private Double sum = 0.0;

    public void addData(MetricData data) {
        this.data.add(data);
        sum += data.getValue();
        if (this.data.size() == 1) {
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

    public Set<MetricData> getData() {
        return data;
    }

    public double getMax() {
        return max;
    }

    public double getMin() {
        return min;
    }

    public double getAvg() {
        return sum / data.size();
    }

}
