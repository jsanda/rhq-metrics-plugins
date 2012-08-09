package org.rhq.server.plugins.metrics.infinispan;

import java.io.Serializable;

/**
 * @author John Sanda
 */
public class MetricData implements Serializable {
    private static final long serialVersionUID = 1L;

    private long timestamp;

    private Double value;

    private Double max;

    private Double min;

    private Double avg;

    public MetricData(long timestamp, Double value) {
        this.timestamp = timestamp;
        this.value = value;
        max = value;
        min = value;
        avg = value;
    }

    public MetricData(long timestamp, Double avg, Double min, Double max) {
        this.timestamp = timestamp;
        this.avg = avg;
        this.min = min;
        this.max = max;
        value = avg;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public Double getValue() {
        return value;
    }

    public Double getMax() {
        return max;
    }

    public Double getMin() {
        return min;
    }

    public Double getAvg() {
        return avg;
    }
}
