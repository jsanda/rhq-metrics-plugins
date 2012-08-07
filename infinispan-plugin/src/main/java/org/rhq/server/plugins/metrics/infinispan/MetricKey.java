package org.rhq.server.plugins.metrics.infinispan;

import java.io.Serializable;

/**
 * @author John Sanda
 */
public class MetricKey implements Serializable {

    private static final long serialVersionUID = 1L;

    private int scheduleId;

    private long timestamp;

    public MetricKey(int scheduleId, long timestamp) {
        this.scheduleId = scheduleId;
        this.timestamp = timestamp;
    }

    public int getScheduleId() {
        return scheduleId;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MetricKey metricKey = (MetricKey) o;

        if (scheduleId != metricKey.scheduleId) return false;
        if (timestamp != metricKey.timestamp) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = scheduleId;
        result = 31 * result + (int) (timestamp ^ (timestamp >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "MetricKey[scheduleId: " + scheduleId + ", timestamp: " + timestamp + "]";
    }
}
