package org.rhq.server.plugins.metrics.infinispan;

import java.io.Serializable;

/**
 * @author John Sanda
 */
public class RawData implements Serializable {
    private static final long serialVersionUID = 1L;

    private long timestamp;

    private Double value;

    public RawData(long timestamp, Double value) {
        this.timestamp = timestamp;
        this.value = value;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public Double getValue() {
        return value;
    }
}
