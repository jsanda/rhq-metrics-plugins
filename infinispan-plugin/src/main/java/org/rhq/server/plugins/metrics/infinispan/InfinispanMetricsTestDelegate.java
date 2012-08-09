package org.rhq.server.plugins.metrics.infinispan;

import java.util.List;

import org.rhq.core.domain.auth.Subject;
import org.rhq.core.domain.configuration.Configuration;
import org.rhq.core.domain.configuration.PropertySimple;
import org.rhq.enterprise.server.plugin.pc.ServerPluginContext;
import org.rhq.enterprise.server.plugin.pc.metrics.AggregateTestData;
import org.rhq.enterprise.server.plugin.pc.metrics.MetricsServerPluginTestDelegate;

/**
 * @author John Sanda
 */
public class InfinispanMetricsTestDelegate implements MetricsServerPluginTestDelegate {

    @Override
    public ServerPluginContext createTestContext() {
        Configuration config = new Configuration();
        config.put(new PropertySimple("cache.config.file", "infinispan.xml"));

        return new ServerPluginContext(null, null, null, new Configuration(), null);
    }

    @Override
    public void purgeRawData() {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void purge1HourData() {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void purge6HourData() {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void purge24HourData() {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void insert1HourData(List<AggregateTestData> aggregateTestDatas) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public List<AggregateTestData> find1HourData(Subject subject, int i, long l, long l1) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public List<AggregateTestData> find6HourData(Subject subject, int i, long l, long l1) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }
}
