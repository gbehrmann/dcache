package org.dcache.auth.dpm;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import javax.jdo.PersistenceManagerFactory;
import org.springframework.beans.factory.annotation.Required;

/**
 * Component that periodically flushes the second level JDO cache.
 */
public class JdoCacheEvicter
{
    private PersistenceManagerFactory _persistenceManagerFactory;
    private ScheduledExecutorService _executor;
    private ScheduledFuture<?> _future;
    private long _period;

    @Required
    public synchronized void setPersistenceManagerFactory(PersistenceManagerFactory pmf)
    {
        _persistenceManagerFactory = pmf;
    }

    @Required
    public synchronized void setExecutor(ScheduledExecutorService executor)
    {
        _executor = executor;
    }

    public synchronized long getFlushPeriod()
    {
        return _period;
    }

    @Required
    public synchronized void setFlushPeriod(long period)
    {
        _period = period;
        if (_future != null) {
            stop();
            start();
        }
    }

    public synchronized void start()
    {
        if (_future != null) {
            throw new IllegalStateException("Already running");
        }
        _future = _executor.scheduleWithFixedDelay(new EvictionTask(), _period, _period, TimeUnit.MILLISECONDS);
    }

    public synchronized void stop()
    {
        if (_future != null) {
            _future.cancel(false);
            _future = null;
        }
    }

    private class EvictionTask implements Runnable
    {
        public void run()
        {
            _persistenceManagerFactory.getDataStoreCache().evictAll();
        }
    }
}
