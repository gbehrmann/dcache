package dmg.cells.nucleus;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Monitor;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import javax.annotation.Nonnull;

import java.io.FileNotFoundException;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import dmg.cells.zookeeper.CellCuratorFramework;
import dmg.util.Pinboard;
import dmg.util.logback.FilterThresholdSet;
import dmg.util.logback.RootFilterThresholds;

import org.dcache.util.BoundedCachedExecutor;
import org.dcache.util.BoundedExecutor;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.consumingIterable;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static org.dcache.util.MathUtils.addWithInfinity;
import static org.dcache.util.MathUtils.subWithInfinity;

/**
 *
 *
 * @author Patrick Fuhrmann
 * @version 0.1, 15 Feb 1998
 */
public class CellNucleus implements ThreadFactory
{
    private static final Logger LOGGER =
            LoggerFactory.getLogger(CellNucleus.class);

    private enum State
    {
        NEW(INITIAL,         false),
        PRE_STARTUP(ACTIVE,  true),
        POST_STARTUP(ACTIVE, true),
        RUNNING(ACTIVE,      false),
        FAILED(REMOVING,     false),
        STOPPING(REMOVING,   false),
        TERMINATED(DEAD,     false);

        /** State included in CellInfo. */
        int externalState;

        /**
         * Whether the cell is currently processing startup callbacks.
         */
        boolean isStarting;

        State(int externalState, boolean isStarting)
        {
            this.externalState = externalState;
            this.isStarting = isStarting;
        }
    }

    private static final int PINBOARD_DEFAULT_SIZE = 200;
    private static final  int    INITIAL  =  0;
    private static final  int    ACTIVE   =  1;
    private static final  int    REMOVING =  2;
    private static final  int    DEAD     =  3;
    private static CellGlue __cellGlue;
    private final  String    _cellName;
    private final  String    _cellType;
    private final  ThreadGroup _threads;
    private final  AtomicInteger _threadCounter = new AtomicInteger();
    private final  Cell      _cell;
    private final  Date      _creationTime   = new Date();

    private volatile State _state = State.NEW;

    //  have to be synchronized map
    private final  Map<UOID, CellLock> _waitHash = new HashMap<>();
    private String _cellClass;

    private final BoundedExecutor _messageExecutor;
    private final AtomicInteger _eventQueueSize = new AtomicInteger();

    /**
     * Timer for periodic low-priority maintenance tasks. Shared among
     * all cell instances. Since a Timer is single-threaded,
     * it is important that the timer is not used for long-running or
     * blocking tasks, nor for time critical tasks.
     */
    private static final ScheduledExecutorService _timer = Executors.newSingleThreadScheduledExecutor(
        new ThreadFactoryBuilder().setDaemon(true).setNameFormat("Cell maintenance task timer").build());

    /**
     * Task for calling the Cell nucleus message timeout mechanism.
     */
    private Future<?> _timeoutTask;

    /**
     * Task starting the cell.
     */
    private ListenableFuture<Void> _startup;

    private Pinboard _pinboard;
    private FilterThresholdSet _loggingThresholds;
    private final BlockingQueue<Runnable> _deferredTasks = new LinkedBlockingQueue<>();
    private volatile long _lastQueueTime;
    private final CellCuratorFramework _curatorFramework;

    private final Monitor _lifeCycleMonitor = new Monitor();

    private final Monitor.Guard isNotStarting = new Monitor.Guard(_lifeCycleMonitor) {
        @Override
        public boolean isSatisfied()
        {
            return !_state.isStarting;
        }
    };

    public CellNucleus(Cell cell, String name, String type, Executor executor)
    {
        String cellName = name.replace('@', '+');

        if (cellName.isEmpty()) {
            cellName = "*";
        }
        if (cellName.charAt(cellName.length() - 1) == '*') {
            if (cellName.length() == 1) {
                cellName = "$-" + getUnique();
            } else {
                cellName = cellName.substring(0, cellName.length() - 1) + "-" + getUnique();
            }
        }

        _cellName = cellName;
        _cellType    = type;

        _cell = cell;
        _cellClass = _cell.getClass().getName();

        setPinboard(new Pinboard(PINBOARD_DEFAULT_SIZE));
        __cellGlue.registerCell(this);

        /* Instantiate management component for log filtering.
         */
        CellNucleus parentNucleus =
                CellNucleus.getLogTargetForCell(MDC.get(CDC.MDC_CELL));
        FilterThresholdSet parentThresholds =
                (parentNucleus.isSystemNucleus() || parentNucleus == this)
                        ? RootFilterThresholds.getInstance()
                        : parentNucleus.getLoggingThresholds();
        setLoggingThresholds(new FilterThresholdSet(parentThresholds));

        _threads = new ThreadGroup(__cellGlue.getMasterThreadGroup(), _cellName + "-threads");

        _messageExecutor = (executor == null) ? new BoundedCachedExecutor(this, 1) : new BoundedExecutor(executor, 1);

        CuratorFramework curatorFramework = __cellGlue.getCuratorFramework();
        if (curatorFramework != null) {
            _curatorFramework = new CellCuratorFramework(curatorFramework, _messageExecutor);
            _curatorFramework.start();
        } else {
            _curatorFramework = null;
        }

        LOGGER.info("Created {}", name);
    }

    /**
     * Returns the CellNucleus to which log messages tagged with a
     * given cell are associated.
     */
    public static CellNucleus getLogTargetForCell(String cell)
    {
        CellNucleus nucleus = null;
        if (__cellGlue != null) {
            if (cell != null) {
                nucleus = __cellGlue.getCell(cell);
            }
            if (nucleus == null) {
                nucleus = __cellGlue.getSystemNucleus();
            }
        }
        return nucleus;
    }

    public static void initCellGlue(String cellDomainName, CuratorFramework curatorFramework)
    {
        checkState(__cellGlue == null);
        __cellGlue = new CellGlue(cellDomainName, curatorFramework);
    }

    public static void startCurator()
    {
        CuratorFramework curatorFramework = __cellGlue.getCuratorFramework();
        if (curatorFramework != null) {
            curatorFramework.start();
        }
    }

    public static void shutdownCellGlue()
    {
        if (__cellGlue != null) {
            __cellGlue.shutdown();
        }
    }

    boolean isSystemNucleus() {
        return this == __cellGlue.getSystemNucleus();
    }

    public String getCellName() { return _cellName; }
    public String getCellType() { return _cellType; }

    public String getCellClass()
    {
        return _cellClass;
    }

    public void setCellClass(String cellClass)
    {
        _cellClass = cellClass;
    }

    public CellAddressCore getThisAddress() {
        return new CellAddressCore(_cellName, __cellGlue.getCellDomainName());
    }

    public String getCellDomainName() {
        return __cellGlue.getCellDomainName();
    }
    public List<String> getCellNames() { return __cellGlue.getCellNames(); }
    public CellInfo getCellInfo(String name) {
        return __cellGlue.getCellInfo(name);
    }
    public CellInfo getCellInfo() {
        return _getCellInfo();
    }

    public Map<String, Object> getDomainContext()
    {
        return __cellGlue.getCellContext();
    }

    public Reader getDomainContextReader(String contextName)
        throws FileNotFoundException  {
        Object o = __cellGlue.getCellContext(contextName);
        if (o == null) {
            throw new
                    FileNotFoundException("Context not found : " + contextName);
        }
        return new StringReader(o.toString());
    }
    public void   setDomainContext(String contextName, Object context) {
        __cellGlue.getCellContext().put(contextName, context);
    }
    public Object getDomainContext(String str) {
        return __cellGlue.getCellContext(str);
    }

    Cell getThisCell() { return _cell; }

    CellInfo _getCellInfo() {
        CellInfo info = new CellInfo();
        info.setCellName(getCellName());
        info.setDomainName(getCellDomainName());
        info.setCellType(getCellType());
        info.setCreationTime(_creationTime);
        try {
            info.setCellVersion(_cell.getCellVersion());
        } catch(Exception e) {}
        try {
            info.setPrivateInfo(_cell.getInfo());
        } catch(Exception e) {
            info.setPrivateInfo("Not yet/No more available\n");
        }
        try {
            info.setShortInfo(_cell.toString());
        } catch(Exception e) {
            info.setShortInfo("Not yet/No more available");
        }
        info.setCellClass(_cellClass);
        try {
            int eventQueueSize = getEventQueueSize();
            info.setEventQueueSize(eventQueueSize);
            info.setExpectedQueueTime((eventQueueSize == 0) ? 0 : _lastQueueTime);
            info.setState(_state.externalState);
            info.setThreadCount(_threads.activeCount());
        } catch(Exception e) {
            info.setEventQueueSize(0);
            info.setState(0);
            info.setThreadCount(0);
        }
        return info;
    }

    public void setLoggingThresholds(FilterThresholdSet thresholds)
    {
        _loggingThresholds = thresholds;
    }

    public FilterThresholdSet getLoggingThresholds()
    {
        return _loggingThresholds;
    }

    public synchronized void setPinboard(Pinboard pinboard)
    {
        _pinboard = pinboard;
    }

    public synchronized Pinboard getPinboard()
    {
        return _pinboard;
    }

    public void setMaximumPoolSize(int size)
    {
        _messageExecutor.setMaximumPoolSize(size);
    }

    public int getMaximumPoolSize()
    {
        return _messageExecutor.getMaximumPoolSize();
    }

    public void setMaximumQueueSize(int size)
    {
        _messageExecutor.setMaximumQueueSize(size);
    }

    public int getMaximumQueueSize()
    {
        return _messageExecutor.getMaximumQueueSize();
    }

    public void  sendMessage(CellMessage msg,
                             boolean locally,
                             boolean remotely)
        throws SerializationException
    {
        if (!msg.isStreamMode()) {
            // Have to do this first to log the right UOID
            msg.touch();
            msg.addSourceAddress(getThisAddress());
        }

        EventLogger.sendBegin(this, msg, "async");
        try {
            __cellGlue.sendMessage(msg, locally, remotely);
        } finally {
            EventLogger.sendEnd(msg);
        }
    }

    /**
     * Sends <code>envelope</code> and waits <code>timeout</code>
     * milliseconds for an answer to arrive.  The answer will bypass
     * the ordinary queuing mechanism and will be delivered before any
     * other asynchronous message.  The answer need to have the
     * getLastUOID set to the UOID of the message send with
     * sendAndWait. If the answer does not arrive withing the specified
     * time interval, the method returns <code>null</code> and the
     * answer will be handled as if it was an ordinary asynchronous
     * message.
     *
     * This method mostly exists for backwards compatibility. dCache code
     * should use CellStub or CellEndpoint.
     *
     * @param envelope the cell message to be sent.
     * @param timeout milliseconds to wait for an answer.
     * @return the answer or null if the timeout was reached.
     * @throws SerializationException if the payload object of this
     *         message is not serializable.
     * @throws NoRouteToCellException if the destination
     *         could not be reached.
     * @throws ExecutionException if an exception was returned.
     */
    public CellMessage sendAndWait(CellMessage envelope, long timeout)
            throws SerializationException, NoRouteToCellException, InterruptedException, ExecutionException
    {
        final SettableFuture<CellMessage> future = SettableFuture.create();
        sendMessage(envelope, true, true,
                    new CellMessageAnswerable()
                    {
                        @Override
                        public void answerArrived(CellMessage request, CellMessage answer)
                        {
                            future.set(answer);
                        }

                        @Override
                        public void exceptionArrived(CellMessage request, Exception exception)
                        {
                            future.setException(exception);
                        }

                        @Override
                        public void answerTimedOut(CellMessage request)
                        {
                            future.set(null);
                        }
                    }, directExecutor(), timeout);
        try {
            return future.get(timeout, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            return null;
        } catch (ExecutionException e) {
            Throwables.propagateIfInstanceOf(e.getCause(), NoRouteToCellException.class);
            Throwables.propagateIfInstanceOf(e.getCause(), SerializationException.class);
            throw e;
        }
    }

    public Map<UOID,CellLock > getWaitQueue()
    {
        synchronized (_waitHash) {
            return new HashMap<>(_waitHash);
        }
    }

    private void executeMaintenanceTasks()
    {
        Collection<CellLock> expired = new ArrayList<>();
        long now = System.currentTimeMillis();

        synchronized (_waitHash) {
            Iterator<CellLock> i = _waitHash.values().iterator();
            while (i.hasNext()) {
                CellLock lock =  i.next();
                if (lock.getTimeout() < now) {
                    expired.add(lock);
                    i.remove();
                }
            }
        }

        //
        // _waitHash can't be used here. Otherwise
        // we will end up in a deadlock (NO LOCKS WHILE CALLING CALLBACKS)
        //
        for (final CellLock lock: expired) {
            try (CDC ignored = lock.getCdc().restore()) {
                try {
                    lock.getExecutor().execute(() -> {
                        CellMessage envelope = lock.getMessage();
                        try {
                            lock.getCallback().answerTimedOut(envelope);
                            EventLogger.sendEnd(envelope);
                        } catch (RejectedExecutionException e) {
                            /* May happen when the callback itself tries to schedule the call
                             * on an executor. Put the request back and let it time out.
                             */
                            synchronized (_waitHash) {
                                _waitHash.put(envelope.getUOID(), lock);
                            }
                            LOGGER.warn("Failed to invoke callback: {}", e.toString());
                        }
                    });
                } catch (RejectedExecutionException e) {
                    /* Put it back and deal with it later.
                     */
                    synchronized (_waitHash) {
                        _waitHash.put(lock.getMessage().getUOID(), lock);
                    }
                    LOGGER.warn("Failed to invoke callback: {}", e.toString());
                } catch (RuntimeException e) {
                    /* Don't let a problem in the callback prevent us from
                     * expiring all messages.
                     */
                    Thread t = Thread.currentThread();
                    t.getUncaughtExceptionHandler().uncaughtException(t, e);
                }
            }
        }

        // Execute delayed operations
        for (Runnable task : consumingIterable(_deferredTasks)) {
            task.run();
        }
    }

    /**
     * Sends <code>msg</code>.
     *
     * The <code>callback</code> argument specifies an object which is informed
     * as soon as an has answer arrived or if the timeout has expired.
     *
     * The callback is run in the supplied executor. The executor may
     * execute the callback inline, but such an executor must only be
     * used if the callback is non-blocking, and the callback should
     * refrain from CPU heavy operations. Care should be taken that
     * the executor isn't blocked by tasks waiting for the callback;
     * such tasks could lead to a deadlock.
     *
     * @param msg the cell message to be sent.
     * @param local whether to attempt delivery to cells in the same domain
     * @param remote whether to attempt delivery to cells in other domains
     * @param callback specifies an object class which will be informed
     *                 as soon as the message arrives.
     * @param executor the executor to run the callback in
     * @param timeout  is the timeout in msec.
     * @exception SerializationException if the payload object of this
     *            message is not serializable.
     */
    public void sendMessage(CellMessage msg,
                            boolean local,
                            boolean remote,
                            final CellMessageAnswerable callback,
                            Executor executor,
                            long timeout)
        throws SerializationException
    {
        // Have to do this first to log the right UOID
        if (!msg.isStreamMode()) {
            msg.touch();
            msg.addSourceAddress(getThisAddress());
        }

        msg.setTtl(timeout);

        final UOID uoid = msg.getUOID();
        final CellLock lock = new CellLock(msg, callback, executor, timeout);

        EventLogger.sendBegin(this, msg, "callback");
        synchronized (_waitHash) {
            _waitHash.put(uoid, lock);
        }
        try {
            __cellGlue.sendMessage(msg, local, remote);
        } catch (SerializationException e) {
            synchronized (_waitHash) {
                _waitHash.remove(uoid);
            }
            EventLogger.sendEnd(msg);
            throw e;
        } catch (RuntimeException e) {
            synchronized (_waitHash) {
                _waitHash.remove(uoid);
            }
            try {
                executor.execute(() -> {
                    try {
                        callback.exceptionArrived(msg, e);
                        EventLogger.sendEnd(msg);
                    } catch (RejectedExecutionException e1) {
                        /* May happen when the callback itself tries to schedule the call
                         * on an executor. Put the request back and let it time out.
                         */
                        synchronized (_waitHash) {
                            _waitHash.put(uoid, lock);
                        }
                        LOGGER.error("Failed to invoke callback: {}", e1.toString());
                    }
                });
            } catch (RejectedExecutionException e1) {
                /* Put it back and let it time out.
                 */
                synchronized (_waitHash) {
                    _waitHash.put(uoid, lock);
                }
                LOGGER.error("Failed to invoke callback: {}", e1.toString());
            }
        }
    }

    public void addCellEventListener(CellEventListener listener) {
        __cellGlue.addCellEventListener(this, new CellEventListener()
        {
            @Override
            public void cellCreated(CellEvent ce)
            {
                try (CDC ignored = CDC.reset(CellNucleus.this)) {
                    listener.cellCreated(ce);
                }
            }

            @Override
            public void cellDied(CellEvent ce)
            {
                try (CDC ignored = CDC.reset(CellNucleus.this)) {
                    listener.cellDied(ce);
                }
            }

            @Override
            public void routeAdded(CellEvent ce)
            {
                try (CDC ignored = CDC.reset(CellNucleus.this)) {
                    listener.routeAdded(ce);
                }
            }

            @Override
            public void routeDeleted(CellEvent ce)
            {
                try (CDC ignored = CDC.reset(CellNucleus.this)) {
                    listener.routeDeleted(ce);
                }
            }
        });
    }

    public void consume(String queue) { __cellGlue.consume(this, queue);  }

    public void subscribe(String topic) { __cellGlue.subscribe(this, topic); }

    /**
     *
     * The kill method schedules the specified cell for deletion.
     * The actual remove operation will run in a different
     * thread. So on return of this method the cell may
     * or may not be alive.
     */
    public void kill() {   __cellGlue.kill(this);  }
    /**
     *
     * The kill method schedules this Cell for deletion.
     * The actual remove operation will run in a different
     * thread. So on return of this method the cell may
     * or may not be alive.
     */
    public void kill(String cellName) throws IllegalArgumentException {
        __cellGlue.kill(this, cellName);
    }


    /**
     * List the threads of some cell to stdout.  This is
     * indended for diagnostic information.
     */
    public void listThreadGroupOf(String cellName) {
        __cellGlue.threadGroupList(cellName);
    }

    /**
     * Print diagnostic information about currently running
     * threads at warn level.
     */
    public void  threadGroupList() {
        Thread[] threads = new Thread[_threads.activeCount()];
        int n = _threads.enumerate(threads);
        for (int i = 0; i < n; i++) {
            Thread thread = threads[i];
            LOGGER.warn("Thread: {} [{}{}{}] ({}) {}",
                    thread.getName(),
                    (thread.isAlive() ? "A" : "-"),
                    (thread.isDaemon() ? "D" : "-"),
                    (thread.isInterrupted() ? "I" : "-"),
                    thread.getPriority(),
                    thread.getState());
            for(StackTraceElement s : thread.getStackTrace()) {
                LOGGER.warn("    {}", s);
            }
        }
    }



    /**
     * Blocks until the given cell is dead.
     *
     * @throws InterruptedException if another thread interrupted the
     * current thread before or while the current thread was waiting
     * for a notification. The interrupted status of the current
     * thread is cleared when this exception is thrown.
     * @return True if the cell died, false in case of a timeout.
     */
    public boolean join(String cellName)
        throws InterruptedException
    {
        return __cellGlue.join(cellName, 0);
    }

    /**
     * Blocks until the given cell is dead.
     *
     * @param timeout the maximum time to wait in milliseconds.
     * @throws InterruptedException if another thread interrupted the
     * current thread before or while the current thread was waiting
     * for a notification. The interrupted status of the current
     * thread is cleared when this exception is thrown.
     * @return True if the cell died, false in case of a timeout.
     */
    public boolean join(String cellName, long timeout)
        throws InterruptedException
    {
        return __cellGlue.join(cellName, timeout);
    }

    /**
     * Returns the non-daemon threads of a thread group.
     */
    private Collection<Thread> getNonDaemonThreads(ThreadGroup group)
    {
        Thread[] threads = new Thread[group.activeCount()];
        int count = group.enumerate(threads);
        Collection<Thread> nonDaemonThreads = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            Thread thread = threads[i];
            if (!thread.isDaemon()) {
                nonDaemonThreads.add(thread);
            }
        }
        return nonDaemonThreads;
    }

    /**
     * Waits for at most timeout milliseconds for the termination of a
     * set of threads.
     *
     * @return true if all threads terminated, false otherwise
     */
    private boolean joinThreads(Collection<Thread> threads, long timeout)
        throws InterruptedException
    {
        long deadline = addWithInfinity(System.currentTimeMillis(), timeout);
        for (Thread thread: threads) {
            if (thread.isAlive()) {
                long wait = subWithInfinity(deadline, System.currentTimeMillis());
                if (wait <= 0) {
                    return false;
                }
                thread.join(wait);
                if (thread.isAlive()) {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * Interrupts a set of threads.
     */
    private void killThreads(Collection<Thread> threads)
    {
        for (Thread thread: threads) {
            if (thread.isAlive()) {
                LOGGER.debug("killerThread : interrupting {}", thread.getName());
                thread.interrupt();
            }
        }
    }

    private Runnable wrapLoggingContext(final Runnable runnable)
    {
        return () -> {
            try (CDC ignored = CDC.reset(CellNucleus.this)) {
                runnable.run();
            }
        };
    }

    private <T> Callable<T> wrapLoggingContext(final Callable<T> callable)
    {
        return () -> {
            try (CDC ignored = CDC.reset(CellNucleus.this)) {
                return callable.call();
            }
        };
    }

    /**
     * Submits a task for execution on the message thread.
     */
    <T> Future<T> invokeOnMessageThread(Callable<T> task)
    {
        return _messageExecutor.submit(wrapLoggingContext(task));
    }

    /**
     * Submits a task for execution on the message thread.
     */
    Future<?> invokeOnMessageThread(Runnable task)
    {
        return _messageExecutor.submit(wrapLoggingContext(task));
    }

    void invokeLater(Runnable runnable)
    {
        _deferredTasks.add(runnable);
    }

    @Override @Nonnull
    public Thread newThread(@Nonnull Runnable target)
    {
        return newThread(target, getCellName() + "-" + _threadCounter.getAndIncrement());
    }

    @Nonnull
    public Thread newThread(@Nonnull Runnable target, @Nonnull String name)
    {
        return CellGlue.newThread(_threads, wrapLoggingContext(target), name);
    }

    //
    //  package
    //
    Thread [] getThreads(String cellName) {
        return __cellGlue.getThreads(cellName);
    }
    public ThreadGroup getThreadGroup() { return _threads; }
    Thread [] getThreads() {
        if (_threads == null) {
            return new Thread[0];
        }

        int threadCount = _threads.activeCount();
        Thread [] list  = new Thread[threadCount];
        int rc = _threads.enumerate(list);
        if (rc == list.length) {
            return list;
        }
        Thread [] ret = new Thread[rc];
        System.arraycopy(list, 0, ret, 0, rc);
        return ret;
    }

    private String getUnique() {
        return __cellGlue.getUnique();
    }

    int getEventQueueSize()
    {
        return _eventQueueSize.get();
    }

    void addToEventQueue(MessageEvent ce)
    {
        CellMessage msg = ce.getMessage();
        LOGGER.trace("addToEventQueue : message arrived : {}", msg);

        CellLock lock;
        synchronized (_waitHash) {
            lock = _waitHash.remove(msg.getLastUOID());
        }

        if (lock != null) {
            //
            // we were waiting for you (sync or async)
            //
            LOGGER.trace("addToEventQueue : lock found for : {}", msg);
            try {
                _eventQueueSize.incrementAndGet();
                lock.getExecutor().execute(new CallbackTask(lock, msg));
            } catch (RejectedExecutionException e) {
                _eventQueueSize.decrementAndGet();
                /* Put it back; the timeout handler will eventually take care of it.
                 */
                synchronized (_waitHash) {
                    _waitHash.put(msg.getLastUOID(), lock);
                }
                LOGGER.error("Dropping reply: {}", e.getMessage());
            }
        } else {
            /* Fail fast for requests if the cell is busy. We consider the cell busy
             * if the last queue time exceeds the TTL of the request.
             */
            if (_eventQueueSize.get() == 0) {
                _lastQueueTime = 0;
            } else if (!msg.isReply()) {
                long queueTime = _lastQueueTime;
                if (msg.getTtl() < queueTime) {
                    CellMessage envelope = new CellMessage(msg.getSourcePath().revert(),
                            new NoRouteToCellException(msg, getCellName() + "@" + getCellDomainName() +
                                                            " is busy (its estimated response time of " +
                                                            queueTime + " ms is longer than the message TTL of " +
                                                            msg.getTtl() + " ms)."));
                    envelope.setLastUOID(msg.getUOID());
                    sendMessage(envelope, true, true);
                }
            }

            try {
                EventLogger.queueBegin(ce);
                _eventQueueSize.incrementAndGet();
                _messageExecutor.execute(new DeliverMessageTask(ce));
            } catch (RejectedExecutionException e) {
                EventLogger.queueEnd(ce);
                _eventQueueSize.decrementAndGet();
                LOGGER.error("Dropping message: {}", e.getMessage());
            }
        }
    }

    private void setState(State newState)
    {
        _lifeCycleMonitor.enter();
        try {
            _state = newState;
        } finally {
            _lifeCycleMonitor.leave();
        }
    }

    /**
     * Starts the cell asynchronously.
     *
     * Calls the startup callbacks of the cell, registers the cell with the cell glue and
     * initiates cell message delivery. If startup fails, the cell is torn down.
     *
     * Must only be called once.
     */
    public ListenableFuture<Void> start()
    {
        _lifeCycleMonitor.enter();
        try {
            checkState(_state == State.NEW);
            _state = State.PRE_STARTUP;
            _startup = _messageExecutor.submit(wrapLoggingContext(this::doStart));
        } finally {
            _lifeCycleMonitor.leave();
        }
        return Futures.nonCancellationPropagating(_startup);
    }

    private Void doStart() throws Exception
    {
        try {
            checkState(_state == State.PRE_STARTUP);
            _timeoutTask = _timer.scheduleWithFixedDelay(wrapLoggingContext(this::executeMaintenanceTasks),
                                                         20, 20, TimeUnit.SECONDS);
            StartEvent event = new StartEvent(new CellPath(_cellName), 0);
            _cell.prepareStartup(event);
            setState(State.POST_STARTUP);
            __cellGlue.publishCell(this);
            _cell.postStartup(event);
            setState(State.RUNNING);
        } catch (Throwable e) {
            setState(State.FAILED);
            __cellGlue.kill(CellNucleus.this);
            throw e;
        }
        return null;
    }

    void shutdown(KillEvent event)
    {
        try (CDC ignored = CDC.reset(CellNucleus.this)) {
            LOGGER.trace("Received {}", event);

            /* Wait for cell initialization to complete to ensure sequential execution of callbacks.
             */
            _lifeCycleMonitor.enter();
            try {
                if (!_lifeCycleMonitor.waitForUninterruptibly(isNotStarting, 2, TimeUnit.SECONDS)) {
                    _startup.cancel(true);
                    _lifeCycleMonitor.waitForUninterruptibly(isNotStarting);
                }
                State state = _state;
                checkState(state == State.NEW || state == State.RUNNING || state == State.FAILED);
                _state = State.STOPPING;
            } finally {
                _lifeCycleMonitor.leave();
            }

            /* Shut down the curator decorator; this just kills the internal executor of the decorator
             * while still allowing it to be used for operations without callbacks.
             */
            if (_curatorFramework != null) {
                _curatorFramework.close();
            }

            /* Shut down message executor.
             */
            if (!MoreExecutors.shutdownAndAwaitTermination(_messageExecutor, 2, TimeUnit.SECONDS)) {
                LOGGER.warn("Failed to flush message queue during shutdown.");
            }

            /* Stop executing deferred tasks.
             */
            if (_timeoutTask != null) {
                _timeoutTask.cancel(false);
                try {
                    Uninterruptibles.getUninterruptibly(_timeoutTask);
                } catch (CancellationException | ExecutionException ignore) {
                }
            }

            /* Shut down cell.
             */
            try {
                _cell.prepareRemoval(event);
            } catch (Throwable e) {
                Thread t = Thread.currentThread();
                t.getUncaughtExceptionHandler().uncaughtException(t, e);
            }

            /* Shut down remaining threads.
             */
            LOGGER.debug("Waiting for all threads in {} to finish", _threads);
            try {
                Collection<Thread> threads = getNonDaemonThreads(_threads);

                /* Some threads shut down asynchronously. Give them
                 * one second before we start to kill them.
                 */
                while (!joinThreads(threads, 1000)) {
                    killThreads(threads);
                }
                _threads.destroy();
            } catch (IllegalThreadStateException e) {
                _threads.setDaemon(true);
            } catch (InterruptedException e) {
                LOGGER.warn("Interrupted while waiting for threads");
            }

            /* Declare the cell as dead.
             */
            __cellGlue.destroy(CellNucleus.this);
            setState(State.TERMINATED);
        }
    }

    ////////////////////////////////////////////////////////////
    //
    //
    // the routing stuff
    //

    /**
     * Installs a new route in the routing table.
     *
     * @param route The route to add
     * @throws IllegalArgumentException If the route is a duplicate or if it routes through
     *                                  a non-existing local cell.
     */
    public void routeAdd(CellRoute route) throws IllegalArgumentException
    {
        __cellGlue.routeAdd(route);
    }

    public void routeDelete(CellRoute  route) throws IllegalArgumentException {
        __cellGlue.routeDelete(route);
    }
    CellRoute routeFind(CellAddressCore addr) {
        return __cellGlue.getRoutingTable().find(addr, true);
    }
    public CellRoutingTable getRoutingTable() { return __cellGlue.getRoutingTable(); }
    public CellRoute [] getRoutingList() { return __cellGlue.getRoutingList(); }
    //
    public List<CellTunnelInfo> getCellTunnelInfos() { return __cellGlue.getCellTunnelInfos(); }

    public CuratorFramework getCuratorFramework()
    {
        return _curatorFramework;
    }

    //

    private class CallbackTask implements Runnable
    {
        private final CellLock _lock;
        private final CellMessage _message;

        public CallbackTask(CellLock lock, CellMessage message)
        {
            _lock = lock;
            _message = message;
        }

        @Override
        public void run()
        {
            _eventQueueSize.decrementAndGet();
            try (CDC ignored = _lock.getCdc().restore()) {
                try {
                    CellMessageAnswerable callback = _lock.getCallback();
                    CellMessage request = _lock.getMessage();
                    try {
                        Object obj = _message.getMessageObject();
                        if (obj instanceof Exception) {
                            callback.exceptionArrived(request, (Exception) obj);
                        } else {
                            callback.answerArrived(request, _message);
                        }
                        EventLogger.sendEnd(request);
                    } catch (RejectedExecutionException e) {
                        /* May happen when the callback itself tries to schedule the call
                         * on an executor. Put the request back and let it time out.
                         */
                        synchronized (_waitHash) {
                            _waitHash.put(request.getUOID(), _lock);
                        }
                        LOGGER.error("Failed to invoke callback: {}", e.toString());
                    }
                    LOGGER.trace("addToEventQueue : callback done for : {}", _message);
                } catch (Throwable e) {
                    Thread t = Thread.currentThread();
                    t.getUncaughtExceptionHandler().uncaughtException(t, e);
                }
            }
        }

        @Override
        public String toString()
        {
            return "Delivery-of-" + _message;
        }
    }

    private class DeliverMessageTask implements Runnable
    {
        private final MessageEvent _event;

        public DeliverMessageTask(MessageEvent event)
        {
            _event = event;
        }

        @Override
        public void run()
        {
            try (CDC ignored = CDC.reset(CellNucleus.this)) {
                try {
                    EventLogger.queueEnd(_event);
                    _lastQueueTime = _event.getMessage().getLocalAge();
                    _eventQueueSize.decrementAndGet();

                    if (_event instanceof RoutedMessageEvent) {
                        _cell.messageArrived(_event);
                    } else {
                        CDC.setMessageContext(_event.getMessage());
                        try {
                            _cell.messageArrived(_event);
                        } catch (RuntimeException e) {
                            CellMessage msg = _event.getMessage();
                            if (!msg.isReply()) {
                                msg.revertDirection();
                                msg.setMessageObject(e);
                                sendMessage(msg, true, true);
                            }
                            throw e;
                        }
                    }
                } catch (Throwable e) {
                    Thread t = Thread.currentThread();
                    t.getUncaughtExceptionHandler().uncaughtException(t, e);
                }
            }
        }

        @Override
        public String toString()
        {
            return "Delivery-of-" + _event;
        }
    }
}
