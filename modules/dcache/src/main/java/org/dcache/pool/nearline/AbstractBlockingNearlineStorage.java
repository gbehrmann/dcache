/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2014 Deutsches Elektronen-Synchrotron
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.dcache.pool.nearline;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import diskCacheV111.util.InProgressCacheException;

import org.dcache.commons.util.NDC;
import org.dcache.pool.nearline.spi.FlushRequest;
import org.dcache.pool.nearline.spi.NearlineRequest;
import org.dcache.pool.nearline.spi.NearlineStorage;
import org.dcache.pool.nearline.spi.RemoveRequest;
import org.dcache.pool.nearline.spi.StageRequest;
import org.dcache.util.Checksum;
import org.dcache.vehicles.FileAttributes;

import static com.google.common.base.Preconditions.checkState;

/**
 * Base class for NearlineStorage implementations that follow the one-thread-per-task
 * paradigm.
 *
 * Implements request activation, request termination callbacks, request cancellation,
 * and nearline storage shutdown logic. Request cancellation is implemented by interrupting
 * the thread executing the cancelled task.
 *
 * Subclasses must implement the abstract methods for flush, stage and remove, as well as
 * the three methods for providing an executor for each of the there operations.
 */
public abstract class AbstractBlockingNearlineStorage implements NearlineStorage
{
    protected final String type;
    protected final String name;

    private final Map<UUID, Task<?, ?>> requests = new ConcurrentHashMap<>();

    public AbstractBlockingNearlineStorage(String type, String name)
    {
        this.type = type;
        this.name = name;
    }

    @Override
    public void cancel(UUID uuid)
    {
        Task<?, ?> task = requests.get(uuid);
        if (task != null) {
            task.cancel();
        }
    }

    @Override
    public void flush(Iterable<FlushRequest> requests)
    {
        for (FlushRequest request : requests) {
            getFlushExecutor().execute(new Task<FlushRequest, Set<URI>>(request)
            {
                @Override
                public Set<URI> call() throws Exception
                {
                    FileAttributes fileAttributes = request.getFileAttributes();
                    NDC.push(fileAttributes.getPnfsId().toString());
                    try {
                        return flush(request);
                    } finally {
                        NDC.pop();
                    }
                }

                @Override
                protected void execute()
                {
                    getFlushExecutor().execute(this);
                }
            });
        }
    }

    @Override
    public void stage(Iterable<StageRequest> requests)
    {
        for (StageRequest request : requests) {
            getStageExecutor().execute(new Task<StageRequest, Set<Checksum>>(request)
            {
                @Override
                public Set<Checksum> call() throws Exception
                {
                    FileAttributes attributes = request.getFileAttributes();
                    NDC.push(attributes.getPnfsId().toString());
                    try {
                        request.allocate().get();
                        return stage(request);
                    } finally {
                        NDC.pop();
                    }
                }

                @Override
                protected void execute()
                {
                    getStageExecutor().execute(this);
                }
            });
        }
    }

    @Override
    public void remove(Iterable<RemoveRequest> requests)
    {
        for (RemoveRequest request : requests) {
            getRemoveExecutor().execute(new Task<RemoveRequest, Void>(request)
            {
                @Override
                public Void call() throws Exception
                {
                    NDC.push(request.getUri().toString());
                    try {
                        remove(request);
                    } finally {
                        NDC.pop();
                    }
                    return null;
                }

                @Override
                protected void execute()
                {
                    getRemoveExecutor().execute(this);
                }
            });
        }
    }

    @Override
    public void shutdown()
    {
        requests.values().forEach(Task::cancel);
    }

    /**
     * Returns the nearline storage locations of a file for this nearline storage.
     *
     * @param fileAttributes Attributes of a file
     * @return The storage locations of the file on this nearline storage.
     */
    protected List<URI> getLocations(FileAttributes fileAttributes)
    {
        return filteredLocations(fileAttributes).collect(Collectors.toList());
    }

    /**
     * Returns the nearline storage locations of a file for this nearline storage.
     *
     * @param fileAttributes Attributes of a file
     * @return The storage locations of the file on this nearline storage.
     */
    private Stream<URI> filteredLocations(FileAttributes fileAttributes)
    {
        return fileAttributes.getStorageInfo().locations().stream()
                .filter(uri -> uri.getScheme().equals(type))
                .filter(uri -> uri.getAuthority().equals(name));
    }

    protected abstract Executor getFlushExecutor();
    protected abstract Executor getStageExecutor();
    protected abstract Executor getRemoveExecutor();

    protected abstract Set<URI> flush(FlushRequest request)
            throws Exception;
    protected abstract Set<Checksum> stage(StageRequest request)
            throws Exception;
    protected abstract void remove(RemoveRequest request)
            throws Exception;

    /**
     * Hook called when a request is retryed because InProgressCacheException was thrown. Subclasses
     * may choose to delay invocation of the runnable.
     */
    protected void retry(Runnable runnable)
    {
        runnable.run();
    }

    /**
     * Base class for tasks processing nearline requests.
     * @param <R> Request type
     * @param <T> Result type provided to the callback upon completion
     */
    protected abstract class Task<R extends NearlineRequest<T>, T> implements Runnable
    {
        protected final R request;
        private Thread thread;
        private boolean isDone;
        private boolean isActivated;

        protected Task(R request)
        {
            this.request = request;
            requests.put(request.getId(), this);
        }

        public synchronized void cancel()
        {
            if (!isDone) {
                isDone = true;
                if (thread != null) {
                    thread.interrupt();
                } else {
                    request.failed(new CancellationException());
                }
                requests.remove(request.getId());
            }
        }

        /**
         * Binds task to a particular thread. When the request is cancelled, the thread
         * is interrupted.
         */
        private synchronized boolean bind(Thread thread)
        {
            checkState(this.thread == null);
            if (isDone) {
                return false;
            }
            this.thread = thread;
            return true;
        }

        /**
         * Releases task from its thread. If the task was cancelled, InterruptedException
         * is thrown.
         */
        private synchronized void release(boolean isRetrying) throws InterruptedException
        {
            thread = null;
            if (isDone) {
                /* If done it must because the task was cancelled.
                 */
                throw new InterruptedException();
            }
            if (!isRetrying) {
                isDone = true;
                requests.remove(request.getId());
            }
        }

        /** Returns true the first time it is called, otherwise false. */
        private synchronized boolean activate()
        {
            if (!isActivated) {
                isActivated = true;
                return true;
            }
            return false;
        }

        public void run()
        {
            try {
                Thread thread = Thread.currentThread();
                if (bind(thread)) {
                    boolean isSuccess = false;
                    T result = null;
                    try {
                        try {
                            result = processRequest();
                            release(false);
                            isSuccess = true;
                        } catch (InProgressCacheException e) {
                            release(true);
                            throw e;
                        } catch (Exception e) {
                            release(false);
                            throw e;
                        }
                    } catch (InProgressCacheException e) {
                        retry(this::execute);
                    } catch (InterruptedException e) {
                        request.failed(new CancellationException());
                    } catch (Exception cause) {
                        request.failed(cause);
                    }
                    if (isSuccess) {
                        request.completed(result);
                    }
                }
            } catch (Throwable e) {
                thread.getUncaughtExceptionHandler().uncaughtException(thread, e);
            }
        }

        private T processRequest() throws Throwable
        {
            if (activate()) {
                try {
                    request.activate().get();
                } catch (ExecutionException e) {
                    throw e.getCause();
                }
            }
            NDC.push(request.getId().toString());
            try {
                return call();
            } finally {
                NDC.pop();
            }
        }

        protected abstract void execute();

        protected abstract T call() throws Exception;
    }
}
