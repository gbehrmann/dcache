package org.dcache.pool.hsm;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Base class for NearlineStorage implementations that follow the one-thread-per-task
 * paradigm.
 *
 * Implements request activation, request termination callbacks, request cancellation,
 * and nearline storage shutdown logic.
 */
public abstract class AbstractBlockingNearlineStorage implements NearlineStorage
{
    private final Map<UUID, Task<?, ?>> requests = new ConcurrentHashMap<>();

    @Override
    public void cancel(UUID uuid)
    {
        Task task = requests.get(uuid);
        if (task != null) {
            task.cancel();
        }
    }

    @Override
    public void shutdown()
    {
        for (Task<?, ?> task : requests.values()) {
            task.cancel();
        }
    }

    /**
     * Base class for tasks processing nearline requests.
     * @param <T> Result type provided to the callback upon completion
     * @param <R> Request type
     */
    protected abstract class Task<T, R extends NearlineRequest<T>> implements Runnable
    {
        protected final R request;
        private boolean isDone;
        private Thread thread;

        protected Task(R request)
        {
            this.request = request;
            requests.put(request.getId(), this);
        }

        public synchronized void cancel()
        {
            if (thread != null) {
                thread.interrupt();
            } else {
                failed(new CancellationException());
            }
        }

        /**
         * Binds task to a particular thread. When the request is cancelled, the thread
         * is interrupted.
         */
        private synchronized boolean bind(Thread thread)
        {
            this.thread = thread;
            return !isDone;
        }

        /** Marks successful termination of the task. */
        private synchronized void completed(T result)
        {
            requests.remove(request.getId());
            request.completed(result);
            isDone = true;
        }

        /** Marks failed termination of the task .*/
        private synchronized void failed(Throwable cause)
        {
            requests.remove(request.getId());
            request.failed(cause);
            isDone = true;
        }

        public void run()
        {
            Thread thread = Thread.currentThread();
            if (bind(thread)) {
                try {
                    request.activate().get();
                    completed(call());
                } catch (InterruptedException e) {
                    failed(new CancellationException());
                } catch (Exception cause) {
                    failed(cause);
                } catch (Throwable t) {
                    thread.getUncaughtExceptionHandler().uncaughtException(thread, t);
                }
            }
        }

        /** Implemented by subclasses. Processes the nearline request. */
        protected abstract T call() throws Exception;
    }
}
