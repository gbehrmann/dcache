/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2013 Deutsches Elektronen-Synchrotron
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
package org.dcache.util;

import com.google.common.io.Closer;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import java.io.Closeable;

import static com.google.common.base.Preconditions.checkState;

/**
 * A future that implements an asynchronous variant of the try-with-resource
 * construct of Java 7.
 *
 * The class follows the template pattern often seen in the Spring Framework. A
 * client is supposed to create a (possibly anonymous inner) subclass implementing
 * the {@code execute} method.
 *
 * {@code execute} may be synchronous or asynchronous. If synchronous, it can
 * signal completion by calling {@code set} or {@code setException}. If asynchronous,
 * it must call {@code setResult}. It is a failure to call by {@code set} and
 * {@code setResult}.
 *
 * {@code execute} may register Closeable resources by calling {@code autoclose}.
 * These are guaranteed to be closed once this {@code TryCatchFuture} completes.
 * Failure to close any resource is propagated as a failure of this future, unless the
 * future has already failed (in that case the failure to close a resource is added
 * as a suppressed throwable).
 *
 * Any exceptions thrown by {@code execute} are caught and will result in failure
 * of this future.
 *
 * An delegate registered through {@code setResult} will be cancelled if {@code execute}
 * throws an exception or calls {@code setException}, or if this future is cancelled.
 *
 * A subclass may override {@code onSuccess} and {@code onFailure} to add
 * additional processing when this future completes. These are only to be used
 * for light-weight non-blocking operations. The thread on which these are
 * called is unpredictable.
 * */
public abstract class TryCatchFuture<V>
        extends AbstractFuture<V>
{
    private final Closer _closer = Closer.create();

    private ListenableFuture<V> _delegate;

    public TryCatchFuture()
    {
        try {
            execute();
            if (_delegate != null) {
                Futures.addCallback(_delegate, new FutureCallback<V>()
                {
                    @Override
                    public void onSuccess(V result)
                    {
                        closeAndSucceed(result);
                    }

                    @Override
                    public void onFailure(Throwable t)
                    {
                        closeAndFail(t);
                    }
                });
            } else if (!isDone()) {
                set(null);
            }
        } catch (Throwable t) {
            setException(t);
        }
    }

    private boolean closeAndSucceed(V value)
    {
        try {
            _closer.close();
            onSuccess(value);
            return super.set(value);
        } catch (Throwable t) {
            return fail(t);
        }
    }

    private boolean closeAndFail(Throwable throwable)
    {
        /* This weird looking code is to fulfill the contract of Closer.
         * It ensures that suppressed exceptions from the Closeables are
         * handled correctly.
         */
        try {
            try {
                throw _closer.rethrow(throwable, Exception.class);
            } finally {
                _closer.close();
            }
        } catch (Throwable t) {
            return fail(t);
        }
    }

    private boolean fail(Throwable t)
    {
        try {
            onFailure(t);
        } catch (Exception replacement) {
            if (replacement.getCause() == t) {
                t = replacement;
            } else if (replacement != t) {
                t.addSuppressed(replacement);
            }
        } catch (Throwable suppressed) {
            t.addSuppressed(t);
        }
        return super.setException(t);
    }

    /**
     * Registers the given {@code closeable} to be closed when this
     * {@code TryCatchFuture} completed.
     *
     * @return the given {@code closeable}
     */
    protected <C extends Closeable> C autoclose(C closeable)
    {
        return _closer.register(closeable);
    }

    /**
     * Registers the given {@code delegate} as the result of this {@code TryCatchFuture}.
     * When {@code delegate} finishes so does this future, and when this future is
     * cancelled so is the {@code delegate}.
     *
     * @throws IllegalStateException if this future already has a result
     */
    protected void setResult(ListenableFuture<V> delegate)
    {
        checkState(_delegate == null && !isDone());
        _delegate = delegate;
    }

    /**
     * Called by the constructor to execute the template.
     *
     * Either {@code set}, {@code setException}, or {@code setResult} should be called to
     * provide a result of the operation. If neither is called, the future is completed with
     * a null value.
     *
     * A subclass is expected to override either {@code execute} or {@code executeWithResult}.
     */
    protected void execute() throws Exception
    {
        setResult(executeWithResult());
    }

    /**
     * Like {@code execute} but allows a future to be returned.
     *
     * An implementation should not call {@code set} or {@code setResult} from within
     * {@code executeWithResult}.
     */
    protected ListenableFuture<V> executeWithResult() throws Exception
    {
        return null;
    }

    /**
     * Invoked with the result of the execution when it is successful.
     *
     * If an {@code Exception} is thrown, the future fails. Otherwise the future
     * succeeds.
     *
     * Must not call {@code set} or {@code setException}.
     */
    protected void onSuccess(V result)
            throws Exception
    {
    }

    /**
     * Invoked when the execution fails or is canceled.
     *
     * If an {@code Exception} is thrown of which {@code t} is the cause, that exception
     * is used to fail this future. Thus an implementation may replace the reason the
     * future fails by throwing a new exception with {@code t} set as the cause.
     *
     * Otherwise the future fails with {@code t}. Any other exception thrown by this
     * method is suppressed.
     *
     * Must not call {@code set} or {@code setException}.
     */
    protected void onFailure(Throwable t)
            throws Exception
    {
    }

    @Override
    protected boolean set(V value)
    {
        checkState(_delegate == null);
        return closeAndSucceed(value);
    }

    @Override
    protected boolean setException(Throwable throwable)
    {
        if (_delegate != null) {
            _delegate.cancel(true);
        }
        return closeAndFail(throwable);
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning)
    {
        return (_delegate != null) && _delegate.cancel(mayInterruptIfRunning);
    }

    @Override
    public boolean isCancelled()
    {
        return (_delegate != null) && _delegate.isCancelled();
    }
}
