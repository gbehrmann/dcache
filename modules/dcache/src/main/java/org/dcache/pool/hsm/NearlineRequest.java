package org.dcache.pool.hsm;

import com.google.common.util.concurrent.ListenableFuture;

import java.util.UUID;

/**
 * A request to nearline storage.
 *
 * A request has a lifetime containing three stages: queued, activated and
 * completed/failed. Activation is signalled by calling activated, while
 * completion/failure is signaled by calling completed/failed.
 */
public interface NearlineRequest<T>
{
    /**
     * Returns an identifier uniquely identifying this request.
     *
     * @return A unique identifier
     */
    UUID getId();

    /**
     * A deadline for the request.
     *
     * If the request does not complete before the deadline, the pool will
     * likely cancel the request. The deadline is not a promise that the
     * request will be cancelled, nor is it a promise that the request will
     * not be cancelled ahead of time.
     *
     * @return Deadline in milliseconds since the epoch
     */
    long getDeadline();

    /**
     * Signals that the request is being activated.
     *
     * An activated request is actively being processed rather than just being
     * queued. Note however than an external HSM may itself queue requests. Such
     * requests are still considered active by dCache.
     *
     * The activation may not be instantaneous as dCache may perform additional
     * name space lookups during activation. For this reason the result is
     * provided asynchronously. A NearlineStorage should not proceed with
     * processing the request until activation has completed.
     *
     * @return An asynchronous reply indicating when to proceed with processing
     *         the request. The activation may fail and a NearlineStorage must
     *         fail the entire request by calling {@code failed} with the exception
     *         returned by the future.
     */
    ListenableFuture<Void> activate();

    /**
     * Signals that the request has failed.
     *
     * @param cause Exception indicating the cause of the failure
     */
    void failed(Throwable cause);

    /**
     * Signals that the request has completed successfully.
     *
     * @param result The result of the request
     */
    void completed(T result);
}
