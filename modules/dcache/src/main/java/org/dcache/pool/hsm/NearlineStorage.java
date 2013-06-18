package org.dcache.pool.hsm;

import java.util.Map;
import java.util.UUID;

/**
 * Service provider interface for nearline storage.
 *
 * Files can be flushed to, staged from, and removed from nearline storage.
 *
 * The interface is designed for bulk operations. Whether a nearline storage
 * makes use of that or processes each request individually is an implementation
 * detail.
 *
 * Each request has a unique identifier that can be used to cancel the request.
 *
 * A file flushed to nearline storage is identified by an implementation
 * specific URI. This URI is used to stage or remove the file from nearline
 * storage.
 */
public interface NearlineStorage
{
    /**
     * Flush all files in {@code files} to nearline storage.
     */
    void flush(Iterable<FlushRequest> files);

    /**
     * Stage all files iun {@code files} from nearline storage.
     */
    void stage(Iterable<StageRequest> file);

    /**
     * Delete all files in {@code files} from nearline storage.
     */
    void remove(Iterable<RemoveRequest> file);

    /**
     * Cancel any flush, stage or remove request with the given id.
     *
     * The failed method of any cancelled request should be called with a
     * CancellationException. If the request completes before it can be
     * cancelled, then the completed or failed method should be called as
     * appropriate.
     *
     * @param uuid  Id of the request to cancel
     */
    void cancel(UUID uuid);

    /**
     * Applies a new configuration.
     *
     * @throws IllegalArgumentException If the configuration is invalid
     */
    void configure(Map<String, String> properties)
            throws IllegalArgumentException;

    /**
     * Cancels all requests and initiates a shutdown of the nearline storage
     * interface.
     *
     * This method does not wait for actively executing requests to
     * terminate.
     */
    void shutdown();
}

