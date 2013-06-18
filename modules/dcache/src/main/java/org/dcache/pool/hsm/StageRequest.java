package org.dcache.pool.hsm;

import com.google.common.util.concurrent.ListenableFuture;

import java.io.File;
import java.util.Set;

import org.dcache.util.Checksum;
import org.dcache.vehicles.FileAttributes;

/**
 * A request to flush a file to nearline storage.
 *
 * The result of a stage request are zero or more checksums of the
 * file. Some implementations may be able to extract such a checksum
 * from an external storage system.
 */
public interface StageRequest extends NearlineRequest<Set<Checksum>>
{
    /**
     * A file system path to the replica in the pool.
     *
     * @return A file system path
     */
    File getFile();

    /**
     * Attributes of the file to which this request applies.
     *
     * @return Attributes of the file
     */
    FileAttributes getFileAttributes();

    /**
     * Triggers space allocation for the file being requested.
     *
     * Before completing a stage request, space must be allocated for the file.
     * A NearlineStorage must take care to not use any of the pool's disk space
     * before space has been allocated to it.
     *
     * Some NearlineStorage implementations may have a dedicated buffer area
     * separate from the pool allocation. Such implementations are able to
     * stage the file first and ask the pool for space afterwards. This allows
     * the implementation to reorder stage requests to optimize tape access
     * patterns while allowing more stage requests to be submitted than the
     * pool has free space (presumably the files would be streamed of the pool
     * as they become available, thus eventually allowing all files to be
     * staged).
     *
     * Space allocation may not be instantaneous as dCache may have to delete
     * other files to free up space. For this reason the result is provided
     * asynchronously. A NearlineStorage should not proceed with processing
     * the request until allocation has completed.
     *
     * @return An asynchronous reply indicating when to proceed with processing
     *         the request. The allocation may fail and a NearlineStorage must
     *         fail the entire request by calling failed with the exception
     *         returned by the future.
     */
    ListenableFuture<Void> allocate();
}
