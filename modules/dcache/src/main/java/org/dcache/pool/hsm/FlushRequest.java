package org.dcache.pool.hsm;

import java.io.File;
import java.net.URI;
import java.util.Set;

import org.dcache.vehicles.FileAttributes;

/**
 * A request to flush a file to nearline storage.
 *
 * The result of a flush request is one or more URIs that identify
 * the flushed file.
 */
public interface FlushRequest extends NearlineRequest<Set<URI>>
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
}
