package org.dcache.pool.hsm;

import java.net.URI;

/**
 * A request to remove a file from nearline storage.
 */
public interface RemoveRequest extends NearlineRequest<Void>
{
    URI getUri();
}
