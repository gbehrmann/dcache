package org.dcache.chimera.nfsv41.door;

import diskCacheV111.util.PnfsId;

/**
 * An {@code AccessLogMode} controls amount of information logged by access
 * logger.
 *
 * @since 2.15
 */
public enum AccessLogMode {

    /**
     * Logging is disabled.
     */
    NONE,
    /**
     * Logging enabled, parent directories logged in a string form of {@link PnfsId}.
     */
    MINIMAL,
    /**
     * Logging enabled, parent directories as full path.
     */
    FULL
}
