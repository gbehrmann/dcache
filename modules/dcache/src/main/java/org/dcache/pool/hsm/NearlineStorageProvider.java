package org.dcache.pool.hsm;

/**
 * Factory of NearlineStorage.
 */
public interface NearlineStorageProvider
{
    String getName();
    String getDescription();
    NearlineStorage createNearlineStorage(String type, String name);
}
