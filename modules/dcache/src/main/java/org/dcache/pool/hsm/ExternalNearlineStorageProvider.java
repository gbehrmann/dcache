package org.dcache.pool.hsm;

public class ExternalNearlineStorageProvider implements NearlineStorageProvider
{
    @Override
    public String getName()
    {
        return "external";
    }

    @Override
    public String getDescription()
    {
        return "Nearline storage implemented through callouts to an external program";
    }

    @Override
    public NearlineStorage createNearlineStorage(String type, String name)
    {
        return new ExternalNearlineStorage(type, name);
    }
}
