package dmg.cells.nucleus;

import java.io.Serializable;

public class CellDomainInfo implements Serializable
{
    private static final long serialVersionUID = 486982068268709272L;
    private final String _domainName;
    private final String _version;

    public CellDomainInfo(String name, String version)
    {
        _domainName = name;
        _version = version;
    }

    public String getVersion()
    {
        return _version;
    }

    public String getCellDomainName()
    {
        return _domainName;
    }

    public String toString()
    {
        return _domainName;
    }
}
