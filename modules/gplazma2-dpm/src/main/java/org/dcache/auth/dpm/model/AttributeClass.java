package org.dcache.auth.dpm.model;

import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;
import javax.jdo.annotations.PrimaryKey;

/**
 * The AttributeClass describes various aspects of an implementation
 * of the LoginAttribute interface.
 */
@PersistenceCapable
public class AttributeClass
{
    /**
     * Fully qualified class name of the principal.
     */
    @PrimaryKey
    private String _className;

    /**
     * Short name used in logs and the CLI.
     */
    @Persistent
    private String _shortName;

    /**
     * Displayable name used in the user interface.
     */
    @Persistent
    private String _displayName;

    /**
     * Whether this attribute will also be picked up from secondary
     * groups or only the primary group of a login.
     */
    @Persistent
    private boolean _appliesToSecondaryGroups;

    /**
     * Whether multiple values of an attribute of this AttributeClass
     * can be associated with a single login.
     */
    @Persistent
    private boolean _allowMultiple;

    protected AttributeClass()
    {
    }

    public AttributeClass(Class class_)
    {
        _className = class_.getName();
        _shortName = class_.getSimpleName();
        _displayName = class_.getSimpleName();
    }

    public AttributeClass(String className)
    {
        _className = className;
        _shortName = className;
        _displayName = className;
    }

    public void setShortName(String shortName)
    {
        _shortName = shortName;
    }

    public String getShortName()
    {
        return _shortName;
    }

    public void setDisplayName(String displayName)
    {
        _displayName = displayName;
    }

    public String getDisplayName()
    {
        return _displayName;
    }

    public String getClassName()
    {
        return _className;
    }

    public boolean getAppliesToSecondaryGroups()
    {
        return _appliesToSecondaryGroups;
    }

    public void setAppliesToSecondaryGroups(boolean value)
    {
        _appliesToSecondaryGroups = value;
    }

    public boolean getAllowMultiple()
    {
        return _allowMultiple;
    }

    public void setAllowMultiple(boolean value)
    {
        _allowMultiple = value;
    }
}
