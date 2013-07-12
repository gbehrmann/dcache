package org.dcache.auth.dpm.model;

import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;
import javax.jdo.annotations.PrimaryKey;
import java.io.Serializable;
import java.security.Principal;

/**
 * The AliasClass describes various aspects of an implementation of
 * the Principal interface.
 */
@PersistenceCapable(objectIdClass=AliasClass.PK.class)
public class AliasClass
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
     * Coarse categorization of how to use instances of this principal
     * class.
     */
    @Persistent
    private AliasCategory _category;

    /**
     * Default authorization for auto created entities.
     */
    @Persistent
    private Authorization _defaultAuthorization;

    /**
     * Whether to automatically create a user or group record.
     */
    @Persistent
    private boolean _autoCreate;

    /**
     * Whether to include instances of this principal in the
     * LoginReply.
     */
    @Persistent
    private boolean _embodied;

    /**
     * Discrimator used to determine the primary group.
     */
    @Persistent
    private int _rank;

    protected AliasClass()
    {
        _category = AliasCategory.UNCLASSIFIED;
        _defaultAuthorization = Authorization.UNKNOWN;
    }

    public AliasClass(Class<? extends Principal> class_)
    {
        this();
        _className = class_.getName();
        _shortName = class_.getSimpleName();
        _displayName = class_.getSimpleName();
    }

    public AliasClass(String name)
    {
        this();
        _className = name;
        _shortName = name;
        _displayName = name;
    }

    public String getClassName()
    {
        return _className;
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

    public AliasCategory getCategory()
    {
        return _category;
    }

    public void setCategory(AliasCategory category)
    {
        _category = category;
    }

    public void setAutoCreate(boolean autoCreate)
    {
        _autoCreate = autoCreate;
    }

    public Authorization getDefaultAuthorization()
    {
        return _defaultAuthorization;
    }

    public void setDefaultAuthorization(Authorization defaultAuthorization)
    {
        _defaultAuthorization = defaultAuthorization;
    }

    public boolean isAutoCreate()
    {
        return _autoCreate;
    }

    public void setEmbodied(boolean embodied)
    {
        _embodied = embodied;
    }

    public boolean isEmbodied()
    {
        return _embodied;
    }

    public int getRank()
    {
        return _rank;
    }

    public void setRank(int rank)
    {
        _rank = rank;
    }

    /**
     * Inner class representing Primary Key
     */
    public static class PK implements Serializable
    {
        public String _className;

        public PK()
        {
        }

        public PK(String s)
        {
            _className = s;
        }

        public PK(Class<? extends Principal> principal)
        {
            _className = principal.getName();
        }

        @Override
        public String toString()
        {
            return _className;
        }

        @Override
        public int hashCode()
        {
            return _className.hashCode();
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }
            if (obj == null || !(obj instanceof PK)) {
                return false;
            }

            PK other = (PK) obj;
            return _className.equals(other._className);
        }
    }
}
