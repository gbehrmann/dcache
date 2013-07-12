package org.dcache.auth.dpm.model;

import java.util.Collection;
import java.util.Set;
import java.util.HashSet;

import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;
import javax.jdo.annotations.PrimaryKey;

/**
 * A group Entity.
 */
@PersistenceCapable
public class Group extends Entity<GroupAlias>
{
    /**
     * The unique gid of the group.
     */
    @PrimaryKey
    private long _gid;

    /**
     * The authorization status.
     */
    @Persistent
    private Authorization _authorization;

    /**
     * The aliases that map to this group.
     */
    @Persistent(mappedBy="_group", dependentElement="true", defaultFetchGroup="true")
    private Set<GroupAlias> _aliases;

    /**
     * The (login) attributes of this group.
     */
    @Persistent(dependentElement="true", defaultFetchGroup="true")
    private Set<Attribute> _attributes;

    public Group()
    {
        _authorization = Authorization.UNKNOWN;
        _aliases = new HashSet<GroupAlias>();
        _attributes = new HashSet<Attribute>();
    }

    public Group(long gid)
    {
        this();
        _gid = gid;
    }

    public long getGid()
    {
        return _gid;
    }

    @Override
    public Authorization getAuthorization()
    {
        return _authorization;
    }

    @Override
    public void setAuthorization(Authorization authorization)
    {
        _authorization = authorization;
    }

    @Override
    public Collection<GroupAlias> getAliases()
    {
        return _aliases;
    }

    @Override
    public GroupAlias addAlias(AliasClass aliasClass, String name)
    {
        if (aliasClass.getCategory() != AliasCategory.GROUP) {
            throw new IllegalArgumentException("Not a group alias: " + aliasClass.getDisplayName());
        }
        GroupAlias alias = new GroupAlias(aliasClass, name);
        alias.setGroup(this);
        _aliases.add(alias);
        return alias;
    }

    @Override
    public Collection<Attribute> getAttributes()
    {
        return _attributes;
    }
}
