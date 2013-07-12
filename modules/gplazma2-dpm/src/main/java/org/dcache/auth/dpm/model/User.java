package org.dcache.auth.dpm.model;

import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;
import javax.jdo.annotations.PrimaryKey;

import java.util.Collection;
import java.util.Set;
import java.util.HashSet;
import java.util.Iterator;

/**
 * A user Entity.
 */
@PersistenceCapable
public class User extends Entity<UserAlias>
{
    /**
     * The unique uid.
     */
    @PrimaryKey
    private long _uid;

    /**
     * The group of this user. Will be used as a fallback for the
     * primary group if no other group can be associated with a login.
     */
    @Persistent(defaultFetchGroup="true")
    private Group _group;

    /**
     * Other groups this user belongs to.
     */
    @Persistent(defaultFetchGroup="true")
    private Set<Group> _groups;

    /**
     * The authorization status.
     */
    @Persistent
    private Authorization _authorization;

    /**
     * The aliases that map to this user.
     */
    @Persistent(mappedBy="_user", dependentElement="true", defaultFetchGroup="true")
    private Set<UserAlias> _aliases;

    /**
     * The (login) attributes of this group.
     */
    @Persistent(dependentElement="true", defaultFetchGroup="true")
    private Set<Attribute> _attributes;

    public User()
    {
        _aliases = new HashSet<UserAlias>();
        _group = new Group();
        _groups = new HashSet<Group>();
        _authorization = Authorization.UNKNOWN;
        _attributes = new HashSet<Attribute>();
    }

    public User(long uid)
    {
        this();
        _uid = uid;
    }

    public long getUid()
    {
        return _uid;
    }

    public Group getGroup()
    {
        return _group;
    }

    public void setGroup(Group group)
    {
        _group = group;
    }

    public Collection<Group> getGroups()
    {
        return _groups;
    }

    public void  addGroup(Group group)
    {
        _groups.add(group);
    }

    public void removeGroup(long gid)
    {
        Iterator<Group> i = _groups.iterator();
        while (i.hasNext()) {
            if (i.next().getGid() == gid) {
                i.remove();
            }
        }
    }

    @Override
    public void setAuthorization(Authorization authorization)
    {
        _authorization = authorization;
    }

    @Override
    public Authorization getAuthorization()
    {
        return _authorization;
    }

    @Override
    public Collection<UserAlias> getAliases()
    {
        return _aliases;
    }

    @Override
    public UserAlias addAlias(AliasClass aliasClass, String name)
    {
        if (aliasClass.getCategory() != AliasCategory.USER) {
            throw new IllegalArgumentException("Not a group alias: " + aliasClass.getDisplayName());
        }
        UserAlias alias = new UserAlias(aliasClass, name);
        alias.setUser(this);
        _aliases.add(alias);
        return alias;
    }

    @Override
    public Collection<Attribute> getAttributes()
    {
        return _attributes;
    }
}
