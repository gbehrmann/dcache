package org.dcache.auth.dpm.model;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.security.Principal;

import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;

/**
 * An Alias that maps to a Group.
 */
@PersistenceCapable
public class GroupAlias extends Alias
{
    @Persistent(defaultFetchGroup="true")
    private Group _group;

    public GroupAlias(AliasClass aliasClass, String name)
    {
        super(aliasClass, name);
        if (aliasClass.getCategory() != AliasCategory.GROUP) {
            throw new IllegalArgumentException("Invalid class for user alias");
        }
    }

    public Group getGroup()
    {
        return _group;
    }

    public void setGroup(Group group)
    {
        _group = group;
    }

    public Principal createPrincipal(boolean isPrimary)
        throws ClassNotFoundException,
               NoSuchMethodException,
               InstantiationException,
               IllegalAccessException,
               InvocationTargetException
    {
        try {
            Class<?> c = Class.forName(_aliasClass.getClassName());
            Constructor<?> constructor =
                c.getConstructor(String.class, Boolean.TYPE);
            return (Principal) constructor.newInstance(getName(), isPrimary);
        } catch (NoSuchMethodException e) {
            return createPrincipal();
        }
    }
}
