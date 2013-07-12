package org.dcache.auth.dpm.model;

import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;

/**
 * An Alias that maps to a User.
 */
@PersistenceCapable
public class UserAlias extends Alias
{
    @Persistent(defaultFetchGroup="true")
    private User _user;

    public UserAlias(AliasClass aliasClass, String name)
    {
        super(aliasClass, name);
        if (aliasClass.getCategory() != AliasCategory.USER) {
            throw new IllegalArgumentException("Invalid class for user alias");
        }
    }

    public User getUser()
    {
        return _user;
    }

    public void setUser(User user)
    {
        _user = user;
    }
}
