package org.dcache.auth.dpm;

import java.security.Principal;

import org.dcache.auth.attributes.LoginAttribute;
import org.dcache.auth.dpm.model.AliasClass;
import org.dcache.auth.dpm.model.AttributeClass;
import org.dcache.auth.dpm.model.Group;
import org.dcache.auth.dpm.model.GroupAlias;
import org.dcache.auth.dpm.model.User;
import org.dcache.auth.dpm.model.UserAlias;

/**
 * Data access object needed by the DpmPlugin.
 */
public interface AuthorizationDao
{
    User createUser();
    Group createGroup();
    AttributeClass createAttributeClass(Class<? extends LoginAttribute> c);
    AliasClass createAliasClass(Class<? extends Principal> c);

    AliasClass getOrCreateAliasClass(Class<? extends Principal> principal);
    UserAlias getUserAlias(Principal principal);
    GroupAlias getGroupAlias(Principal principal);
    User getUser(long uid);
    Group getGroup(long gid);
}
