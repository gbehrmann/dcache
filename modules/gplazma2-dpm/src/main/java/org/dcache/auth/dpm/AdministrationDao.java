package org.dcache.auth.dpm;

import java.util.Collection;

import org.dcache.auth.dpm.model.Alias;
import org.dcache.auth.dpm.model.AliasClass;
import org.dcache.auth.dpm.model.AttributeClass;
import org.dcache.auth.dpm.model.Group;
import org.dcache.auth.dpm.model.User;

/**
 * Data access object needed by the DpmAdministrator.
 */
public interface AdministrationDao extends AuthorizationDao
{
    Alias getAlias(String shortName, String name);
    Collection<Alias> getAliases(String name);
    AliasClass getAliasClass(String name);
    AttributeClass getAttributeClass(String name);

    Iterable<User> getUsers();
    Iterable<Group> getGroups();
    Iterable<AliasClass> getAliasClasses();
    Iterable<AttributeClass> getAttributeClasses();

    void delete(AliasClass aliasClass);
    void delete(AttributeClass attributeClass);
    void delete(User user);
    void delete(Group group);
}
