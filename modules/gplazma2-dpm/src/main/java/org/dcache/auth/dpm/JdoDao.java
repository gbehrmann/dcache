package org.dcache.auth.dpm;

import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;
import javax.jdo.Query;
import javax.jdo.JDOObjectNotFoundException;
import javax.jdo.identity.LongIdentity;
import java.security.Principal;
import java.util.Collection;

import org.dcache.auth.attributes.LoginAttribute;
import org.dcache.auth.dpm.model.Alias;
import org.dcache.auth.dpm.model.AliasClass;
import org.dcache.auth.dpm.model.AttributeClass;
import org.dcache.auth.dpm.model.User;
import org.dcache.auth.dpm.model.Group;
import org.dcache.auth.dpm.model.UserAlias;
import org.dcache.auth.dpm.model.GroupAlias;

import org.springframework.stereotype.Repository;

import org.springframework.beans.factory.annotation.Required;

/**
 * JDO based implementation of the AuthorizationDao and
 * AdministrationDao interfaces.
 *
 * The implementation can only be used within active Spring managed
 * transactions. Objects returned by any method will only be valid
 * with the transaction in which they were returned.. All calls will
 * fail if used outside of a transaction.
 */
@Repository
public class JdoDao implements AuthorizationDao, AdministrationDao
{
    private PersistenceManagerFactory _persistenceManagerFactory;

    @Required
    public void setPersistenceManagerFactory(PersistenceManagerFactory pmf)
    {
        _persistenceManagerFactory = pmf;
    }

    @Override
    public AliasClass getOrCreateAliasClass(Class<? extends Principal> principal)
    {
        PersistenceManager pm =
            _persistenceManagerFactory.getPersistenceManager();
        try {
            return (AliasClass) pm.getObjectById(new AliasClass.PK(principal));
        } catch (JDOObjectNotFoundException e) {
            AliasClass ac = new AliasClass(principal);
            return pm.makePersistent(ac);
        }
    }

    @Override
    public UserAlias getUserAlias(Principal principal)
    {
        PersistenceManager pm =
            _persistenceManagerFactory.getPersistenceManager();
        try {
            pm.getFetchPlan().setMaxFetchDepth(-1);
            return pm.getObjectById(UserAlias.class, new Alias.PK(principal).toString());
        } catch (JDOObjectNotFoundException e) {
            return null;
        }
    }

    @Override
    public GroupAlias getGroupAlias(Principal principal)
    {
        PersistenceManager pm =
            _persistenceManagerFactory.getPersistenceManager();
        try {
            pm.getFetchPlan().setMaxFetchDepth(-1);
            return pm.getObjectById(GroupAlias.class, new Alias.PK(principal).toString());
        } catch (JDOObjectNotFoundException e) {
            return null;
        }
    }

    @Override
    public User getUser(long uid)
    {
        try {
            PersistenceManager pm =
                _persistenceManagerFactory.getPersistenceManager();
            return (User) pm.getObjectById(new LongIdentity(User.class, uid));
        } catch (JDOObjectNotFoundException e) {
            return null;
        }
    }

    @Override
    public Group getGroup(long gid)
    {
        try {
            PersistenceManager pm =
                _persistenceManagerFactory.getPersistenceManager();
            return (Group) pm.getObjectById(new LongIdentity(Group.class, gid));
        } catch (JDOObjectNotFoundException e) {
            return null;
        }
    }

    @Override
    public Collection<Alias> getAliases(String name)
    {
        PersistenceManager pm =
            _persistenceManagerFactory.getPersistenceManager();
        Query query = pm.newQuery(Alias.class, "_name == :name");
        return (Collection<Alias>) query.execute(name);
    }

    @Override
    public Alias getAlias(String shortName, String name)
    {
        PersistenceManager pm =
            _persistenceManagerFactory.getPersistenceManager();
        Query query =
            pm.newQuery(Alias.class,
                        "_aliasClass._shortName == :alias && _name == :name");
        query.setUnique(true);
        return (Alias) query.execute(shortName, name);
    }

    @Override
    public AliasClass getAliasClass(String name)
    {
        PersistenceManager pm =
            _persistenceManagerFactory.getPersistenceManager();
        Query q = pm.newQuery(AliasClass.class, "_className == :name || _shortName == :name");
        q.setUnique(true);
        return (AliasClass) q.execute(name);
    }

    @Override
    public AttributeClass getAttributeClass(String shortName)
    {
        PersistenceManager pm =
            _persistenceManagerFactory.getPersistenceManager();
        Query q = pm.newQuery(AttributeClass.class, "_shortName == :name");
        q.setUnique(true);
        return (AttributeClass) q.execute(shortName);
    }

    @Override
    public Iterable<User> getUsers()
    {
        PersistenceManager pm =
            _persistenceManagerFactory.getPersistenceManager();
        return pm.getExtent(User.class);
    }

    @Override
    public Iterable<Group> getGroups()
    {
        PersistenceManager pm =
            _persistenceManagerFactory.getPersistenceManager();
        return pm.getExtent(Group.class);
    }

    @Override
    public Iterable<AliasClass> getAliasClasses()
    {
        PersistenceManager pm =
            _persistenceManagerFactory.getPersistenceManager();
        return pm.getExtent(AliasClass.class);
    }

    @Override
    public Iterable<AttributeClass> getAttributeClasses()
    {
        PersistenceManager pm =
            _persistenceManagerFactory.getPersistenceManager();
        return pm.getExtent(AttributeClass.class);
    }

    private void delete(Object object)
    {
        PersistenceManager pm =
            _persistenceManagerFactory.getPersistenceManager();
        pm.deletePersistent(object);
    }

    @Override
    public void delete(AliasClass aliasClass)
    {
        delete((Object) aliasClass);
    }

    @Override
    public void delete(AttributeClass attributeClass)
    {
        delete((Object) attributeClass);
    }

    @Override
    public void delete(User user)
    {
        delete((Object) user);
    }

    @Override
    public void delete(Group group)
    {
        delete((Object) group);
    }

    private <T> T create(T object)
    {
       PersistenceManager pm =
            _persistenceManagerFactory.getPersistenceManager();
       return pm.makePersistent(object);
    }

    @Override
    public AliasClass createAliasClass(Class<? extends Principal> c)
    {
        return create(new AliasClass(c));
    }

    @Override
    public AttributeClass createAttributeClass(Class<? extends LoginAttribute> c)
    {
        return create(new AttributeClass(c));
    }

    @Override
    public User createUser()
    {
        return create(new User());
    }

    @Override
    public Group createGroup()
    {
        return create(new Group());
    }
}
