package org.dcache.auth.dpm.model;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.io.Serializable;
import java.security.Principal;

import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;
import javax.jdo.annotations.PrimaryKey;

/**
 * An Alias corresponce to an instance of a principal.
 */
@PersistenceCapable(objectIdClass=Alias.PK.class)
public class Alias
{
    /**
     * The AliasClass corresponding to the class of the Principal.
     */
    @PrimaryKey @Persistent(defaultFetchGroup="true")
    protected AliasClass _aliasClass;

    /**
     * The name of the principal. That is the value returned by the
     * getName method of a Principal.
     */
    @PrimaryKey
    protected String _name;

    public Alias(AliasClass aliasClass, String name)
    {
        _aliasClass = aliasClass;
        _name = name;
    }

    public String getName()
    {
        return _name;
    }

    public Principal createPrincipal()
        throws ClassNotFoundException,
               NoSuchMethodException,
               InstantiationException,
               IllegalAccessException,
               InvocationTargetException
    {
        Class<?> c = Class.forName(_aliasClass.getClassName());
        Constructor<?> constructor = c.getConstructor(String.class);
        return (Principal) constructor.newInstance(_name);
    }

    public AliasClass getAliasClass()
    {
        return _aliasClass;
    }

    public int getRank()
    {
        return _aliasClass.getRank();
    }

    /**
     * Inner class representing Primary Key
     */
    public static class PK implements Serializable
    {
        public AliasClass.PK _aliasClass;
        public String _name;

        public PK()
        {
        }

        public PK(String s)
        {
            String[] tokens = s.split("::");
            _aliasClass = new AliasClass.PK(tokens[0]);
            _name = tokens[1];
        }

        public PK(AliasClass aliasClass, String name)
        {
            _aliasClass = new AliasClass.PK(aliasClass.getClassName());
            _name = name;
        }

        public PK(Principal principal)
        {
            _aliasClass = new AliasClass.PK(principal.getClass());
            _name = principal.getName();
        }

        @Override
        public String toString()
        {
            return _aliasClass.toString() + "::" + _name;
        }

        @Override
        public int hashCode()
        {
            return _aliasClass.hashCode() ^ _name.hashCode();
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
            return _aliasClass.equals(other._aliasClass) &&
                _name.equals(other._name);
        }
    }
}


