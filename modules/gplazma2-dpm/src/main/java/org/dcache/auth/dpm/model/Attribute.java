package org.dcache.auth.dpm.model;

import javax.jdo.annotations.PersistenceCapable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import org.dcache.auth.attributes.LoginAttribute;
import javax.jdo.annotations.Persistent;

/**
 * An Attribute is the representation of a LoginAttribute instance.
 */
@PersistenceCapable
public class Attribute
{
    /**
     * The AttributeClass that describes the class of the
     * LoginAttribute.
     */
    @Persistent
    private AttributeClass _attributeClass;

    /**
     * The value of the LoginAttribute. The LoginAttribute must have a
     * constructor that takes this value as the only argument.
     */
    @Persistent
    private String _value;

    public Attribute(AttributeClass attributeClass, String value)
    {
        if (attributeClass == null) {
            throw new NullPointerException();
        }
        _attributeClass = attributeClass;
        _value = value;
    }

    public AttributeClass getAttributeClass()
    {
        return _attributeClass;
    }

    public String getValue()
    {
        return _value;
    }

    public boolean getAppliesToSecondaryGroups()
    {
        return _attributeClass.getAppliesToSecondaryGroups();
    }

    public boolean getAllowMultiple()
    {
        return _attributeClass.getAllowMultiple();
    }

    public LoginAttribute createLoginAttribute()
        throws ClassNotFoundException,
               NoSuchMethodException,
               InstantiationException,
               IllegalAccessException,
               InvocationTargetException
    {
        Class<?> c = Class.forName(_attributeClass.getClassName());
        Constructor<?> constructor = c.getConstructor(String.class);
        return (LoginAttribute) constructor.newInstance(_value);
    }
}
