package org.dcache.auth.dpm.model;

import java.util.Collection;
import java.util.Iterator;

/**
 * Abstract base class for User and Group. To simplify the ORM this
 * class doesn't actually contain any fields. (REVISIT)
 */
public abstract class Entity<T extends Alias>
{
    public abstract Authorization getAuthorization();

    public abstract void setAuthorization(Authorization authorization);

    public abstract Collection<T> getAliases();

    public abstract T addAlias(AliasClass aliasClass, String name);

    public abstract Collection<Attribute> getAttributes();

    public Attribute addAttribute(AttributeClass attributeClass, String value)
    {
        Attribute attribute = new Attribute(attributeClass, value);
        getAttributes().add(attribute);
        return attribute;
    }

    public void removeAliases(AliasClass aliasClass)
    {
        Iterator<T> i = getAliases().iterator();
        while (i.hasNext()) {
            if (i.next().getAliasClass().equals(aliasClass)) {
                i.remove();
            }
        }
    }

    public void removeAttributes(AttributeClass attributeClass)
    {
        Iterator<Attribute> i = getAttributes().iterator();
        while (i.hasNext()) {
            if (i.next().getAttributeClass().equals(attributeClass)) {
                i.remove();
            }
        }
    }
}
