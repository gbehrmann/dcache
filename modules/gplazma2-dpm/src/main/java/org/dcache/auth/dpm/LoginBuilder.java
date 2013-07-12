package org.dcache.auth.dpm;

import java.lang.reflect.InvocationTargetException;
import java.security.Principal;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.dcache.auth.GidPrincipal;
import org.dcache.auth.GroupNamePrincipal;
import org.dcache.auth.GroupPrincipal;
import org.dcache.auth.UidPrincipal;
import org.dcache.auth.attributes.LoginAttribute;
import org.dcache.auth.attributes.ReadOnly;
import org.dcache.auth.dpm.model.Alias;
import org.dcache.auth.dpm.model.AliasClass;
import org.dcache.auth.dpm.model.Attribute;
import org.dcache.auth.dpm.model.Authorization;
import org.dcache.auth.dpm.model.Group;
import org.dcache.auth.dpm.model.GroupAlias;
import org.dcache.auth.dpm.model.User;
import org.dcache.auth.dpm.model.UserAlias;
import org.dcache.gplazma.AuthenticationException;

/**
 * Builder for constructing principals and attributes for a login.
 *
 * The typical call order is:
 *
 *   - addUnmappedPrincipal, addUserPrincipal, addGroupPrincipal
 *   - setLoginUid, setLoginGid, setLoginName
 *
 * or
 *
 *   - setUser, addGroup
 *
 * followed by
 *
 *   - getAuthorization
 *   - toPrincipals, toAttributes
 *
 */
public class LoginBuilder
{
    /** Input principals mapped from their alias. */
    private final Map<Alias,Principal> _identifiedPrincipals =
        new HashMap<>();

    /** Input principals without an alias. */
    private final Map<Principal,AliasClass> _unmappedPrincipals =
        new HashMap<>();

    /** Groups of the subject. */
    private final Set<Group> _groups = new HashSet<>();

    /** The alias that identified the user. */
    private UserAlias _userAlias;

    /** User record of the Subject.  */
    private User _user;

    /** Group chosen by setting a login GID. */
    private Group _primaryGroup;

    /** Highest ranked group of principals marked as primary groups. */
    private GroupAlias _bestPrimary;

    /** Highest ranked group of principals not marked as primary groups. */
    private GroupAlias _bestSecondary;

    /**
     * An unmapped principal is one that doesn't map to any alias in
     * the database. It always got an AliasClass as we always create
     * those for all principals. Unmapped principals may be included
     * in the LoginReply if the AliasClass is configured to embody
     * such principals.
     */
    public void addUnmappedPrincipal(AliasClass class_, Principal principal)
    {
        _unmappedPrincipals.put(principal, class_);
    }

    /**
     * A user principal is one that maps to a UserAlias. One one such
     * principal is allowed. Duplicate calls will result in a
     * CacheException.
     */
    public void addUserPrincipal(Principal principal, UserAlias alias)
        throws AuthenticationException
    {
        if (alias.getUser() == null) {
            throw new NullPointerException();
        }
        if (_userAlias != null) {
            throw new AuthenticationException("Subject has multiple user identities");
        }
        _userAlias = alias;
        _user = alias.getUser();
        _groups.add(alias.getUser().getGroup());
        _groups.addAll(alias.getUser().getGroups());
        _identifiedPrincipals.put(alias, principal);
    }

    /**
     * A group principal is one that maps to a GroupAlias.
     *
     * If the principal implements GroupPrincipal, then we will take
     * the primary group flag of the principal into account when
     * finding the primary group of the login. Conflicts between
     * multiple groups are resolved using the rank of the AliasClass
     * of the group alias.
     */
    public void addGroupPrincipal(Principal principal, GroupAlias alias)
        throws AuthenticationException
    {
        if (alias.getGroup() == null) {
            throw new NullPointerException();
        }

        _groups.add(alias.getGroup());

        int rank = alias.getRank();
        if (_bestPrimary == null || _bestPrimary.getRank() < rank) {
            if (principal instanceof GroupPrincipal &&
                ((GroupPrincipal) principal).isPrimaryGroup()) {
                _bestPrimary = alias;
            }
        }

        if (_bestSecondary == null ||  _bestSecondary.getRank() < rank) {
            _bestSecondary = alias;
        }

        _identifiedPrincipals.put(alias, principal);
    }

    /**
     * Some protocols allow an explicit uid to be specified during
     * login. This will never be used to authorize access to that
     * uid. We will however fail with a CacheException if the uid
     * doesn't match the user record we have determined as part of the
     * mapping steps.
     *
     * Must be called after the user principal has been set.
     */
    public void setLoginUid(long uid)
        throws AuthenticationException
    {
        if (uid != getUid()) {
            throw new AuthenticationException("Subject is not authorized to use preselected UID");
        }
    }

    /**
     * Some protocols allow an explicit gid to be specified during
     * login. This will never be used to authorize access to that
     * group. The gid will however be used for identifying the primary
     * group to use.
     *
     * An exception is thrown if the login isn't already a member of
     * the given group.
     *
     * Must be called after the group principals have been set.
     */
    public void setLoginGid(long gid)
        throws AuthenticationException
    {
        Group group = getGroup(gid);
        if (group == null) {
            throw new AuthenticationException("Subject is not authorized to use preselected GID");
        }
        _primaryGroup = group;
    }

    /**
     * Some protocols allow an explicit unauthenticated login name to
     * be specified during login. The login name is not used for user
     * mapping which must happen through other principals. This login
     * name is however interpreted as a group name and influences the
     * selection of the primary group.
     *
     * An exception is thrown if the login isn't already a member of
     * the given group.
     *
     * Must be called after the group principals have been set.
     */
    public void setLoginName(String name)
        throws AuthenticationException
    {
        Group group =
            getGroupByClass(GroupNamePrincipal.class, name);
        if (group == null) {
            throw new AuthenticationException("Subject is not authorized to use preselected group");
        }
        if (_primaryGroup == null) {
            _primaryGroup = group;
        }
    }

    public void setUser(User user)
    {
        _user = user;
        _userAlias = null;
    }

    public void addGroup(Group group, boolean primary)
    {
        _groups.add(group);
        if (primary) {
            _primaryGroup = group;
        }
    }

    /**
     * Returns the UserAlias associated with the user
     * principal. Although the user record may have several aliases,
     * this specific alias is the one that was used during login.
     */
    public UserAlias getUserAlias()
    {
        return _userAlias;
    }

    /**
     * Returns the User record. If no user principal has been
     * specified yet,
     */
    public User getUser()
    {
        if (_user == null) {
            throw new IllegalStateException("User is missing");
        }
        return _user;
    }

    /**
     * Returns the Group with the given gid. Returns null if the login
     * doesn't have such a group.
     */
    public Group getGroup(long gid)
        throws AuthenticationException
    {
        for (Group group: _groups) {
            if (group.getGid() == gid) {
                return group;
            }
        }
        return null;
    }

    /**
     * Returns the Group with the given principal type and
     * name. Returns null if the login doesn't have such a group.
     */
    Group getGroupByClass(Class<? extends Principal> class_, String name)
        throws AuthenticationException
    {
        String className = class_.getName();
        for (Group group: _groups) {
            for (GroupAlias alias: group.getAliases()) {
                if (alias.getAliasClass().getClassName().equals(className) && alias.getName().equals(name)) {
                    return group;
                }
            }
        }
        return null;
    }

    /**
     * Returns the authorization status of the login. This is computed
     * by combining the authorization fields of the user record and
     * all group records of the login.
     */
    public Authorization getAuthorization()
    {
        Authorization smallest = getUser().getAuthorization();
        for (Group group: _groups) {
            Authorization authorization = group.getAuthorization();
            if (authorization.compareTo(smallest) < 0) {
                smallest = authorization;
            }
        }
        return smallest;
    }

    /**
     * Determines the primary group of the login.
     *
     * The logic is the following (in order):
     *
     * - If a login gid was specified, then it defines the primary
     *   group.
     *
     * - If a login name was specified, then it defines the primary
     *   group.
     *
     * - If a GroupPrincipal with the primary flag set is specified,
     *   then the highest ranked such principal determines the primary
     *   group.
     *
     * - If another group principal is given, then the highest ranked
     *   such principal determines the primary group.
     *
     * - Otherwise the user group associated with the user record is
     *   the primary group.
     */
    private Group getPrimaryGroup()
    {
        if (_primaryGroup != null) {
            return _primaryGroup;
        } else if (_bestPrimary != null) {
            return _bestPrimary.getGroup();
        } else if (_bestSecondary != null) {
            return _bestSecondary.getGroup();
        } else {
            return getUser().getGroup();
        }
    }

    /**
     * Returns the UID of the user.
     */
    public long getUid()
    {
        return getUser().getUid();
    }

    /**
     * Returns the GIDs of the user. The primary GID is returned
     * first.
     */
    public long[] getGids()
    {
        Group primaryGroup = getPrimaryGroup();
        long[] gids = new long[_groups.size()];
        gids[0] = primaryGroup.getGid();
        int i = 1;
        for (Group group: _groups) {
            if (!group.equals(primaryGroup)) {
                gids[i++] = group.getGid();
            }
        }
        return gids;
    }

    /**
     * Generates the set of principals to include in the LoginReply.
     *
     * This set includes a UidPrincipal and one or more GidPrincipals,
     * one of them being marked as the primary group.
     *
     * All embodied user and group aliases of the login are included
     * as principals.
     *
     * Finally, all embodied unmapped principals are included.
     *
     * If the login request already contained the principal, then the
     * original instance is included in the LoginReply. This ensures
     * that embedded parameters are preserved.
     */
    public Set<Principal> toPrincipals()
        throws ClassNotFoundException,
               NoSuchMethodException,
               InstantiationException,
               IllegalAccessException,
               InvocationTargetException
    {
        Set<Principal> principals = new HashSet<>();
        Group primaryGroup = getPrimaryGroup();

        for (Group group: _groups) {
            boolean isPrimary = group.equals(primaryGroup);
            for (GroupAlias alias: group.getAliases()) {
                if (alias.getAliasClass().isEmbodied()) {
                    Principal principal = _identifiedPrincipals.get(alias);
                    if (principal == null) {
                        principal = alias.createPrincipal(isPrimary);
                    }
                    principals.add(principal);
                }
            }
            principals.add(new GidPrincipal(group.getGid(), isPrimary));
        }

        for (UserAlias alias: getUser().getAliases()) {
            if (alias.getAliasClass().isEmbodied()) {
                Principal principal = _identifiedPrincipals.get(alias);
                if (principal == null) {
                    principal = alias.createPrincipal();
                }
                principals.add(principal);
            }
        }

        principals.add(new UidPrincipal(getUid()));

        for (Map.Entry<Principal,AliasClass> e: _unmappedPrincipals.entrySet()) {
            if (e.getValue().isEmbodied()) {
                principals.add(e.getKey());
            }
        }

        return principals;
    }

    /**
     * Adds attribute to a set of attributes.
     *
     * If an attribute of that type is already present, then it is
     * only added if the type allows multiple values.
     *
     * If the attribute is added, then both the attributes and classes
     * sets are updated.
     *
     * @param attributes The set to add the attribute to
     * @param classes The set of classes in attributes
     * @param attribute The attribute to add
     */
    private static
        void addAttribute(Set<LoginAttribute> attributes, Set<Class> classes,
                          Attribute attribute)
        throws ClassNotFoundException,
               NoSuchMethodException,
               InstantiationException,
               IllegalAccessException,
               InvocationTargetException
    {
        LoginAttribute loginAttribute = attribute.createLoginAttribute();
        Class c = loginAttribute.getClass();
        if (attribute.getAllowMultiple() || !classes.contains(c)) {
            attributes.add(loginAttribute);
            classes.add(c);
        }
    }

    /**
     * Generates the set of LoginAttributes to include in the
     * LoginReply.
     *
     * This is a combination of the attributes associated the user,
     * the groups of the login, and the extra attributes added to the
     * builder.
     *
     * Attributes are added in the following order: user, primary
     * group, other groups. The order is relevant for attributes that
     * may not have multiple values. In that case only the first
     * occurrence of an attribute class is included.
     */
    public Collection<LoginAttribute> toLoginAttributes()
        throws ClassNotFoundException,
               NoSuchMethodException,
               InstantiationException,
               IllegalAccessException,
               InvocationTargetException
    {
        Set<LoginAttribute> attributes = new HashSet<>();
        Set<Class> classes = new HashSet<>();

        if (getAuthorization() == Authorization.READONLY) {
            attributes.add(new ReadOnly(true));
            classes.add(ReadOnly.class);
        }

        for (Attribute attribute: getUser().getAttributes()) {
            addAttribute(attributes, classes, attribute);
        }

        for (Attribute attribute: getPrimaryGroup().getAttributes()) {
            addAttribute(attributes, classes, attribute);
        }

        for (Group group: _groups) {
            for (Attribute attribute: group.getAttributes()) {
                if (attribute.getAppliesToSecondaryGroups()) {
                    addAttribute(attributes, classes, attribute);
                }
            }
        }

        return attributes;
    }
}
