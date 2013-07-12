package org.dcache.auth.dpm;

import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import com.google.common.primitives.Longs;
import org.globus.gsi.jaas.GlobusPrincipal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.lang.reflect.InvocationTargetException;
import java.security.Principal;
import java.util.Collection;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.dcache.auth.GidPrincipal;
import org.dcache.auth.GroupNamePrincipal;
import org.dcache.auth.LoginGidPrincipal;
import org.dcache.auth.LoginNamePrincipal;
import org.dcache.auth.LoginUidPrincipal;
import org.dcache.auth.Origin;
import org.dcache.auth.UidPrincipal;
import org.dcache.auth.dpm.model.Alias;
import org.dcache.auth.dpm.model.AliasClass;
import org.dcache.auth.dpm.model.Group;
import org.dcache.auth.dpm.model.GroupAlias;
import org.dcache.auth.dpm.model.User;
import org.dcache.auth.dpm.model.UserAlias;
import org.dcache.gplazma.AuthenticationException;
import org.dcache.gplazma.NoSuchPrincipalException;
import org.dcache.gplazma.plugins.GPlazmaIdentityPlugin;
import org.dcache.gplazma.plugins.GPlazmaMappingPlugin;
import org.dcache.gplazma.plugins.GPlazmaSessionPlugin;
import org.dcache.gplazma.util.GridMapFile;

/**
 * Dynamic Principal Mapping plugin.
 *
 * This class cannot be used without providing a Spring managed transactional context.
 *
 * @see TransactionalDpmPlugin
 */
public class DpmPlugin
        implements GPlazmaMappingPlugin, GPlazmaSessionPlugin, GPlazmaIdentityPlugin
{
    static final Logger LOGGER =
            LoggerFactory.getLogger(TransactionalDpmPlugin.class);
    static final Logger AUTH_LOG =
            LoggerFactory.getLogger("org.dcache.billing.auth");

    private final JdoDao _dao;
    private GridMapFile _gridmap;

    public DpmPlugin(Properties properties, JdoDao dao)
    {
        String path = properties.getProperty("gplazma.dpm.gridmap");
        _gridmap = (path == null) ? null : new GridMapFile(path);
        _dao = dao;
    }

    private void setLoginPrincipals(LoginBuilder builder,
                            Set<Principal> principals)
            throws AuthenticationException
    {
        for (Principal principal : principals) {
            if (principal instanceof LoginUidPrincipal) {
                builder.setLoginUid(((LoginUidPrincipal) principal).getUid());
            } else if (principal instanceof LoginGidPrincipal) {
                builder.setLoginGid(((LoginGidPrincipal) principal).getGid());
            } else if (principal instanceof LoginNamePrincipal) {
                builder.setLoginName(principal.getName());
            }
        }
    }

    void logOrigin(Set<Principal> principals)
    {
        Origin origin = Iterables.getFirst(Iterables.filter(principals, Origin.class), null);
        if (origin != null) {
            MDC.put("login.origin", origin.getName());
        }
    }

    void logUser(LoginBuilder builder)
    {
        UserAlias alias = builder.getUserAlias();
        MDC.put("login.type", alias.getAliasClass().getShortName());
        MDC.put("login.user", alias.getName());
    }

    void logAllowed(LoginBuilder builder)
    {
        long uid = builder.getUid();
        long[] gids = builder.getGids();
        AUTH_LOG.info("accepted as {}:{}",
                uid, Joiner.on(',').join(Longs.asList(gids)));
    }

    void logClear()
    {
        MDC.remove("login.ip");
        MDC.remove("login.type");
        MDC.remove("login.user");
    }

    void logDenied(Exception e)
    {
        AUTH_LOG.warn("denied: {}", e.getMessage());
    }

    /**
     * Maps DNs of the LoginBuilder to GroupNamePrincipal.
     *
     * The reason we cannot use the regular gridmap plugin for gPlazma is that
     * we need to capture mappings for non X.509 logins too, eg. a user logging
     * in through username and password may have a DN associated with the user
     * account.
     *
     * The main use case for gridmap file is for non-voms-proxy based VOMS
     * integration, in which the VOMS DB is periodically dumped into a gridmap
     * file.
     */
    private void gridmap(LoginBuilder builder)
            throws AuthenticationException
    {
        if (_gridmap == null) {
            return;
        }

        _gridmap.refresh();

        for (UserAlias alias: builder.getUser().getAliases()) {
            if (alias.getAliasClass().getClassName().equals(GlobusPrincipal.class.getName())) {
                for (String name: _gridmap.getMappedUsernames(alias.getName())) {
                    Principal principal = new GroupNamePrincipal(name, false);
                    mapPrincipal(builder, principal);
                }
            }
        }
    }

    private void authorize(LoginBuilder builder)
            throws AuthenticationException
    {
        switch (builder.getAuthorization()) {
        case BANNED:
            throw new AuthenticationException("Subject is banned");
        case UNKNOWN:
            throw new AuthenticationException("Subject is not authorized");
        case READONLY:
        case ALLOWED:
            break;
        }
    }

    @Override
    public void map(Set<Principal> principals)
            throws AuthenticationException
    {
        try {
            logOrigin(principals);

            LoginBuilder builder = mapPrincipals(principals);
            gridmap(builder);
            logUser(builder);
            authorize(builder);
            setLoginPrincipals(builder, principals);
            principals.addAll(builder.toPrincipals());
            logAllowed(builder);
        } catch (ClassNotFoundException | NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
            logDenied(e);
            LOGGER.error("Invalid login configuration: {}", e);
            throw new AuthenticationException("Internal error: Invalid login configuration", e);
        } finally {
            logClear();
        }
    }

    @Override
    public Principal map(Principal principal) throws NoSuchPrincipalException
    {
        AliasClass class_ = _dao.getOrCreateAliasClass(principal.getClass());
        switch (class_.getCategory()) {
        case USER:
            UserAlias userAlias = _dao.getUserAlias(principal);
            if (userAlias != null) {
                return new UidPrincipal(userAlias.getUser().getUid());
            }
            break;

        case GROUP:
            GroupAlias groupAlias = _dao.getGroupAlias(principal);
            if (groupAlias != null) {
                return new GidPrincipal(groupAlias.getGroup().getGid(), false);
            }
            break;
        }
        return null;
    }

    @Override
    public Set<Principal> reverseMap(Principal principal) throws NoSuchPrincipalException
    {
        Set<Principal> principals = new HashSet<>();
        if (principal instanceof UidPrincipal) {
            long uid = ((UidPrincipal) principal).getUid();
            User user = _dao.getUser(uid);
            if (user != null) {
                principals.addAll(toPrincipals(user.getAliases()));
            }
        } else if (principal instanceof GidPrincipal) {
            long gid = ((GidPrincipal) principal).getGid();
            Group group = _dao.getGroup(gid);
            if (group != null) {
                principals.addAll(toPrincipals(group.getAliases()));
            }
        }
        return principals;
    }

    @Override
    public void session(Set<Principal> principals, Set<Object> attrib)
            throws AuthenticationException
    {
        try {
            LoginBuilder builder = new LoginBuilder();

            UidPrincipal uidPrincipal =
                    Iterables.getFirst(Iterables
                            .filter(principals, UidPrincipal.class), null);
            if (uidPrincipal == null) {
                throw new AuthenticationException("No account");
            }
            long uid = uidPrincipal.getUid();
            User user = _dao.getUser(uid);
            if (user == null) {
                throw new AuthenticationException("No account for " + uid);
            }
            builder.setUser(user);

            boolean hasPrimary = false;
            Iterable<GidPrincipal> gidPrincipals =
                    Iterables.filter(principals, GidPrincipal.class);
            for (GidPrincipal principal : gidPrincipals) {
                long gid = principal.getGid();
                Group group = _dao.getGroup(gid);
                if (group == null) {
                    throw new AuthenticationException("No group for " + gid);
                }

                boolean isPrimary = principal.isPrimaryGroup();
                builder.addGroup(group, isPrimary);
                hasPrimary |= isPrimary;
            }

            if (!hasPrimary) {
                throw new AuthenticationException("No primary group");
            }

            attrib.addAll(builder.toLoginAttributes());
        } catch (ClassNotFoundException | NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
            LOGGER.error("Invalid login configuration: {}", e);
            throw new AuthenticationException("Internal error: Invalid login configuration", e);
        }
    }

    private LoginBuilder mapPrincipals(Set<Principal> principals)
        throws AuthenticationException
    {
        LoginBuilder builder = new LoginBuilder();
        for (Principal principal: principals) {
            if (principal instanceof LoginUidPrincipal ||
                principal instanceof LoginGidPrincipal ||
                principal instanceof LoginNamePrincipal) {
                continue;
            }

            mapPrincipal(builder, principal);
        }
        if (builder.getUserAlias() == null) {
            throw new AuthenticationException("Subject lacks identifiable user identity");
        }
        return builder;
    }

    private void mapPrincipal(LoginBuilder builder, Principal principal)
        throws AuthenticationException
    {
        String name = principal.getName();
        AliasClass class_ = _dao.getOrCreateAliasClass(principal.getClass());
        switch (class_.getCategory()) {
        case USER:
            UserAlias userAlias = _dao.getUserAlias(principal);
            if (userAlias == null) {
                if (!class_.isAutoCreate()) {
                    builder.addUnmappedPrincipal(class_, principal);
                    break;
                }
                User user = _dao.createUser();
                user.setAuthorization(class_.getDefaultAuthorization());
                userAlias = user.addAlias(class_, name);
            }
            builder.addUserPrincipal(principal, userAlias);
            break;

        case GROUP:
            GroupAlias groupAlias = _dao.getGroupAlias(principal);
            if (groupAlias == null) {
                if (!class_.isAutoCreate()) {
                    builder.addUnmappedPrincipal(class_, principal);
                    break;
                }
                Group group = _dao.createGroup();
                group.setAuthorization(class_.getDefaultAuthorization());
                groupAlias = group.addAlias(class_, name);
            }
            builder.addGroupPrincipal(principal, groupAlias);
            break;

        default:
            builder.addUnmappedPrincipal(class_, principal);
            break;
        }
    }

    private Set<Principal> toPrincipals(Collection<? extends Alias> aliases)
    {
        try {
            Set<Principal> principals = new HashSet<>();
            for (Alias alias: aliases) {
                principals.add(alias.createPrincipal());
            }
            return principals;
        } catch (ClassNotFoundException | NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
            LOGGER.error("Invalid login configuration: {}", e);
            throw new RuntimeException("Internal error: Invalid login configuration", e);
        }
    }
}
