package org.dcache.auth.dpm;

import org.springframework.beans.factory.annotation.Required;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;

import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringWriter;
import java.security.Principal;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.Callable;

import dmg.util.Args;
import dmg.util.command.Argument;
import dmg.util.command.Command;
import dmg.util.command.Option;

import org.dcache.auth.attributes.LoginAttribute;
import org.dcache.auth.dpm.model.Alias;
import org.dcache.auth.dpm.model.AliasCategory;
import org.dcache.auth.dpm.model.AliasClass;
import org.dcache.auth.dpm.model.Attribute;
import org.dcache.auth.dpm.model.AttributeClass;
import org.dcache.auth.dpm.model.Authorization;
import org.dcache.auth.dpm.model.Entity;
import org.dcache.auth.dpm.model.Group;
import org.dcache.auth.dpm.model.GroupAlias;
import org.dcache.auth.dpm.model.User;
import org.dcache.auth.dpm.model.UserAlias;
import org.dcache.cells.CellCommandListener;

/**
 * Cells CLI for managing database entries of DpmPlugin.
 */
public class DpmAdministrator
    implements CellCommandListener
{
    private static final Set<String> RESERVED_NAMES =
        new HashSet<>(Arrays.asList(new String[] {
                    "gid", "uid", "groups", "auth",
                    "rank", "category",
                    "display", "short",
                    "secondary", "no-secondary",
                    "multiple", "no-multiple",
                    "auto", "no-auto",
                    "embodied", "not-embodied"
                }));

    private AdministrationDao _dao;
    private TransactionTemplate _tx;

    @Required
    public void setAdministrationDao(AdministrationDao dao)
    {
        _dao = dao;
    }

    @Required
    public void setTransactionTemplate(TransactionTemplate template)
    {
        _tx = template;
    }

    private void update(Entity entity, Args args, boolean append)
    {
        String authorization = args.getOption("auth");
        if (authorization != null) {
            entity.setAuthorization(Authorization.valueOf(authorization.toUpperCase()));
        }

        for (Map.Entry<String,String> e: args.options().entries()) {
            String key = e.getKey();
            String value = e.getValue();
            if (!RESERVED_NAMES.contains(key)) {
                AttributeClass attributeClass = _dao.getAttributeClass(key);
                if (attributeClass != null) {
                    if (!append) {
                        entity.removeAttributes(attributeClass);
                    }
                    if (!value.isEmpty()) {
                        entity.addAttribute(attributeClass, value);
                    }
                    continue;
                }

                AliasClass aliasClass = _dao.getAliasClass(key);
                if (aliasClass != null) {
                    if (!append) {
                        entity.removeAliases(aliasClass);
                    }
                    if (!value.isEmpty()) {
                        entity.addAlias(aliasClass, value);
                    }
                    continue;
                }

                throw new IllegalArgumentException("No such alias or attribute: " + key + "=" + value);
            }
        }
    }

    private Group getGroup(String gid)
    {
        Group group = _dao.getGroup(Long.parseLong(gid));
        if (group == null) {
            throw new IllegalArgumentException("No such gid: " + gid);
        }
        return group;
    }

    private void update(User user, Args args, boolean append)
    {
        String gid = args.getOption("gid");
        if (gid != null) {
            user.setGroup(getGroup(gid));
        }

        String groups = args.getOption("groups");
        if (groups != null) {
            if (!append) {
                user.getGroups().clear();
            }
            for (String group: groups.split(",")) {
                user.getGroups().add(getGroup(group));
            }
        }

        update((Entity) user, args, append);
    }

    private void update(AttributeClass attributeClass, Args args)
    {
        String shortName = args.getOption("short");
        if (shortName != null) {
            attributeClass.setShortName(shortName);
        }
        String displayName = args.getOption("display");
        if (displayName != null) {
            attributeClass.setDisplayName(displayName);
        }
        if (args.getOption("secondary") != null) {
            attributeClass.setAppliesToSecondaryGroups(true);
        }
        if (args.getOption("no-secondary") != null) {
            attributeClass.setAppliesToSecondaryGroups(false);
        }
        if (args.getOption("multiple") != null) {
            attributeClass.setAllowMultiple(true);
        }
        if (args.getOption("no-multiple") != null) {
            attributeClass.setAllowMultiple(false);
        }
    }

    private void update(AliasClass aliasClass, Args args)
    {
        String shortName = args.getOption("short");
        String displayName = args.getOption("display");
        String category = args.getOption("category");
        String rank = args.getOption("rank");
        String auth = args.getOption("auth");
        boolean auto = (args.getOption("auto") != null);
        boolean noAuto = (args.getOption("no-auto") != null);
        boolean embodied = (args.getOption("embodied") != null);
        boolean notEmbodied = (args.getOption("not-embodied") != null);

        if (auto && noAuto) {
            throw new IllegalArgumentException("Only one of -auto and -no-auto is allowed");
        }
        if (shortName != null) {
            aliasClass.setShortName(shortName);
        }
        if (displayName != null) {
            aliasClass.setDisplayName(displayName);
        }
        if (category != null) {
            aliasClass.setCategory(AliasCategory.valueOf(category.toUpperCase()));
        }
        if (rank != null) {
            aliasClass.setRank(Integer.parseInt(rank));
        }
        if (auth != null) {
            aliasClass.setDefaultAuthorization(Authorization.valueOf(auth.toUpperCase()));
        }
        if (auto) {
            aliasClass.setAutoCreate(true);
        }
        if (noAuto) {
            aliasClass.setAutoCreate(false);
        }
        if (embodied) {
            aliasClass.setEmbodied(true);
        }
        if (notEmbodied) {
            aliasClass.setEmbodied(false);
        }
    }

    private void print(PrintWriter out, AliasClass a, boolean verbose)
    {
        out.append(a.getShortName()).
            append(" (").append(a.getDisplayName()).println(")");
        if (verbose) {
            out.append("    Class name  : ").println(a.getClassName());
            out.append("    Category    : ").println(a.getCategory());
            out.append("    Default auth: ").println(a.getDefaultAuthorization());
            out.append("    Auto create : ").println(a.isAutoCreate());
            out.append("    Embodied    : ").println(a.isEmbodied());
            out.append("    Rank        : ").println(a.getRank());
            out.println();
        }
    }

    private void print(PrintWriter out, AttributeClass a, boolean verbose)
    {
        out.append(a.getShortName()).
            append(" (").append(a.getDisplayName()).println(")");
        if (verbose) {
            out.append("    Class name      : ").println(a.getClassName());
            out.append("    Secondary groups: ").println(a.getAppliesToSecondaryGroups());
            out.append("    Allow multiple  : ").println(a.getAllowMultiple());
        }
    }

    private void print(PrintWriter out, Alias alias)
    {
        switch (alias.getAliasClass().getCategory()) {
        case USER:
            print(out, alias, ((UserAlias) alias).getUser(), true);
            break;
        case GROUP:
            print(out, alias, ((GroupAlias) alias).getGroup(), true);
            break;
        }
    }

    private void print(PrintWriter out, Alias alias, User user, boolean verbose)
    {
        if (alias != null) {
            out.println(alias);
        } else if (user.getAliases().isEmpty()) {
            out.println(user.getUid());
        } else {
            out.println(user.getAliases().iterator().next().getName());
        }

        if (verbose) {
            out.append("    UID          : ").println(user.getUid());
            out.append("    GID          : ").println(user.getGroup().getGid());
            out.append("    Groups       : ");
            for (Group group: user.getGroups()) {
                out.print(group.getGid());
                out.append(' ');
            }
            out.println();
            print(out, user);
            out.println();
        }
    }

    private void print(PrintWriter out, Alias alias, Group group, boolean verbose)
    {
        if (alias != null) {
            out.println(alias);
        } else if (group.getAliases().isEmpty()) {
            out.println(group.getGid());
        } else {
            out.println(group.getAliases().iterator().next().getName());
        }
        if (verbose) {
            out.append("    GID          : ").println(group.getGid());
            print(out, group);
            out.println();
        }
    }

    private <T extends Alias> void print(PrintWriter out, Entity<T> entity)
    {
        out.append("    Authorization: ").println(entity.getAuthorization());
        for (T alias: entity.getAliases()) {
            out.append("    ").
                append(alias.getAliasClass().getDisplayName()).
                append(": ").
                println(alias.getName());
        }
        for (Attribute attribute: entity.getAttributes()) {
            out.append("    ").
                append(attribute.getAttributeClass().getDisplayName()).
                append(": ").
                println(attribute.getValue());
        }
    }

    private abstract class TransactionalCommand<T extends Serializable> implements Callable<T>
    {
        @Override
        public T call()
        {
            return _tx.execute(new TransactionCallback<T>() {
                public T doInTransaction(TransactionStatus status)
                {
                    return callInTransaction();
                }
            });
        }

        protected abstract T callInTransaction();
    }

    @Command(name = "user ls",
            usage = "List user records. The listing can be constrained to a " +
                    "single user record.")
    class UserLsCommand extends TransactionalCommand<String>
    {
        @Option(name = "l", usage = "verbose")
        boolean verbose;

        @Argument(required = false)
        Long uid;

        @Override
        public String callInTransaction()
        {
            StringWriter s = new StringWriter();
            PrintWriter out = new PrintWriter(s);
            if (uid != null) {
                User user = _dao.getUser(uid);
                if (user != null) {
                    print(out, null, user, verbose);
                }
            } else {
                for (User user: _dao.getUsers()) {
                    print(out, null, user, verbose);
                }
            }
            return s.toString();
        }
    }

    public static final String hh_user_add =
        "[-gid=<gid>] [-groups=<gid>[,<gid>]...] [-auth=<auth>] [-<attribute>=<value>]... [-<alias>=<name>]...";
    public static final String fh_user_add =
        "Add a new user record.\n\n" +
        "Syntax:\n" +
        "  user add [OPTIONS]\n\n" +
        "Options:\n" +
        "  -gid=<gid> the GID of the primary login group\n" +
        "  -groups=<gid>[,<gid>]...\n" +
        "             the GIDs of groups the user belongs to\n" +
        "  -auth=banned|readonly|allowed|unknown\n" +
        "             the authorization status of the user\n" +
        "  -<attribute>=<value>\n" +
        "             where <attribute> is the short name of an attribute,\n" +
        "             and <value> is the attribute value\n" +
        "  -<alias>=<name>\n" +
        "             where <alias> is the short name of a user alias, and\n" +
        "             <name> is the alias name\n" +
        "\n" +
        "If -gid is not specified, then a new group is created for\n "+
        "this user.\n";
    public String ac_user_add(final Args args)
    {
        return _tx.execute(new TransactionCallback<String>() {
                public String doInTransaction(TransactionStatus status)
                {
                    User user = _dao.createUser();
                    update(user, args, true);
                    return "User " + user.getUid() + " created.";
                }
            });
    }

    @Command(name = "user del",
            usage = "Delete user record.")
    class UserDeleteCommand extends TransactionalCommand<String>
    {
        @Argument
        long uid;

        @Override
        protected String callInTransaction()
        {
            User user = _dao.getUser(uid);
            if (user == null) {
                throw new NoSuchElementException("No such user");
            }
            _dao.delete(user);
            return "User " + uid + " deleted.";
        }
    }

    public static final String hh_user_mod =
        "[-gid=<gid>] [-groups=<gid>[,<gid>]...] [-auth=<auth>] [-<alias>=<name>]... <uid>";
    public static final String fh_user_mod =
        "Modifies a user record.\n\n" +
        "Syntax:\n" +
        "  user mod [OPTIONS] <uid>\n\n" +
        "where <uid> is the UID of the user record to modify.\n\n" +
        "Options:\n" +
        "  -gid=<gid> the GID of the primary login group\n" +
        "  -groups=<gid>[,<gid>]...\n" +
        "             the GIDs of groups the user belongs to\n" +
        "  -auth=banned|readonly|allowed|unknown\n" +
        "             the authorization status of the user\n" +
        "  -<attribute>=<value>\n" +
        "             where <attribute> is the short name of an attribute,\n" +
        "             and <value> is the attribute value\n" +
        "  -<alias>=<name>\n" +
        "             where <alias> is the short name of a user alias, and\n" +
        "             <name> is the alias name\n";
    public String ac_user_mod_$_1(final Args args)
    {
        return _tx.execute(new TransactionCallback<String>() {
                public String doInTransaction(TransactionStatus status)
                {
                    long uid = Long.parseLong(args.argv(0));
                    User user = _dao.getUser(uid);
                    update(user, args, (args.getOption("append") != null));
                    return "User " + uid + " updated.";
                }
            });
    }

    @Command(name = "group ls",
            usage = "List group records. The listing can be constrained to a single group record.")
    class GroupLsCommand extends TransactionalCommand<String>
    {
        @Option(name = "l", usage = "verbose")
        boolean verbose;

        @Argument(required = false)
        Long gid;

        @Override
        protected String callInTransaction()
        {
            StringWriter s = new StringWriter();
            PrintWriter out = new PrintWriter(s);
            if (gid != null) {
                Group group = _dao.getGroup(gid);
                if (group != null) {
                    print(out, null, group, verbose);
                }
            } else {
                for (Group group: _dao.getGroups()) {
                    print(out, null, group, verbose);
                }
            }
            return s.toString();
        }
    }

    public static final String hh_group_add =
        "[-auth=<auth>] [-<attribute>=<value>]... [-<alias>=<name>]...";
    public static final String fh_group_add =
        "Add a new group record.\n\n" +
        "Syntax:\n" +
        "  group add [OPTIONS]\n\n" +
        "Options:\n" +
        "  -auth=banned|readonly|allowed|unknown\n" +
        "             the authorization status of the group\n" +
        "  -<attribute>=<value>\n" +
        "             where <attribute> is the short name of an attribute,\n" +
        "             and <value> is the attribute value\n" +
        "  -<alias>=<name>\n" +
        "             where <alias> is the short name of a group alias, and\n" +
        "             name is the alias name\n";
    public String ac_group_add(final Args args)
    {
        return _tx.execute(new TransactionCallback<String>() {
                public String doInTransaction(TransactionStatus status)
                {
                    Group group = _dao.createGroup();
                    update(group, args, true);
                    return "Group " + group.getGid() + " created.";
                }
            });
    }

    @Command(name = "group del",
            usage = "Delete group record.")
    class GroupDelCommand extends TransactionalCommand<String>
    {
        @Argument
        long gid;

        @Override
        protected String callInTransaction()
        {
            Group group = _dao.getGroup(gid);
            if (group == null) {
                throw new NoSuchElementException("No such group");
            }
            _dao.delete(group);
            return "Group " + gid + " deleted.";
        }
    }

    public static final String hh_group_mod =
        "[-auth=<auth>] [-<attribute>=<value>]... [-<alias>=<name>]... <gid>";
    public static final String fh_group_mod =
        "Modifies a group record.\n\n" +
        "Syntax:\n" +
        "  group mod [OPTIONS] <gid>\n\n" +
        "where <gid> is the GID of the group record to modify.\n\n" +
        "Options:\n" +
        "  -auth=banned|readonly|allowed|unknown\n" +
        "             the authorization status of the group\n" +
        "  -<attribute>=<value>\n" +
        "             where <attribute> is the short name of an attribute,\n" +
        "             and <value> is the attribute value\n" +
        "  -<alias>=<name>\n" +
        "             where <alias> is the short name of a user alias, and\n" +
        "             <name> is the alias name\n";
    public String ac_group_mod_$_1(final Args args)
    {
        return _tx.execute(new TransactionCallback<String>() {
                public String doInTransaction(TransactionStatus status)
                {
                    long uid = Long.parseLong(args.argv(0));
                    Group group = _dao.getGroup(uid);
                    update(group, args, (args.getOption("append") != null));
                    return "Group " + uid + " updated.";
                }
            });
    }

    public static final String hh_attribute_add =
        "[-display=<name>] [-short=<name>] [-secondary] [-no-secondary] [-multiple] [-no-multiple] <class>";
    public static final String fh_attribute_add =
        "Add a new attribute class.\n\n" +
        "Syntax:\n" +
        "  attribute add [OPTIONS] <class>\n\n" +
        "where <class> is the Java class name of the LoginAttribute to add.\n\n" +
        "Options:\n" +
        "  -display=<name>\n" +
        "             user readable name\n" +
        "  -short=<name>\n" +
        "             unique short identifier\n" +
        "  -secondary\n" +
        "             repect attribute in seconday groups\n" +
        "  -no-secondary\n" +
        "             ignore attribute in seconday groups\n" +
        "  -multiple\n" +
        "             allow multiple values per login\n" +
        "  -no-multiple\n" +
        "             do not allow multiple values per login\n";
    public String ac_attribute_add_$_1(final Args args)
        throws ClassNotFoundException
    {
        final Class<? extends LoginAttribute> class_ =
            toClass(LoginAttribute.class, args.argv(0));

        return _tx.execute(new TransactionCallback<String>() {
                public String doInTransaction(TransactionStatus status)
                {
                    AttributeClass attributeClass =
                        _dao.createAttributeClass(class_);
                    update(attributeClass, args);
                    return attributeClass.getClassName() + " created.";
                }
            });
    }

    public static final String hh_attribute_del =
        "<class>|<attribute>";
    public static final String fh_attribute_del =
        "Delete attribute class.\n" +
        "\n" +
        "Syntax:\n" +
        "  attrbute del <class>|<attribute>\n" +
        "\n" +
        "where <class> is the Java class name of the LoginAttribute and\n" +
        "<attribute> is the unique identifier of the attribute class.\n";
    public String ac_attribute_del_$_1(final Args args)
    {
        return _tx.execute(new TransactionCallback<String>() {
                public String doInTransaction(TransactionStatus status)
                {
                    AttributeClass attributeClass =
                        _dao.getAttributeClass(args.argv(0));
                    if (attributeClass == null) {
                        throw new NoSuchElementException("No such attribute class");
                    }
                    String className = attributeClass.getClassName();
                    _dao.delete(attributeClass);
                    return className + " deleted.";
                }
            });
    }

    public static final String hh_attribute_mod =
        "[-display=<name>] [-short=<name>] [-secondary] [-no-secondary] [-multiple] [-no-multiple] <class>|<attribute>";
    public static final String fh_attribute_mod =
        "Modify an attribute class.\n\n" +
        "Syntax:\n" +
        "  attribute mod [OPTIONS] <class>|<attribute>\n\n" +
        "where <class> is the Java class name of the LoginAttribute and\n" +
        "<attribute> is the unique identifier of the attribute class to modify.\n\n" +
        "Options:\n" +
        "  -display=<name>\n" +
        "             user readable name\n" +
        "  -short=<name>\n" +
        "             unique short identifier\n" +
        "  -secondary\n" +
        "             repect attribute in seconday groups\n" +
        "  -no-secondary\n" +
        "             ignore attribute in seconday groups\n" +
        "  -multiple\n" +
        "             allow multiple values per login\n" +
        "  -no-multiple\n" +
        "             do not allow multiple values per login\n";
    public String ac_attribute_mod_$_1(final Args args)
    {
        return _tx.execute(new TransactionCallback<String>() {
                public String doInTransaction(TransactionStatus status)
                {
                    AttributeClass attributeClass =
                        _dao.getAttributeClass(args.argv(0));
                    update(attributeClass, args);
                    return attributeClass.getClassName() + " updated.";
                }
            });
    }

    @Command(name = "attribute ls",
            usage = "List attribute classes. The listing can be constrained to a single attribute class.")
    class AttributeLsCommand extends TransactionalCommand<String>
    {
        @Option(name = "l", usage = "verbose")
        boolean verbose;

        @Argument(valueSpec = "CLASS|ATTRIBUTE", required = false)
        String name;

        @Override
        protected String callInTransaction()
        {
            StringWriter s = new StringWriter();
            PrintWriter out = new PrintWriter(s);
            if (name != null) {
                print(out, _dao.getAttributeClass(name), verbose);
            } else {
                for (AttributeClass a: _dao.getAttributeClasses()) {
                    print(out, a, verbose);
                }
            }
            return s.toString();
        }
    }

    public static final String hh_alias_add =
        "[-short=<name>] [-display=<name>] [-category=<category>] [-auth=<authorization>] [-auto] [-no-auto] [-embodied] [-not-embodied] [-rank=<int>] <class>";
    public static final String fh_alias_add =
        "Add a new alias class.\n\n" +
        "Syntax:\n" +
        "  alias add [OPTIONS] <class>\n\n" +
        "where <class> is the Java class name of a principal.\n\n" +
        "Options:\n" +
        "  -display=<name>\n" +
        "             user readable name\n" +
        "  -short=<name>\n" +
        "             unique short identifier\n" +
        "  -auth=banned|readonly|allowed|unknown\n" +
        "             the default authorization status for automatically\n" +
        "             created entitities\n" +
        "  -auto      automatically create entities corresponding\n" +
        "             to instances of this alias class\n" +
        "  -no-auto   do not automatically create entities corresponding\n" +
        "             to instances of this alias class\n" +
        "  -embodied  pass the principal on to the rest of dCache after\n" +
        "             successful login\n" +
        "  -not-embodied\n" +
        "             do not pass the principal on to the rest of dCache\n" +
        "             after successful login\n" +
        "  -rank=<n>  ranking used for determining the primary group of a \n" +
        "             login\n";
    public String ac_alias_add_$_1(final Args args)
        throws ClassNotFoundException
    {
        final Class<? extends Principal> class_ =
            toClass(Principal.class, args.argv(0));

        return _tx.execute(new TransactionCallback<String>() {
                public String doInTransaction(TransactionStatus status)
                {
                    AliasClass aliasClass = _dao.createAliasClass(class_);
                    update(aliasClass, args);
                    return aliasClass.getClassName() + " created.";
                }
            });
    }

    @Command(name = "alias del",
            usage = "Delete alias class")
    class AliasDelCommand extends TransactionalCommand<String>
    {
        @Argument(valueSpec = "CLASS|ALIAS")
        String name;

        @Override
        protected String callInTransaction()
        {
            AliasClass aliasClass = _dao.getAliasClass(name);
            if (aliasClass == null) {
                throw new NoSuchElementException("No such alias class");
            }
            String className = aliasClass.getClassName();
            _dao.delete(aliasClass);
            return className + " deleted.";
        }
    }

    public static final String hh_alias_mod =
        "[-<short>=<name>] [-display=<name>] [-category=<category>] [-auth=<auth>] [-auto] [-no-auto] [-embodied] [-not-embodied] [-rank=<int>] <class>|<alias>";
    public static final String fh_alias_mod =
        "Modify an alias class.\n\n" +
        "Syntax:\n" +
        "  alias mod [OPTIONS] <class>|<alias>\\n" +
        "where <class> is the Java class name of a principal and <alias>" +
        "is the unique identifier of the alias class to modify.\n\n" +
        "Options:\n" +
        "  -display=<name>\n" +
        "             user readable name\n" +
        "  -short=<name>\n" +
        "             unique short identifier\n" +
        "  -auth=banned|readonly|allowed|unknown\n" +
        "             the default authorization status for automatically\n" +
        "             created entities\n" +
        "  -auto      automatically create entities corresponding\n" +
        "             to instances of this alias class\n" +
        "  -no-auto   do not automatically create entities corresponding\n" +
        "             to instances of this alias class\n" +
        "  -embodied  pass the principal on to the rest of dCache after\n" +
        "             successful login\n" +
        "  -not-embodied\n" +
        "             do not pass the principal on to the rest of dCache\n" +
        "             after successful login\n" +
        "  -rank=<n>  ranking used for determining the primary group of a \n" +
        "             login\n";
    public String ac_alias_mod_$_1(final Args args)
    {
        return _tx.execute(new TransactionCallback<String>() {
                public String doInTransaction(TransactionStatus status)
                {
                    AliasClass aliasClass = _dao.getAliasClass(args.argv(0));
                    update(aliasClass, args);
                    return aliasClass.getClassName() + " updated.";
                }
            });
    }

    @Command(name = "alias ls",
            usage = "List attribute classes. The listing can be constrained to a single attribute class.")
    class AliasLsCommand extends TransactionalCommand<String>
    {
        @Option(name = "l", usage = "verbose")
        boolean verbose;

        @Argument(required = false, valueSpec = "CLASS|ALIAS")
        String name;

        @Override
        protected String callInTransaction()
        {
            StringWriter s = new StringWriter();
            PrintWriter out = new PrintWriter(s);
            if (name != null) {
                print(out, _dao.getAliasClass(name), verbose);
            } else {
                for (AliasClass a: _dao.getAliasClasses()) {
                    print(out, a, verbose);
                }
            }
            return s.toString();
        }
    }

    public static final String hh_ls =
        "[-<alias>=]<name>";
    public static final String Fh_ls =
        "List users and groups with a particular alias.\n\n" +
        "Syntax:\n" +
        "  ls [-<alias>=]<name>\n\n" +
        "where <name> is the name of an alias. The search can optionally be\n" +
        "constrained to a particular alias class by specifying the unique\n" +
        "identifier of the alias class.\n";
    public String ac_ls_$_0_1(final Args args)
    {
        return _tx.execute(new TransactionCallback<String>() {
                public String doInTransaction(TransactionStatus status)
                {
                    if (args.argc() + args.optc() != 1) {
                        throw new IllegalArgumentException("Exactly one argument or option is allowed");
                    }

                    StringWriter s = new StringWriter();
                    PrintWriter out = new PrintWriter(s);
                    String name = args.argv(0);
                    if (name != null) {
                        for (Alias alias: _dao.getAliases(name)) {
                            print(out, alias);
                        }
                    } else if (args.optc() == 1) {
                        String key = args.optv(0);
                        Alias alias = _dao.getAlias(key, args.getOption(key));
                        if (alias != null) {
                            print(out, alias);
                        }
                    }
                    return s.toString();
                }
            });
    }

    private <T> Class<? extends T> toClass(Class<T> expectedInterface, String className)
        throws ClassNotFoundException
    {
        Class<?> c = Class.forName(className);
        if (!expectedInterface.isAssignableFrom(c) || c.isInterface()) {
            throw new IllegalArgumentException("Class is not a " + expectedInterface.getSimpleName());
        }
        return c.asSubclass(expectedInterface);
    }
}
