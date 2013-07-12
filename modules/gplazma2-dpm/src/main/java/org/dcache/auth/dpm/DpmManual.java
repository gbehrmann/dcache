package org.dcache.auth.dpm;

import dmg.util.Args;

import org.dcache.cells.CellCommandListener;

public class DpmManual
    implements CellCommandListener
{
    public String ac_man(Args args)
    {
        return
            "The database driven user mapping and authorization framework\n" +
            "manages user and group records. Each is associated with zero\n" +
            "or more aliases and zero or more attributes.\n" +
            "\n" +
            "Subtopics: \n" +
            "  man alias\n" +
            "  man attribute\n" +
            "  man user\n" +
            "  man group\n" +
            "  man authorization\n";
    }

    public String ac_man_alias(Args args)
    {
        return
            "Aliases are principals bound to either a user or a group. They\n" +
            "identify the entity. Aliases have a type referred to as the\n" +
            "alias class. The alias class defines several aspects of aliases:\n" +
            "\n" +
            "  class name  : Mapping to Java class\n" +
            "  display name: User friendly name\n" +
            "  short name  : Unique short identifier of the class\n" +
            "  category    : The type of entity (user or group) identified\n" +
            "  auto create : Whether the entity is automatically created\n" +
            "                when the alias is first encountered\n" +
            "  embodied    : Whether the alias is passed on to the rest of\n" +
            "                dCache after successful login.\n" +
            "\n" +
            "The short name is used as the option name in commands that\n" +
            "allow aliases to be manipulated.\n" +
            "\n" +
            "Commands to work with alias classes: \n" +
            "  alias add\n" +
            "  alias mod\n" +
            "  alias del\n" +
            "  alias ls";
    }

    public String ac_man_attribute(Args args)
    {
        return
            "A successful login is usually associated with a number of\n" +
            "attributes such as the root directory and home directory.\n" +
            "Both users and groups may be associated with any number of\n" +
            "attributes. Attributes have a type referred to as the attribute\n"+
            "class. The attribute class defines several aspects of attributes:\n" +
            "\n" +
            "  class name  : Mapping to Java class\n" +
            "  display name: User friendly name\n" +
            "  short name  : Unique short identifier of the class\n" +
            "  secondary group:\n" +
            "                Whether the attribute also applies to secondary\n"+
            "                groups or only to primary groups\n" +
            "  multiple    : Whether a succesful login may be associated with\n" +
            "                multiple values of this attribute class.\n" +
            "\n" +
            "The short name is used as the option name in commands that\n" +
            "allow attributes to be manipulated.\n" +
            "\n" +
            "Commands to work with attribute classes: \n" +
            "  attribute add\n" +
            "  attribute mod\n" +
            "  attribute del\n" +
            "  attribute ls";
    }

    public String ac_man_user(Args args)
    {
        return
            "A user record is uniquely identified by a UID and has one or\n" +
            "more groups. One of the groups is the primary login group and\n" +
            "is used as a primary group if no other group could be elected\n" +
            "as the primary group.\n" +
            "\n" +
            "A user record is identified by zero or more aliases. Each alias\n"+
            "uniquely identifies the user record it is bound to and cannot be\n"+
            "associated with multiple user records.\n" +
            "\n" +
            "A successful login will have exactly one user and at least the\n" +
            "groups associated with that user.";
    }

    public String ac_man_group(Args args)
    {
        return
            "A group record is uniquely identified by a GID.\n" +
            "\n" +
            "A group record is identified by zero or more aliases. Each\n"+
            "alias uniquely identifies the group record it is bound to and\n"+
            "cannot be associated with multiple group records.\n" +
            "\n" +
            "A successful login will have exactly one user and at least the\n" +
            "groups associated with that user. If the login attempt contains\n" +
            "principals that identify additional groups, then those groups\n" +
            "will become part of the login too.";
    }

    public String ac_man_authorization(String args)
    {
        return
            "A login attempt is processed by first mapping all available\n" +
            "principals to aliases which in turn are bound to user and group\n" +
            "records. If more than one group record is identified, then the\n" +
            "login is denied. Otherwise the authorization status of each\n" +
            "identified user record and each group record is consulted.\n" +
            "If any is BANNED then the login is denied; otherwise if any is\n"+
            "READONLY then the login is allowed, but read-only; otherwise if\n"+
            "any is ALLOWED then the login is allowed; otherwise the login\n" +
            "denied.";
    }
}
