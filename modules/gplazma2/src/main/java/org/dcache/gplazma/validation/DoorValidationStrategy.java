package org.dcache.gplazma.validation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;

import java.security.Principal;
import java.util.Set;

import org.dcache.auth.GidPrincipal;
import org.dcache.auth.UidPrincipal;
import org.dcache.gplazma.AuthenticationException;
import org.dcache.gplazma.LoginReply;

import static org.dcache.gplazma.util.Preconditions.checkAuthentication;

/**
 * Door specific validation strategy
 * which checks that there is one and only one user principal, one and only one
 * Uid Principals, one and only one primary GID principal present in principals,
 * and there is one and only one of each home and root directories in the
 * attributes
 * @author timur
 */
public class DoorValidationStrategy  implements ValidationStrategy {

    private static final Logger LOGGER = LoggerFactory.getLogger(DoorValidationStrategy.class);

    @Override
    public void validate(LoginReply loginReply)
            throws AuthenticationException {
        LOGGER.debug("Validating loginReply {}",loginReply);
        if(loginReply == null) {
            throw new NullPointerException ("loginReply is null");
        }
        Set<Principal> principals = getPrincipalsFromLoginReply(loginReply);
        validatePrincipals(principals);
    }

    /**
     * checks if authorizedPrincipals set contain at one and only one
     * instance of each type of
     * {@link UidPrincipal UidPrincipal} and
     * {@link GidPrincipal GidPrincipal}
     * @param principals
     * @throws AuthenticationException if check fails
     */
    private static void validatePrincipals(Set<Principal> principals)
            throws AuthenticationException {
        boolean hasUid = false;
        boolean hasPrimaryGid = false;
        for(Principal principal:principals) {
            if(principal instanceof UidPrincipal) {
                checkAuthentication(!hasUid, "multiple UIDs");
                hasUid = true;
                continue;
            }
            if(principal instanceof GidPrincipal) {
                GidPrincipal gidPrincipal = (GidPrincipal) principal;
                if(gidPrincipal.isPrimaryGroup()) {
                    checkAuthentication(!hasPrimaryGid, "multiple GIDs");
                    hasPrimaryGid = true;
                }
            }
        }

        checkAuthentication(hasUid && hasPrimaryGid,
                principalsErrorMessage(hasUid, hasPrimaryGid));
    }

    private static String principalsErrorMessage(
            boolean hasUid, boolean hasPrimaryGid)
    {
        StringBuilder errorMessage = new StringBuilder();

        if(!hasUid) {
            appendWithComma(errorMessage, "no UID");
        }
        if(!hasPrimaryGid) {
            appendWithComma(errorMessage, "no primary GID");
        }

        return errorMessage.toString();
    }

    private static StringBuilder appendWithComma(StringBuilder sb,
            String message)
    {
        if(sb.length() > 0) {
            sb.append(", ");
        }

        return sb.append(message);
    }

    private static Set<Principal> getPrincipalsFromLoginReply(LoginReply loginReply)
            throws AuthenticationException
    {
        Subject subject = loginReply.getSubject();
        checkAuthentication(subject != null, "subject is null");

        Set<Principal> principals = subject.getPrincipals();
        checkAuthentication(principals != null, "subject principals is null");

        return principals;
    }
}
