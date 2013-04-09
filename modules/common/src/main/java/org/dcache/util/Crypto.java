package org.dcache.util;

/**
 *  Various useful cryptographic utility method
 */
public class Crypto
{
    /* The following is a list of cipher suites that are problematic
     * with currently suported versions of Java.
     *
     * The problem is described here:
     *
     *     https://bugs.launchpad.net/ubuntu/+source/openjdk-6/+bug/1006776
     *
     * Note that, despite the comment in the ticket, the problem is
     * also present for OpenJDK v7.
     *
     * During the SSL/TLS handshake, the client will send a list of
     * supported ciphers.  The server will choose one, based on the
     * client-supplied list and the ciphers it supports.
     *
     * The problem is that libnss3 supports only 3 EC (elliptic curve)
     * ciphers, but the Java security provider that wraps libnss3
     * believes it supports all elliptic curve ciphers.  If the
     * client's list of supported ciphers includes those based on EC
     * then the Java server may choose an EC cipher.  This SSL
     * negotiation will then fail with the 'CKR_DOMAIN_PARAMS_INVALID'
     * error.  For example, JGlobus will log the following:
     *
     *    19 Apr 2013 17:40:43 (SRM-zitpcx6184) []
     *    org.globus.common.Chained IOException: Authentication failed
     *    [Caused by: Failure unspecified at GSS-API level [Caused by:
     *    Failure unspecified at GSS-API level [Caused by:
     *    sun.security.pkcs11.wrapper.PKCS11Exception:
     *    CKR_DOMAIN_PARAMS_INVALID]]]
     *
     * Clients that do not support EC-based ciphers are not affected
     * by this bug; for example, OpenSSL prior to v1.0.0 (or there
     * abouts) and most Java-based clients.
     *
     * Some clients provide control of the cipher selection.  For
     * example, the OpenSSL sample client ('s_client') provides the
     * -cipher option.  This may be used to omit all eliptic curve
     * ciphers as a client-side work-around.  For example, the
     * following command may be used to connect to the localhost's
     * HTTPS SRM endpoint while excluding any EC-based cipher:
     *
     *     openssl s_client -connect localhost:8445 -cipher 'DEFAULT:!ECDH'
     *
     * It is also possible to disable support on the server by editing
     * the 'java.security' file to remove the security provider that
     * is supplying the (broken) eliptic curve support.  This is
     * controlled by the file 'java.security', which is located in a
     * distribution-specific directory.  For Debian, the file is in
     * the /etc/java-7-openjdk/security directory and for RHEL, it's
     * in the /usr/java/<version>/jre/lib/security directory.
     *
     * As we can't control all clients and the instructions for how to
     * disable the EC ciphers is fiddly, we have a server-side
     * work-around: dCache components (typically doors) can remove
     * these ciphers from the list of supported ciphers, preventing
     * them from being chosen during the SSL/TLS negotiation.
     *
     * The following list was generated from OpenJDK source code using
     * the command:
     */

    //    sed -n '/add.*TLS_ECDHE/s/.*add(\([^,]*\).*/        \1,/p'
    //    sun/security/ssl/CipherSuite.java | sort

    public static final String[] BANNED_CIPHERS = {
        "TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA384",
        "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA384",
        "TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA",
        "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA",
        "TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256",
        "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256",
        "TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA",
        "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA",
        "TLS_ECDHE_ECDSA_WITH_RC4_128_SHA",
        "TLS_ECDHE_RSA_WITH_RC4_128_SHA",
        "TLS_ECDHE_ECDSA_WITH_3DES_EDE_CBC_SHA",
        "TLS_ECDHE_RSA_WITH_3DES_EDE_CBC_SHA",
        "TLS_ECDHE_ECDSA_WITH_NULL_SHA",
        "TLS_ECDHE_RSA_WITH_NULL_SHA",
        "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256",
        "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384",
        "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",
        "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384",
        "TLS_ECDHE_PSK_WITH_RC4_128_SHA",
        "TLS_ECDHE_PSK_WITH_3DES_EDE_CBC_SHA",
        "TLS_ECDHE_PSK_WITH_AES_128_CBC_SHA",
        "TLS_ECDHE_PSK_WITH_AES_256_CBC_SHA",
        "TLS_ECDHE_PSK_WITH_AES_128_CBC_SHA256",
        "TLS_ECDHE_PSK_WITH_AES_256_CBC_SHA384",
        "TLS_ECDHE_PSK_WITH_NULL_SHA",
        "TLS_ECDHE_PSK_WITH_NULL_SHA256",
        "TLS_ECDHE_PSK_WITH_NULL_SHA384"};
}
