package org.dcache.services.httpd.adminshell;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Created by arossi on 5/8/15.
 */
public interface Terminal {
    static String DISCONNECT = "TERMINAL_CLOSED";

    void setInputStream(InputStream var1);

    void setOutputStream(OutputStream var1);

    void setErrorStream(OutputStream var1);

    void start() throws IOException;
}
