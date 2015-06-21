package org.dcache.services.httpd.adminshell;

import org.eclipse.jetty.websocket.servlet.WebSocketServlet;
import org.eclipse.jetty.websocket.servlet.WebSocketServletFactory;
import org.springframework.beans.factory.annotation.Required;

import java.util.concurrent.TimeUnit;
import javax.servlet.annotation.WebServlet;

@SuppressWarnings("serial")
@WebServlet(name = "Shell Servlet", urlPatterns = { "/adminshell" })
public class AdminShellServlet extends WebSocketServlet {
    static AdminTerminalFactory factory;

    @Override
    public void configure(WebSocketServletFactory factory) {
        factory.getPolicy().setIdleTimeout(TimeUnit.MINUTES.toMillis(5));
        factory.register(AdminShellSocketAdapter.class);
    }

    @Required
    public void setTerminalFactory(AdminTerminalFactory factory) {
        this.factory = factory;
    }
}
