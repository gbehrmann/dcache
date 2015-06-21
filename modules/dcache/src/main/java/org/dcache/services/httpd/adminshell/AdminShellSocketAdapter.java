package org.dcache.services.httpd.adminshell;

import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.WebSocketAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.concurrent.TimeUnit;

/**
 * Created by arossi on 5/11/15.
 */
public class AdminShellSocketAdapter extends WebSocketAdapter {
    private static final Logger LOGGER = LoggerFactory.getLogger(AdminShellSocketAdapter.class);

    StringBuilder input;
    PipedOutputStream adapterOut;
    PipedInputStream adapterIn;

    private PipedInputStream termIn;
    private PipedOutputStream termOut;

    private Thread outPipe;
    private Thread inPipe;

    private Terminal terminal;

    @Override
    public void onWebSocketText(String message) {
        if (isConnected()) {
            LOGGER.trace("Received {}.", message);
            synchronized (input) {
                input.append(message);
                input.notifyAll();
            }
        }
    }

    @Override
    public void onWebSocketConnect(Session sess) {
        super.onWebSocketConnect(sess);
        try {
            openTerm();
        } catch (IOException e) {
            LOGGER.error("onWebSocketConnect {}, cause {}.",
                            e.getMessage(), e.getCause());
        }
    }

    @Override
    public void onWebSocketError(Throwable cause) {
        LOGGER.debug("onWebSocketError {}, cause {}.", cause.getMessage(),
                        cause.getCause());
        super.onWebSocketError(cause);
    }

    @Override
    public void onWebSocketClose(int statusCode, String reason) {
        LOGGER.debug("onWebSocketClose status: {}, cause {}.", statusCode,
                        reason);
        closeTerm();
        super.onWebSocketClose(statusCode, reason);
    }

    private void openTerm() throws IOException {
        input = new StringBuilder();
        adapterOut = new PipedOutputStream();
        termIn = new PipedInputStream(adapterOut);
        termOut = new PipedOutputStream();
        adapterIn = new PipedInputStream(termOut);
        terminal = AdminShellServlet.factory.createTerminal();
        terminal.setOutputStream(termOut);
        terminal.setErrorStream(termOut);
        terminal.setInputStream(termIn);
        terminal.start();
        outPipe = new Thread(new ReplyPipe());
        outPipe.start();
        inPipe = new Thread(new RequestPipe());
        inPipe.start();
    }

    private void closeTerm() {
        try {
            if (inPipe != null && inPipe.isAlive()) {
                inPipe.interrupt();
                synchronized (input) {
                    input.notifyAll();
                }
            }
        } catch (Exception e) {
            LOGGER.debug("Error on close {}: cause {}.", e.getMessage(),
                            e.getCause());
        }
    }

    private class RequestPipe implements Runnable {
        public void run() {
            try {
                while (!Thread.interrupted() && isConnected()) {
                    synchronized (input) {
                        if (input.length() == 0) {
                            input.wait(TimeUnit.SECONDS.toMillis(1));
                            continue;
                        }
                        adapterOut.write(input.toString().getBytes());
                        adapterOut.flush();
                        input.setLength(0);
                    }
                }
            } catch (InterruptedException | IOException e) {
                LOGGER.debug("RequestPipe: {}; cause {}.",
                                e.getMessage(),
                                e.getCause());
            } finally {
                try {
                    adapterOut.close();
                } catch (IOException e) {
                    LOGGER.debug("Closing adapter out: {}; cause {}.",
                                    e.getMessage(),
                                    e.getCause());
                }
            }
        }
    }

    private class ReplyPipe implements Runnable {
        public void run() {
            try {
                byte[] buf = new byte[1024];
                while (!Thread.interrupted()) {
                    int c = adapterIn.read(buf, 0, 1024);
                    if (c == -1) {
                        return;
                    }

                    String reply = new String(buf, 0, c);
                    if (isConnected()) {
                        getRemote().sendString(reply);
                        getRemote().flush();
                    }

                    if (reply.startsWith(Terminal.DISCONNECT)) {
                        break;
                    }

                    System.out.print(reply);
                }
            } catch (IOException e) {
                LOGGER.debug("ReplyPipe: {}; cause {}.",
                                e.getMessage(),
                                e.getCause());
            } finally {
                try {
                    termOut.close();
                } catch (IOException e) {
                    LOGGER.debug("Closing adapter in: {}; cause {}.",
                                    e.getMessage(),
                                    e.getCause());
                }
            }
        }
    }
}

