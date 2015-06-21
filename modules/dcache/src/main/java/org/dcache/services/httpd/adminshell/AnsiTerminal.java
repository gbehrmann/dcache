package org.dcache.services.httpd.adminshell;

import jline.TerminalSupport;
import jline.console.ConsoleReader;
import jline.console.history.FileHistory;
import jline.console.history.MemoryHistory;
import jline.console.history.PersistentHistory;
import org.fusesource.jansi.Ansi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.OutputStream;

import diskCacheV111.admin.UserAdminShell;
import diskCacheV111.util.CacheException;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.cells.nucleus.SerializationException;
import dmg.util.CommandAclException;
import dmg.util.CommandEvaluationException;
import dmg.util.CommandException;
import dmg.util.CommandExitException;
import dmg.util.CommandPanicException;
import dmg.util.CommandSyntaxException;
import dmg.util.CommandThrowableException;
import org.dcache.commons.util.Strings;

import static org.fusesource.jansi.Ansi.Color.CYAN;
import static org.fusesource.jansi.Ansi.Color.RED;

/**
 * This class implements the Command Interface, which is part of the sshd-core
 * library allowing to access input and output stream of the ssh2Server. This
 * class is also the point of connecting the ssh2 streams to the
 * userAdminShell's input and output streams. The run() method of the thread
 * takes care of handling the user input. It lets the userAdminShell execute the
 * commands entered by the user, waits for the answer and outputs the answer to
 * the terminal of the user.
 * @author bernardt
 */
public abstract class AnsiTerminal implements Terminal, Runnable {
    private static final Logger LOGGER
                    = LoggerFactory.getLogger(AnsiTerminal.class);
    private static final int HISTORY_SIZE = 50;

    class ConsoleReaderTerminal extends TerminalSupport {
        ConsoleReaderTerminal() {
            super(true);
            setAnsiSupported(true);
            setEchoEnabled(false);
        }

        @Override
        public int getHeight() {
            return AnsiTerminal.this.getHeight();
        }

        @Override
        public int getWidth() {
            return  AnsiTerminal.this.getWidth();
        }
    }

    private UserAdminShell userAdminShell;
    private InputStream inputStream;
    private OutputStream outputStream;
    private Thread adminShellThread;
    private ConsoleReader consoleReader;
    private MemoryHistory history;
    private boolean useColors;

    public AnsiTerminal(File historyFile, boolean useColors, UserAdminShell shell)
    {
        this.useColors = useColors;
        userAdminShell = shell;
        if (historyFile != null && (!historyFile.exists() || historyFile.isFile())) {
            try {
                history = new FileHistory(historyFile);
                history.setMaxSize(HISTORY_SIZE);
            } catch (IOException e) {
                LOGGER.warn("History creation failed: " + e.getMessage());
            }
        }
    }

    public abstract int getHeight();

    public abstract int getWidth();

    @Override
    public void setErrorStream(OutputStream err) {
    }

    @Override
    public void setInputStream(InputStream in) {
        inputStream = in;
    }

    @Override
    public void setOutputStream(OutputStream out) {
        outputStream = new FilterOutputStream(out);
    }

    @Override
    public void start() throws IOException {
        consoleReader = new ConsoleReader(inputStream,
                                          outputStream,
                                          new ConsoleReaderTerminal());
        adminShellThread = new Thread(this);
        adminShellThread.start();
    }

    @Override
    public void run() {
        try {
            initAdminShell();
            runAsciiMode();
        } catch (IOException e) {
            LOGGER.warn("RUN:" + e.getMessage());
        } finally {
            try {
                if (history instanceof PersistentHistory) {
                    ((PersistentHistory) history).flush();
                }
            } catch (IOException e) {
                LOGGER.warn("Failed to shutdown console cleanly: "
                                + e.getMessage());
            }
        }
    }

    private void initAdminShell() throws IOException {
        if (history != null) {
            consoleReader.setHistory(history);
        }
        consoleReader.addCompleter(userAdminShell);
        consoleReader.println(userAdminShell.getHello());
        consoleReader.flush();
    }

    private void runAsciiMode() throws IOException {
        Ansi.setEnabled(useColors);
        boolean exit = false;
        while (!exit) {
            String prompt = Ansi.ansi().bold().a(userAdminShell.getPrompt()).boldOff().toString();
            Object result;
            try {
                String str = consoleReader.readLine(prompt);
                try {
                    if (str == null) {
                        throw new CommandExitException();
                    }
                    result = userAdminShell.executeCommand(str);
                } catch (IllegalArgumentException e) {
                    result = e.toString();
                } catch (SerializationException e) {
                    result =
                            "There is a bug here, please report to support@dcache.org";
                    LOGGER.error("This must be a bug, please report to support@dcache.org.",
                                    e);
                } catch (CommandSyntaxException e) {
                    result = e;
                } catch (CommandEvaluationException | CommandAclException e) {
                    result = e.getMessage();
                } catch (CommandExitException e) {
                    result = null;
                    exit = true;
                } catch (CommandPanicException e) {
                    result = "Command '" + str + "' triggered a bug (" + e.getTargetException() +
                             "); the service log file contains additional information. Please " +
                             "contact support@dcache.org.";
                } catch (CommandThrowableException e) {
                    Throwable cause = e.getTargetException();
                    if (cause instanceof CacheException) {
                        result = cause.getMessage();
                    } else {
                        result = cause.toString();
                    }
                } catch (CommandException e) {
                    result =
                            "There is a bug here, please report to support@dcache.org: "
                            + e.getMessage();
                    LOGGER.warn("Unexpected exception, please report this "
                                    + "bug to support@dcache.org");
                } catch (NoRouteToCellException e) {
                    result =
                            "Cell name does not exist or cell is not started: "
                            + e.getMessage();
                    LOGGER.warn("The cell the command was sent to is no "
                                    + "longer there: {}", e.getMessage());
                } catch (RuntimeException e) {
                    result = String.format("Command '%s' triggered a bug (%s); please" +
                                           " locate this message in the log file of the admin service and" +
                                           " send an email to support@dcache.org with this line and the" +
                                           " following stack-trace", str, e);
                    LOGGER.error((String) result, e);
                }
            } catch (InterruptedIOException e) {
                consoleReader.getCursorBuffer().clear();
                consoleReader.println();
                result = null;
            } catch (InterruptedException e) {
                consoleReader.println("^C");
                consoleReader.flush();
                consoleReader.getCursorBuffer().clear();
                result = null;
                exit = true;
            } catch (IOException e) {
                throw e;
            } catch (Exception e) {
                result = e.getMessage();
                if(result == null) {
                    result = e.getClass().getSimpleName() + ": (null)";
                }
            }

            if (result != null) {
                if (result instanceof CommandSyntaxException) {
                    CommandSyntaxException e = (CommandSyntaxException) result;
                    Ansi sb = Ansi.ansi();
                    sb.fg(RED).a("Syntax error: ").a(e.getMessage()).newline();
                    String help = e.getHelpText();
                    if (help != null) {
                        sb.fg(CYAN);
                        sb.a("Help : ").newline();
                        sb.a(help);
                    }
                    consoleReader.println(sb.reset().toString());
                } else {
                    String s;
                    s = Strings.toMultilineString(result);
                    if (!s.isEmpty()) {
                        consoleReader.println(s);
                        consoleReader.flush();
                    }
                }
            }
            consoleReader.flush();
        }

        consoleReader.println(Terminal.DISCONNECT);
        consoleReader.flush();
    }
}
