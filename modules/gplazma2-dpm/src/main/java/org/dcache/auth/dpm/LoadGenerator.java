package org.dcache.auth.dpm;

import javax.security.auth.Subject;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.io.PrintWriter;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.globus.gsi.jaas.GlobusPrincipal;

import org.dcache.auth.FQANPrincipal;
import org.dcache.auth.Origin;
import org.dcache.auth.LoginStrategy;
import org.dcache.cells.AbstractCellComponent;
import org.dcache.cells.CellCommandListener;
import diskCacheV111.util.CacheException;

import dmg.util.Args;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoadGenerator
    extends AbstractCellComponent
    implements CellCommandListener, Runnable
{
    private final Logger _log =
        LoggerFactory.getLogger(LoadGenerator.class);

    private final static FQANPrincipal TOP_FQAN =
        new FQANPrincipal("/org.example.test", false);

    private LoginStrategy _loginStrategy;

    private int _threads = 5;
    private int _requests = 10000;

    private int _fqanSpace = 100;
    private int _dnSpace = 1000;
    private int _maxFqansPerRequest = 10;

    private long _lastRunStarted;
    private volatile long _lastRunFinished;

    private AtomicInteger _failures = new AtomicInteger();
    private AtomicInteger _running = new AtomicInteger();
    private AtomicLong _count = new AtomicLong();

    private Origin _origin;

    public void setLoginStrategy(LoginStrategy strategy)
    {
        _loginStrategy = strategy;
    }

    public final static String hh_test =
        "[-threads=<n>] [-requests=<n>] [-fqan=<n>] [-dn=<n>] [-maxFqan=<n>]";
    public synchronized String ac_test(Args args)
        throws UnknownHostException
    {
        _origin = new Origin(Origin.AuthType.ORIGIN_AUTHTYPE_WEAK,
                             InetAddress.getLocalHost());

        if (_running.get() > 0) {
            return "Test already running";
        }

        _threads = args.getIntOption("threads", _threads);
        _requests = args.getIntOption("requests", _requests);
        _fqanSpace = args.getIntOption("fqan", _fqanSpace);
        _dnSpace = args.getIntOption("dn", _dnSpace);
        _maxFqansPerRequest = args.getIntOption("maxFqan", _maxFqansPerRequest);

        _lastRunStarted = System.currentTimeMillis();
        _failures.set(0);
        _count.set(_requests);
        for (int i = 0; i < _threads; i++) {
            _running.getAndIncrement();
            new Thread(this).start();
        }

        return "Test started";
    }

    private int random(int max)
    {
        return (int) (Math.random() * max);
    }

    private String createFqan()
    {
        return "/org.example.test/" + random(_fqanSpace);
    }

    private String createDn()
    {
        return "/O=Grid/O=Test/OU=example.org/CN=" + random(_dnSpace);
    }

    private Subject createSubject()
    {
        Subject subject = new Subject();
        subject.getPrincipals().add(_origin);
        subject.getPrincipals().add(new GlobusPrincipal(createDn()));
        int fqans = random(_maxFqansPerRequest);
        for (int i = 0; i < fqans; i++) {
            subject.getPrincipals().add(new FQANPrincipal(createFqan(), i == 0));
        }
        subject.getPrincipals().add(TOP_FQAN);
        return subject;
    }

    public void run()
    {
        while (_count.getAndDecrement() > 0) {
            try {
                _loginStrategy.login(createSubject());
            } catch (CacheException e) {
                _failures.getAndIncrement();
            } catch (RuntimeException e) {
                _failures.getAndIncrement();
                _log.error("Login failure", e);
            }
        }

        if (_running.decrementAndGet() == 0) {
            _lastRunFinished = System.currentTimeMillis();
        }
    }

    public synchronized void getInfo(PrintWriter pw)
    {
        pw.println("FQAN space: " + _fqanSpace);
        pw.println("DN space: " + _dnSpace);
        pw.println("Max FQANs per request: " + _maxFqansPerRequest);
        pw.println("Failures: " + _failures.get());
        int running = _running.get();
        if (running > 0) {
            pw.println("Current run:");
            pw.println("    Started: " + new Date(_lastRunStarted));
            pw.println("    Threads: " + running);
            pw.println("    Requests left: " + Math.max(0, _count.get()));
        } else {
            long time = _lastRunFinished - _lastRunStarted;
            pw.println("Last run:");
            pw.println("    Requests: " + _requests);
            pw.println("    Time: " + (time / 1000.0) + " sec");
            if (time > 0) {
                pw.println("    Throughput: " +  (1000 * _requests) / time + " r/s");
            }
        }
    }
}
