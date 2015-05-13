package dmg.cells.network;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;

import dmg.cells.nucleus.CellAdapter;
import dmg.cells.nucleus.CellDomainInfo;
import dmg.cells.nucleus.CellExceptionMessage;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellNucleus;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.CellRoute;
import dmg.cells.nucleus.CellTunnel;
import dmg.cells.nucleus.CellTunnelInfo;
import dmg.cells.nucleus.MessageEvent;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.cells.nucleus.Releases;
import dmg.cells.nucleus.RoutedMessageEvent;
import dmg.cells.nucleus.SerializationException;
import dmg.util.StreamEngine;

import org.dcache.util.Args;
import org.dcache.util.Version;

/**
 *
 *
 * @author Patrick Fuhrmann
 * @version 0.1, 5 Mar 2001
 */
public class LocationMgrTunnel
    extends CellAdapter
    implements CellTunnel, Runnable
{
    /**
     * We use a single shared instance of Tunnels to coordinate route
     * creation between tunnels.
     */
    private static final Tunnels _tunnels = new Tunnels();

    private static final Logger _log = LoggerFactory.getLogger(LocationMgrTunnel.class);
    private static final String VERSION = Version.of(LocationMgrTunnel.class).getVersion();

    private final CellNucleus  _nucleus;

    private CellDomainInfo  _remoteDomainInfo;
    private final Socket _socket;

    private final OutputStream _rawOut;
    private final InputStream _rawIn;

    private ObjectSource _input;
    private ObjectSink _output;

    private boolean _down;

    //
    // some statistics
    //
    private int  _messagesToTunnel;
    private int  _messagesToSystem;

    public LocationMgrTunnel(String cellName, StreamEngine engine, Args args)
        throws IOException
    {
        super(cellName, "System", args);

        try {
            _nucleus = getNucleus();
            _socket = engine.getSocket();
            _socket.setTcpNoDelay(true);

            _rawOut = new BufferedOutputStream(engine.getOutputStream());
            _rawIn = new BufferedInputStream(engine.getInputStream());
        } catch (IOException e) {
            start();
            kill();
            throw e;
        }
        getNucleus().newThread(this, "Tunnel").start();
    }

    private void handshake() throws IOException
    {
        try {
            ObjectOutputStream out = new ObjectOutputStream(_rawOut);
            out.writeObject(new CellDomainInfo(_nucleus.getCellDomainName(), VERSION));
            out.flush();
            ObjectInputStream in = new ObjectInputStream(_rawIn);

            _remoteDomainInfo = (CellDomainInfo) in.readObject();

            if (_remoteDomainInfo == null) {
                throw new IOException("Remote dCache domain disconnected during handshake.");
            }

            String version = _remoteDomainInfo.getVersion();
            if (version == null) {
                throw new IOException("Connection from dCache older than 2.6 rejected.");
            }
            short remoteRelease = Releases.getRelease(version);
            String remoteEncoding;
            if (remoteRelease < Releases.RELEASE_2_10) {
                throw new IOException("Connection from dCache older than 2.10 rejected.");
            } else if (remoteRelease < Releases.RELEASE_2_13) {
                /* Releases before dCache 2.13 use Java Serialization for CellMessage.
                 * This branch can be removed in 2.14.
                 */
                _log.debug("Using Java serialization for message envelope.");
                _input = new JavaObjectSource(in);
                _output = new JavaObjectSink(out);
                remoteEncoding = "0";
            } else {
                _log.debug("Using raw serialization for message envelope.");

                /* Exchange message encoding version.
                 */
                out.writeUTF(CellMessage.ENCODING_VERSION);
                out.flush();
                remoteEncoding = in.readUTF();

                _log.debug("{} uses payload encoding {}.", getRemoteDomainName(), remoteEncoding);

                /* Since dCache 2.13 we use raw encoding of CellMessage.
                 */
                _input = new RawObjectSource(_rawIn);
                _output = new RawObjectSink(_rawOut);
            }
            /* If the encoding versions do not match, we use Java Serialization for
             * CellMessage payload. Otherwise we use the internal CellMessage FST
             * encoding.
             */
            if (!CellMessage.ENCODING_VERSION.equals(remoteEncoding)) {
                _input = new ReencodingObjectSource(_input);
                _output = new ReencodingObjectSink(_output);
                _log.warn("{} uses incompatible payload encoding {}. Falling back to slower Java serialization.",
                          getRemoteDomainName(), remoteEncoding);
            }
            _log.info("Established connection with {} version {}.", getRemoteDomainName(), version);
        } catch (ClassNotFoundException e) {
            throw new IOException("Cannot deserialize object. This is most likely due to a version mismatch.", e);
        }
    }

    synchronized private void setDown(boolean down)
    {
        _down = down;
        notifyAll();
    }

    synchronized private boolean isDown()
    {
        return _down;
    }

    private void returnToSender(CellMessage msg, NoRouteToCellException e)
        throws SerializationException
    {
        if (!(msg instanceof CellExceptionMessage)) {
            CellPath retAddr = msg.getSourcePath().revert();
            CellExceptionMessage ret = new CellExceptionMessage(retAddr, e);
            ret.setLastUOID(msg.getUOID());
            _nucleus.sendMessage(ret, true, true);
        }
    }

    private void receive()
        throws IOException, ClassNotFoundException
    {
        CellMessage msg;
        while ((msg = _input.readObject()) != null) {
            sendMessage(msg);
            _messagesToSystem++;
        }
    }

    @Override
    public void run()
    {
        if (isDown()) {
            throw new IllegalStateException("Tunnel has already been closed");
        }

        try {
            handshake();
            start();

            _tunnels.add(this);
            try {
                receive();
            } finally {
                _tunnels.remove(this);
            }
        } catch (EOFException | InterruptedException e) {
        } catch (ClassNotFoundException e) {
            _log.warn("Cannot deserialize object. This is most likely due to a version mismatch.");
        } catch (IOException e) {
            _log.warn("Error while reading from tunnel: {}", e.toString());
        } finally {
            start();
            kill();
        }
    }

    @Override
    public void messageArrived(MessageEvent me)
    {
        if (me instanceof RoutedMessageEvent) {
            CellMessage msg = me.getMessage();
            try {
                if (isDown()) {
                    throw new IOException("Tunnel has been shut down.");
                }
                _messagesToTunnel++;
                _output.writeObject(msg);
            } catch (IOException e) {
                _log.warn("Error while sending message: " + e.getMessage());
                returnToSender(msg, new NoRouteToCellException(msg, "Communication failure. Message could not be delivered."));
                kill();
            }
        } else {
            super.messageArrived(me);
        }
    }

    @Override
    public CellTunnelInfo getCellTunnelInfo()
    {
        return new CellTunnelInfo(getCellName(),
                new CellDomainInfo(_nucleus.getCellDomainName(), VERSION), _remoteDomainInfo);
    }

    protected String getRemoteDomainName()
    {
        return (_remoteDomainInfo == null)
            ? ""
            : _remoteDomainInfo.getCellDomainName();
    }

    public String toString()
    {
        return "Connected to " + getRemoteDomainName();
    }

    @Override
    public void getInfo(PrintWriter pw)
    {
        pw.println("Location Mgr Tunnel : " + getCellName());
        pw.println("-> Tunnel     : " + _messagesToTunnel);
        pw.println("-> Domain     : " + _messagesToSystem);
        pw.println("Peer          : " + getRemoteDomainName());
    }

    @Override
    public void cleanUp()
    {
        _log.info("Closing tunnel to " + getRemoteDomainName());
        setDown(true);
        try {
            _socket.shutdownInput();
            _socket.close();
        } catch (IOException e) {
            _log.warn("Failed to close socket: " + e.getMessage());
        }
    }

    public synchronized void join() throws InterruptedException
    {
        while (!isDown()) {
            wait();
        }
    }

    /**
     * This class encapsulates routing table management. It ensures
     * that at most one tunnel to any given domain is registered at a
     * time.
     *
     * It is assumed that all tunnels share the same cell glue (this
     * is normally the case for cells in the same domain).
     */
    private static class Tunnels
    {
        private Map<String,LocationMgrTunnel> _tunnels =
                new HashMap<>();

        /**
         * Adds a new tunnel. A route for the tunnel destination is
         * registered in the CellNucleus. The same tunnel cannot be
         * registered twice; unregister it first.
         *
         * If another tunnel is already registered for the same
         * destination, then the other tunnel is killed.
         */
        public synchronized void add(LocationMgrTunnel tunnel)
                throws InterruptedException
        {
            CellNucleus nucleus = tunnel.getNucleus();

            if (_tunnels.containsValue(tunnel)) {
                throw new IllegalArgumentException("Cannot register the same tunnel twice");
            }

            String domain = tunnel.getRemoteDomainName();

            /* Kill old tunnel first.
             */
            LocationMgrTunnel old;
            while ((old = _tunnels.get(domain)) != null) {
                old.kill();
                wait();
            }

            /* Add new route.
             */
            CellRoute route = new CellRoute(domain,
                    nucleus.getThisAddress().toString(),
                    CellRoute.DOMAIN);
            try {
                nucleus.routeAdd(route);
            } catch (IllegalArgumentException e) {
                _log.warn("Failed to add route: {}", e.getMessage());
            }

            /* Keep track of what we did.
             */
            _tunnels.put(domain, tunnel);
            notifyAll();
        }

        /**
         * Removes a tunnel and unregisters its routes. If the tunnel
         * was already removed, then nothing happens.
         *
         * It is crucial that the <code>_remoteDomainInfo</code> of
         * the tunnel does not change between the point at which it is
         * added and the point at which it is removed.
         */
        public synchronized void remove(LocationMgrTunnel tunnel)
        {
            if (_tunnels.remove(tunnel.getRemoteDomainName(), tunnel)) {
                notifyAll();
            }
        }
    }

    private interface ObjectSource
    {
        CellMessage readObject() throws IOException, ClassNotFoundException;
    }

    private interface ObjectSink
    {
        void writeObject(CellMessage message) throws IOException;
    }

    private static class JavaObjectSource implements ObjectSource
    {
        private ObjectInputStream in;

        private JavaObjectSource(ObjectInputStream in)
        {
            this.in = in;
        }

        @Override
        public CellMessage readObject() throws IOException, ClassNotFoundException
        {
            return (CellMessage) in.readObject();
        }
    }

    private static class JavaObjectSink implements ObjectSink
    {
        private ObjectOutputStream out;

        private JavaObjectSink(ObjectOutputStream out)
        {
            this.out = out;
        }

        @Override
        public void writeObject(CellMessage message) throws IOException
        {
            /* An object output stream will only serialize an object once
             * and likewise the object input stream will recreate the
             * object DAG at the other end. To avoid that the receiver
             * needs to unnecessarily keep references to previous objects,
             * we reset the stream. Notice that resetting the stream sends
             * a reset message. Hence we reset the stream before flushing
             * it.
             */
            out.writeObject(message);
            out.reset();
            out.flush();
        }
    }

    private static class RawObjectSink implements ObjectSink
    {
        private final DataOutputStream out;

        private RawObjectSink(OutputStream out)
        {
            this.out = new DataOutputStream(out);
        }

        @Override
        public void writeObject(CellMessage message) throws IOException
        {
            message.writeTo(out);
            out.flush();
        }
    }

    private static class RawObjectSource implements ObjectSource
    {
        private final DataInputStream in;

        private RawObjectSource(InputStream in)
        {
            this.in = new DataInputStream(in);
        }

        @Override
        public CellMessage readObject() throws IOException, ClassNotFoundException
        {
            return CellMessage.createFrom(in);
        }
    }

    private static class ReencodingObjectSink implements ObjectSink
    {
        private final ObjectSink out;

        private ReencodingObjectSink(ObjectSink out)
        {
            this.out = out;
        }

        @Override
        public void writeObject(CellMessage message) throws IOException
        {
            out.writeObject(message.decode().encodeJava());
        }
    }

    private static class ReencodingObjectSource implements ObjectSource
    {
        private final ObjectSource in;

        private ReencodingObjectSource(ObjectSource in)
        {
            this.in = in;
        }

        @Override
        public CellMessage readObject() throws IOException, ClassNotFoundException
        {
            return in.readObject().decodeJava().encode();
        }
    }
}
