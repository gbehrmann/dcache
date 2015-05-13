package dmg.cells.nucleus ;

import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.base.Strings;
import de.ruedigermoeller.serialization.FSTConfiguration;
import de.ruedigermoeller.serialization.FSTObjectInput;
import de.ruedigermoeller.serialization.FSTObjectOutput;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InvalidClassException;
import java.io.NotSerializableException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkState;

/**
  *
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
public class CellMessage implements Cloneable , Serializable {

  private static final long serialVersionUID = -5559658187264201731L;

  /**
   * Maximum TTL adjustment in milliseconds.
   */
  private static final int TTL_BUFFER_MAXIMUM = 10000;

  /**
   * Maximum TTL adjustment as a fraction of TTL.
   */
  private static final float TTL_BUFFER_FRACTION = 0.10f;

  /**
   * Configuration object for the FST fast serialization library.
   */
  private static final FSTConfiguration fstConfiguration = FSTConfiguration.createDefaultConfiguration();

    /**
   * Payload encoding version.
   *
   * The payload is encoded using FST, a fast-serialization library.
   *
   * This encoding does not allow versioning, which means that ANY change to fields
   * of serializable classes used in payload causes the serialization format
   * to break.
   *
   * This version field must be changed whenever we break incompatibility. When the
   * encoding versions do not match, cell message tunnels reencode the payload using
   * Java Serialization. Java Serialization does support versioning.
   */
  public static final String ENCODING_VERSION = "1";

  private CellPath    _source , _destination ;
  private Object      _message ;
  private long        _creationTime ;
  private long        _ttl = Long.MAX_VALUE;
  private int         _mode ;
  private UOID        _umid , _lastUmid ;
  private byte[]      _messageStream;
  private boolean     _isPersistent;
  private Object      _session;
  private static final int   ORIGINAL_MODE  = 0 ;
  private static final int   STREAM_MODE    = 1 ;
  private static final int   DUMMY_MODE     = 2 ;
  private transient long _receivedAt;

  public CellMessage(CellAddressCore address, Serializable msg) {
      this(msg, new CellPath(address));
  }

  public CellMessage(CellPath addr, Serializable msg) {
      this(msg, addr.clone());
  }

  private CellMessage(Serializable msg, CellPath path) {
     _destination  = path;
     _message      = msg;
     _source       = new CellPath();
     _creationTime = System.currentTimeMillis();
     _receivedAt   = _creationTime;
     _mode         = ORIGINAL_MODE;
     _umid         = new UOID();
     _lastUmid     = _umid;
     _session      = CDC.getSession();
     _messageStream = null;
  }

  @Override
  public String toString(){
    StringBuilder sb = new StringBuilder() ;
    sb.append( "<CM: S=" ).append( _source.toString() ).
       append( ";D=").append( _destination.toString() ) ;
    if( _mode == ORIGINAL_MODE ) {
        sb.append(";C=").
                append(_message.getClass().getName());
    } else {
        sb.append(";C=Stream");
    }

    sb.append( ";O=" ).append( _umid ).append( ";LO=" ).append( _lastUmid );
    if (_session != null) {
        sb.append(";SID=").append(_session);
    }
    if (_ttl < Long.MAX_VALUE) {
        sb.append(";TTL=").append(_ttl);
    }
    sb.append(">") ;
    return sb.toString() ;
  }
  @Override
public int     hashCode(){ return _umid.hashCode() ; }
  @Override
public boolean equals( Object obj ){
      if( obj instanceof CellMessage ) {
          return ((CellMessage) obj)._umid.equals(_umid);
      } else if( obj instanceof UOID ) {
          return obj.equals(_umid);
      }

      return false ;
  }
  public boolean     isReply() { return _isPersistent; }
  public UOID        getUOID() { return _umid ; }
  public UOID        getLastUOID() { return _lastUmid ; }
  public void        setUOID( UOID umid ) {
     _umid = umid ;
  }
  public void        setLastUOID( UOID lastUOID ) {
     _lastUmid = lastUOID ;
  }
  public Serializable getSession() { return (Serializable) _session; }
  public void        setSession(Serializable session) { _session = session; }
  public void        setTtl(long ttl) { _ttl = ttl; }
  public long        getTtl() { return _ttl; }
  public CellAddressCore getSourceAddress() { return _source.getSourceAddress(); }
  public CellPath    getDestinationPath(){ return _destination ; }
  public CellPath    getSourcePath(){ return _source ; }
  public Serializable getMessageObject(){ return (Serializable) _message  ; }
  public void        setMessageObject( Serializable obj ){ _message = obj ; }
  public void        revertDirection(){
     _destination = _source.revert();
     _source      = new CellPath() ;
     _lastUmid    = _umid ;
     _isPersistent = true;
  }
  public boolean isFinalDestination(){ return _destination.isFinalDestination() ; }
  public boolean isFirstDestination(){ return _destination.isFirstDestination() ; }
  public boolean nextDestination(){ return _destination.next() ; }
  //
  // package methods
  //
  boolean isStreamMode(){ return _mode == STREAM_MODE  ; }
  void touch(){
    if( _destination.isFirstDestination() ){
        _umid = new UOID() ;
    }
  }

    public CellMessage()
    {
        _mode = DUMMY_MODE;
    }

    /**
     * The method does not copy the message object - only the encoded message
     * stream (if any).
     */
    @Override
    public CellMessage clone()
    {
        try {
            CellMessage copy = (CellMessage) super.clone();
            copy._destination = _destination.clone();
            if (_source != null) {
                copy._source = _source.clone();
            }
            copy._messageStream = _messageStream;
            return copy;
        } catch (CloneNotSupportedException e) {
            throw Throwables.propagate(e);
        }
    }

    public CellMessage encode() throws SerializationException
    {
        checkState(_mode == ORIGINAL_MODE);
        CellMessage encoded = clone();
        encoded._mode = STREAM_MODE;
        encoded._message = null;
        encoded.setPayload(_message);
        return encoded;
    }

    public CellMessage decode() throws SerializationException
    {
        checkState(_mode == STREAM_MODE);
        CellMessage decoded = clone();
        decoded._mode = ORIGINAL_MODE;
        decoded._messageStream = null;
        decoded._message = getPayload();
        return decoded;
    }

   public void addSourceAddress( CellAddressCore source ){
      _source.add(source) ;
  }

    private void readObject(ObjectInputStream stream)
        throws IOException, ClassNotFoundException
    {
        stream.defaultReadObject();
        _receivedAt = System.currentTimeMillis();
    }

    /**
     * Returns the number of milliseconds since this message was
     * received by the local domain. If the message created in the
     * local domain, then the method returns the number of
     * milliseconds since it was created.
     */
    public long getLocalAge()
    {
        return System.currentTimeMillis() - _receivedAt;
    }

    /**
     * Returns the adjusted TTL of a message. The adjusted TTL is the
     * TTL with some time subtracted to allow for cell communication
     * overhead. Returns Long.MAX_VALUE if the TTL is infinite.
     */
    public long getAdjustedTtl()
    {
        return (_ttl == Long.MAX_VALUE)
                ? Long.MAX_VALUE
                : _ttl - Math.min(TTL_BUFFER_MAXIMUM, (long) (_ttl * TTL_BUFFER_FRACTION));
    }

    public void writeTo(DataOutput out) throws IOException
    {
        checkState(_mode == STREAM_MODE);

        out.writeByte(_mode);
        out.writeBoolean(_isPersistent);
        out.writeLong(_creationTime);
        out.writeLong(_ttl);
        _umid.writeTo(out);
        _lastUmid.writeTo(out);
        _source.writeTo(out);
        _destination.writeTo(out);

        out.writeUTF(Objects.toString(_session, ""));
        out.writeInt(_messageStream.length);
        out.write(_messageStream);
    }

    public static CellMessage createFrom(DataInput in) throws IOException
    {
        CellMessage message = new CellMessage();
        message._mode = in.readByte();
        if (message._mode != STREAM_MODE) {
            throw new IOException("Invalid message tunnel wire format.");
        }
        /* Need to initialize the transient reception time after the first field is read as
         * this function may have been called while the input stream is empty.
         */
        message._receivedAt = System.currentTimeMillis();
        message._isPersistent = in.readBoolean();
        message._creationTime = in.readLong();
        message._ttl = in.readLong();
        message._umid = UOID.createFrom(in);
        message._lastUmid = UOID.createFrom(in);
        message._source = CellPath.createFrom(in);
        message._destination = CellPath.createFrom(in);
        message._session = Strings.emptyToNull(in.readUTF());
        int len = in.readInt();
        message._messageStream = new byte[len];
        in.readFully(message._messageStream);
        return message;
    }

    public CellMessage encodeJava() throws IOException
    {
        checkState(!isStreamMode());
        ByteArrayOutputStream array = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(array)) {
            out.writeObject(_message);
        } catch (InvalidClassException e) {
            throw new SerializationException("Failed to serialize object: "
                                                     + e + "(this is usually a bug)", e);
        } catch (NotSerializableException e) {
            throw new SerializationException("Failed to serialize object because the object is not serializable (this is usually a bug)", e);
        }
        CellMessage encoded = clone();
        encoded._messageStream = array.toByteArray();
        encoded._message = null;
        encoded._mode = STREAM_MODE;
        return encoded;
    }

    public CellMessage decodeJava() throws IOException
    {
        checkState(isStreamMode());
        ByteArrayInputStream in = new ByteArrayInputStream(_messageStream);
        try (ObjectInputStream stream = new ObjectInputStream(in)) {
            CellMessage decoded = clone();
            decoded._messageStream = null;
            decoded._message = stream.readObject();
            decoded._mode = ORIGINAL_MODE;
            return decoded;
        } catch (ClassNotFoundException e) {
            throw new SerializationException(
                    "Failed to deserialize object: The class could not be found. Is there a software version mismatch in your installation?",
                    e);
        }
    }

    private void setPayload(Object object)
    {
        try {
            FSTObjectOutput out = fstConfiguration.getObjectOutput();
            out.writeObject(object);
            _messageStream = out.getCopyOfWrittenBuffer();
            _mode = STREAM_MODE;
        } catch (InvalidClassException e) {
            throw new SerializationException("Failed to serialize object: "
                                                     + e + "(this is usually a bug)", e);
        } catch (NotSerializableException e) {
            throw new SerializationException("Failed to serialize object because the object is not serializable (this is usually a bug)", e);
        } catch (IOException e) {
            throw new SerializationException("Failed to serialize object: " + e, e);
        }
    }

    private Object getPayload()
    {
        checkState(isStreamMode());
        try {
            FSTObjectInput stream = fstConfiguration.getObjectInput(_messageStream);
            return stream.readObject();
        } catch (ClassNotFoundException e) {
            throw new SerializationException(
                    "Failed to deserialize object: The class could not be found. Is there a software version mismatch in your installation?",
                    e);
        } catch (IOException e) {
            throw new SerializationException("Failed to deserialize object: " + e, e);
        }
    }
}
