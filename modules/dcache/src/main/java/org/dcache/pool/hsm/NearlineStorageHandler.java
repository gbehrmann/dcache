package org.dcache.pool.hsm;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.nio.channels.CompletionHandler;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.DiskErrorCacheException;
import diskCacheV111.util.FileInCacheException;
import diskCacheV111.util.FileNotFoundCacheException;
import diskCacheV111.util.HsmLocationExtractorFactory;
import diskCacheV111.util.HsmSet;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.PoolFileFlushedMessage;
import diskCacheV111.vehicles.StorageInfo;
import diskCacheV111.vehicles.StorageInfoMessage;

import dmg.cells.nucleus.DelayedReply;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.util.command.Argument;
import dmg.util.command.Command;
import dmg.util.command.Option;

import org.dcache.cells.AbstractCellComponent;
import org.dcache.cells.CellStub;
import org.dcache.namespace.FileAttribute;
import org.dcache.pool.classic.ChecksumModule;
import org.dcache.pool.classic.NopCompletionHandler;
import org.dcache.pool.repository.EntryState;
import org.dcache.pool.repository.IllegalTransitionException;
import org.dcache.pool.repository.ReplicaDescriptor;
import org.dcache.pool.repository.Repository;
import org.dcache.pool.repository.StickyRecord;
import org.dcache.util.Checksum;
import org.dcache.vehicles.FileAttributes;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.transform;

/**
 * Entry point to and management interface of the nearline storage subsystem.
 */
public class NearlineStorageHandler extends AbstractCellComponent
{
    private static final Logger LOGGER = LoggerFactory.getLogger(NearlineStorageHandler.class);

    private static final Set<Repository.OpenFlags> NO_FLAGS = Collections.emptySet();

    private final FlushRequestContainer flushRequests = new FlushRequestContainer();
    private final StageRequestContainer stageRequests = new StageRequestContainer();
    private final RemoveRequestContainer removeRequests = new RemoveRequestContainer();

    private ListeningExecutorService executor;
    private Repository repository;
    private ChecksumModule checksumModule;
    private PnfsHandler pnfs;
    private CellStub flushMessageTarget;
    private CellStub billingStub;
    private HsmSet hsmSet;
    private long _maxRestoreRun;
    private long _maxStoreRun;
    private long _maxRemoveRun;

    public void setExecutor(ListeningExecutorService executor)
    {
        this.executor = executor;
    }

    public void setRepository(Repository repository)
    {
        this.repository = repository;
    }

    public void setChecksumModule(ChecksumModule checksumModule)
    {
        this.checksumModule = checksumModule;
    }

    public void setPnfsHandler(PnfsHandler pnfs)
    {
        this.pnfs = pnfs;
    }

    public void setFlushMessageTarget(CellStub flushMessageTarget)
    {
        this.flushMessageTarget = flushMessageTarget;
    }

    public void setBillingStub(CellStub billingStub)
    {
        this.billingStub = billingStub;
    }

    public void setHsmSet(HsmSet hsmSet)
    {
        this.hsmSet = hsmSet;
    }

    /**
     * Flushes a set of files to nearline storage.
     *
     * @param hsm Name of nearline storage
     * @param files Files to flush
     * @param callback Callback notified for every file flushed
     */
    public void flush(String hsm,
                      Iterable<PnfsId> files,
                      CompletionHandler<Void, PnfsId> callback)
    {
        flushRequests.addAll(hsmSet.getNearlineStorageByType(hsm), files, callback);
    }

    /**
     * Stages a file from nearline storage.
     *
     * @param hsm Name of nearline storage
     * @param file Attributes of file to stage
     * @param callback Callback notified when file is staged
     */
    public void stage(String hsm,
                      FileAttributes file,
                      CompletionHandler<Void, PnfsId> callback)
    {
        stageRequests.addAll(hsmSet.getNearlineStorageByType(hsm), Collections.singleton(file), callback);
    }

    /**
     * Removes a set of files from nearline storage.
     *
     * @param hsm Name of nearline storage
     * @param files Files to remove
     * @param callback Callback notified for every file removed
     */
    public void remove(String hsm,
                       Iterable<URI> files,
                       CompletionHandler<Void, URI> callback)
    {
        removeRequests.addAll(hsmSet.getNearlineStorageByType(hsm), files, callback);
    }

    public int getActiveFetchJobs()
    {
        return 0;  //To change body of created methods use File | Settings | File Templates.
    }

    public int getFetchQueueSize()
    {
        return 0;  //To change body of created methods use File | Settings | File Templates.
    }

    public int getActiveStoreJobs()
    {
        return 0;  //To change body of created methods use File | Settings | File Templates.
    }

    public int getStoreQueueSize()
    {
        return 0;  //To change body of created methods use File | Settings | File Templates.
    }


    /**
     * Abstract base class for request implementations.
     *
     * Provides support for registering callbacks and deregistering from a RequestContainer
     * when the request has completed.
     */
    private abstract static class AbstractRequest<K>
    {
        private enum State { QUEUED, ACTIVE, CANCELED }

        private final List<CompletionHandler<Void,K>> callbacks = new ArrayList<>();
        protected final long createdAt = System.currentTimeMillis();
        protected final UUID uuid = UUID.randomUUID();
        protected final NearlineStorage storage;
        protected final AtomicReference<State> state = new AtomicReference<>(State.QUEUED);
        protected volatile long activatedAt;

        AbstractRequest(NearlineStorage storage)
        {
            this.storage = storage;
        }

        public UUID getId()
        {
            return uuid;
        }

        public void addCallback(CompletionHandler<Void,K> callback)
        {
            callbacks.add(callback);
        }

        public Iterable<CompletionHandler<Void,K>> callbacks()
        {
            return this.callbacks;
        }

        public synchronized ListenableFuture<Void> activate()
        {
            state.compareAndSet(State.QUEUED, State.ACTIVE);
            activatedAt = System.currentTimeMillis();
            return Futures.immediateFuture(null);
        }

        public void cancel()
        {
            state.set(State.CANCELED);
            storage.cancel(uuid);
        }

        @Override
        public String toString()
        {
            StringBuilder sb = new StringBuilder();
            sb.append(uuid).append(" ").append(state).append(" ").append(new Date(createdAt));
            long activatedAt = this.activatedAt;
            if (activatedAt > 0) {
                sb.append(" ").append(new Date(activatedAt));
            }
            return sb.toString();
        }
    }

    /**
     * A container for requests of a particular type. Subclassed for each request type.
     *
     * Supports thread safe addition and removal of requests from the container.
     *
     * @param <K> Key identifying a replica
     * @param <F> Information defining a replica
     * @param <R> Type of request
     */
    private abstract static class AbstractRequestContainer<K, F, R extends AbstractRequest<K> & NearlineRequest<?>>
    {
        private final Map<K, R> requests = new HashMap<>();

        public synchronized void addAll(NearlineStorage storage,
                                        Iterable<F> files,
                                        CompletionHandler<Void,K> callback)
        {
            List<R> newRequests = new ArrayList<>();
            for (F file : files) {
                K key = extractKey(file);
                R request = requests.get(key);
                if (request != null) {
                    request.addCallback(callback);
                } else {
                    try {
                        request = checkNotNull(createRequest(storage, file));
                    } catch (Exception e) {
                        callback.failed(e, key);
                        continue;
                    }
                    request.addCallback(callback);
                    requests.put(key, request);
                    newRequests.add(request);
                }
            }
            submit(storage, newRequests);
        }

        public synchronized void cancel(K key)
        {
            R request = requests.get(key);
            if (request != null) {
                request.cancel();
            }
        }

        protected void removeAndCallback(K key, Throwable cause)
        {
            for (CompletionHandler<Void,K> callback : remove(key)) {
                if (cause != null) {
                    callback.failed(cause, key);
                } else {
                    callback.completed(null, key);
                }
            }
        }

        public synchronized String printJobQueue()
        {
            return Joiner.on('\n').join(requests.values());
        }

        private synchronized Iterable<CompletionHandler<Void,K>> remove(K key)
        {
            R actualRequest = requests.remove(key);
            return (actualRequest != null) ? actualRequest.callbacks() : Collections.<CompletionHandler<Void,K>>emptyList();
        }

        /** Returns a key identifying a particular replica. */
        protected abstract K extractKey(F file);

        /** Creates a new nearline storage request. */
        protected abstract R createRequest(NearlineStorage storage, F file) throws Exception;

        /** Submits requests to the nearline storage. */
        protected abstract void submit(NearlineStorage storage, Iterable<R> requests);
    }

    private class FlushRequestContainer extends AbstractRequestContainer<PnfsId, PnfsId, FlushRequestImpl>
    {
        private final Function<FlushRequestImpl, FlushRequest> TO_FLUSH_REQUEST = cast();

        @Override
        protected PnfsId extractKey(PnfsId id)
        {
            return id;
        }

        @Override
        protected FlushRequestImpl createRequest(NearlineStorage storage, PnfsId id)
                throws CacheException, InterruptedException
        {
            return new FlushRequestImpl(storage, id);
        }

        @Override
        protected void submit(NearlineStorage storage, Iterable<FlushRequestImpl> requests)
        {
            storage.flush(transform(requests, TO_FLUSH_REQUEST));
        }
    }

    private class StageRequestContainer extends AbstractRequestContainer<PnfsId, FileAttributes, StageRequestImpl>
    {
        private final Function<StageRequestImpl, StageRequest> TO_STAGE_REQUEST = cast();

        @Override
        protected PnfsId extractKey(FileAttributes file)
        {
            return file.getPnfsId();
        }

        @Override
        protected StageRequestImpl createRequest(NearlineStorage storage,
                                                 FileAttributes file)
                throws FileInCacheException
        {
            return new StageRequestImpl(storage, file);
        }

        @Override
        protected void submit(NearlineStorage storage, Iterable<StageRequestImpl> requests)
        {
            storage.stage(transform(requests, TO_STAGE_REQUEST));
        }
    }

    private class RemoveRequestContainer extends AbstractRequestContainer<URI, URI, RemoveRequestImpl>
    {
        private final Function<RemoveRequestImpl, RemoveRequest> TO_REMOVE_REQUEST = cast();

        @Override
        protected URI extractKey(URI uri)
        {
            return uri;
        }

        @Override
        protected RemoveRequestImpl createRequest(NearlineStorage storage, URI uri)
        {
            return new RemoveRequestImpl(storage, uri);
        }

        @Override
        protected void submit(NearlineStorage storage, Iterable<RemoveRequestImpl> requests)
        {
            storage.remove(transform(requests, TO_REMOVE_REQUEST));
        }
    }

    private class FlushRequestImpl extends AbstractRequest<PnfsId> implements FlushRequest
    {
        private final ReplicaDescriptor descriptor;
        private final StorageInfoMessage infoMsg;

        public FlushRequestImpl(NearlineStorage nearlineStorage, PnfsId pnfsId) throws CacheException, InterruptedException
        {
            super(nearlineStorage);
            infoMsg = new StorageInfoMessage(getCellAddress().toString(), pnfsId, false);
            descriptor = repository.openEntry(pnfsId, NO_FLAGS);
        }

        @Override
        public File getFile()
        {
            return descriptor.getFile();
        }

        @Override
        public FileAttributes getFileAttributes()
        {
            return descriptor.getFileAttributes();
        }

        @Override
        public long getDeadline()
        {
            return Long.MAX_VALUE;
        }

        @Override
        public ListenableFuture<Void> activate()
        {
            super.activate();
            return executor.submit(new Callable<Void>()
            {
                @Override
                public Void call()
                        throws CacheException, InterruptedException, IOException,
                               NoSuchAlgorithmException
                {
                    PnfsId pnfsId = descriptor.getFileAttributes().getPnfsId();
                    LOGGER.debug("Checking if {} still exists", pnfsId);
                    try {
                        pnfs.getFileAttributes(pnfsId, EnumSet.noneOf(FileAttribute.class));
                    } catch (FileNotFoundCacheException e) {
                        try {
                            repository.setState(pnfsId, EntryState.REMOVED);
                            LOGGER.info("File not found in name space; removed {}", pnfsId);
                        } catch (CacheException | InterruptedException | IllegalTransitionException f) {
                            LOGGER.error("File not found in name space, but failed to remove {}: {}", pnfsId, f);
                        }
                        throw e;
                    }
                    checksumModule.enforcePreFlushPolicy(descriptor);
                    return null;
                }
            });
        }

        @Override
        public String toString()
        {
            return super.toString() + " " + getFileAttributes().getPnfsId();
        }

        @Override
        public void failed(Throwable cause)
        {
            /* ListenableFuture#get throws ExecutionException */
            if (cause instanceof ExecutionException) {
                cause = cause.getCause();
            }
            descriptor.close();
            done(cause);
        }

        @Override
        public void completed(Set<URI> uris)
        {
            try {
                FileAttributes fileAttributesForNotification;
                try {
                    fileAttributesForNotification = getFileAttributesForNotification(uris);
                } finally {
                    descriptor.close();
                }

                PnfsId pnfsId = getFileAttributes().getPnfsId();

                infoMsg.setStorageInfo(fileAttributesForNotification.getStorageInfo());
                LOGGER.debug("{}: added HSM locations {}", pnfsId, uris);

                notifyNamespace(fileAttributesForNotification);
                notifyFlushMessageTarget(fileAttributesForNotification);

                LOGGER.info("File successfully stored to tape");

                repository.setState(pnfsId, EntryState.CACHED);

                done(null);
            } catch (IllegalTransitionException e) {
                /* Apparently the file is no longer precious. Most
                 * likely it got deleted, which is fine, since the
                 * flush already succeeded.
                 */
                done(null);
            } catch (Exception e) {
                done(e);
            }
        }

        private FileAttributes getFileAttributesForNotification(Set<URI> uris) throws CacheException
        {
            FileAttributes fileAttributes = descriptor.getFileAttributes();
            StorageInfo storageInfo = fileAttributes.getStorageInfo().clone();
            for (URI uri : uris) {
                try {
                    HsmLocationExtractorFactory.validate(uri);
                    storageInfo.addLocation(uri);
                    storageInfo.isSetAddLocation(true);
                } catch (IllegalArgumentException e) {
                    throw new CacheException(2, e.getMessage(), e);
                }
            }
            FileAttributes fileAttributesForNotification = new FileAttributes();
            fileAttributesForNotification.setAccessLatency(fileAttributes.getAccessLatency());
            fileAttributesForNotification.setRetentionPolicy(fileAttributes.getRetentionPolicy());
            fileAttributesForNotification.setStorageInfo(storageInfo);
            fileAttributesForNotification.setSize(fileAttributes.getSize());
            return fileAttributesForNotification;
        }

        private void notifyNamespace(FileAttributes fileAttributes)
                throws InterruptedException
        {
            while (true) {
                try {
                    pnfs.fileFlushed(getFileAttributes().getPnfsId(), fileAttributes);
                    break;
                } catch (CacheException e) {
                    if (e.getRc() == CacheException.FILE_NOT_FOUND ||
                            e.getRc() == CacheException.NOT_IN_TRASH) {
                            /* In case the file was deleted, we are presented
                             * with the problem that the file is now on tape,
                             * however the location has not been registered
                             * centrally. Hence the copy on tape will not be
                             * removed by the HSM cleaner. The sensible thing
                             * seems to be to remove the file from tape here.
                             * For now we ignore this issue (REVISIT).
                             */
                        break;
                    }

                    /* The message to the PnfsManager failed. There are several
                     * possible reasons for this; we may have lost the
                     * connection to the PnfsManager; the PnfsManager may have
                     * lost its connection to PNFS or otherwise be in trouble;
                     * bugs; etc.
                     *
                     * We keep retrying until we succeed. This will effectively
                     * block this thread from flushing any other files, which
                     * seems sensible when we have trouble talking to the
                     * PnfsManager. If the pool crashes or gets restarted while
                     * waiting here, we will end up flushing the file again. We
                     * assume that the nearline storage is able to eliminate the
                     * duplicate; or at least tolerate the duplicate (given that
                     * this situation should be rare, we can live with a little
                     * bit of wasted tape).
                     */
                    LOGGER.error("Error notifying PNFS about a flushed file: {} ({})",
                            e.getMessage(), e.getRc());
                }
                TimeUnit.MINUTES.sleep(2);
            }
        }

        private void notifyFlushMessageTarget(FileAttributes fileAttributes)
        {
            try {
                PoolFileFlushedMessage poolFileFlushedMessage =
                        new PoolFileFlushedMessage(getCellName(), fileAttributes.getPnfsId(), fileAttributes);
                poolFileFlushedMessage.setReplyRequired(false);
                flushMessageTarget.send(poolFileFlushedMessage);
            } catch (NoRouteToCellException e) {
                LOGGER.info("Failed to send message to {}: {}",
                        flushMessageTarget.getDestinationPath(), e.getMessage());
            }
        }

        protected void done(Throwable cause)
        {
            try {
                infoMsg.setTransferTime(System.currentTimeMillis() - activatedAt);
                infoMsg.setFileSize(getFileAttributes().getSize());
                infoMsg.setTimeQueued(activatedAt - createdAt);

                if (cause instanceof CacheException) {
                    infoMsg.setResult(((CacheException) cause).getRc(), cause.getMessage());
                } else {
                    infoMsg.setResult(666, cause.getMessage());
                }

                billingStub.send(infoMsg);
            } catch (NoRouteToCellException e) {
                LOGGER.error("Failed to send message to billing: {}", e.getMessage());
            }
            flushRequests.removeAndCallback(getFileAttributes().getPnfsId(), cause);
        }
    }

    private class StageRequestImpl extends AbstractRequest<PnfsId> implements StageRequest
    {
        private final StorageInfoMessage infoMsg;
        private ReplicaDescriptor descriptor;

        public StageRequestImpl(NearlineStorage storage, FileAttributes fileAttributes) throws FileInCacheException
        {
            super(storage);
            infoMsg = new StorageInfoMessage(getCellAddress().toString(), fileAttributes.getPnfsId(), true);
            infoMsg.setStorageInfo(fileAttributes.getStorageInfo());
            infoMsg.setFileSize(fileAttributes.getSize());
            descriptor =
                    repository.createEntry(
                            fileAttributes,
                            EntryState.FROM_STORE,
                            EntryState.CACHED,
                            Collections.<StickyRecord>emptyList(),
                            EnumSet.noneOf(Repository.OpenFlags.class));
        }

        public ListenableFuture<Void> allocate()
        {
            return executor.submit(
                    new Callable<Void>()
                    {
                        @Override
                        public Void call()
                                throws InterruptedException
                        {
                            descriptor.allocate(descriptor.getFileAttributes().getSize());
                            return null;
                        }
                    });
        }

        public File getFile()
        {
            return descriptor.getFile();
        }

        public FileAttributes getFileAttributes()
        {
            return descriptor.getFileAttributes();
        }

        public long getDeadline()
        {
            return Long.MAX_VALUE;
        }

        public void failed(Throwable cause)
        {
            done(cause);
        }

        public void completed(Set<Checksum> checksums)
        {
            Throwable error = null;
            try {
                if (checksumModule.hasPolicy(ChecksumModule.PolicyFlag.GET_CRC_FROM_HSM)) {
                    LOGGER.info("Obtained checksums {} for {} from HSM", checksums, getFileAttributes().getPnfsId());
                    descriptor.addChecksums(checksums);
                }
                checksumModule.enforcePostRestorePolicy(descriptor);
                descriptor.commit();
            } catch (InterruptedException | CacheException | RuntimeException | Error e) {
                error = e;
            } catch (NoSuchAlgorithmException e) {
                error = new CacheException(1010, "Checksum calculation failed: " + e.getMessage(), e);
            } catch (IOException e) {
                error = new DiskErrorCacheException("Checksum calculation failed due to I/O error: " + e.getMessage(), e);
            } finally {
                done(error);
            }
        }

        protected void done(Throwable cause)
        {
            descriptor.close();
            if (cause instanceof CacheException) {
                infoMsg.setResult(((CacheException) cause).getRc(), cause.getMessage());
            } else if (cause != null) {
                infoMsg.setResult(44, cause.toString());
            }
            infoMsg.setTransferTime(System.currentTimeMillis() - activatedAt);
            try {
                billingStub.send(infoMsg);
            } catch (NoRouteToCellException e) {
                LOGGER.error("Failed to send message to billing: {}", e.getMessage());
            }
            stageRequests.removeAndCallback(getFileAttributes().getPnfsId(), cause);
        }

        @Override
        public String toString()
        {
            return super.toString() + " " + getFileAttributes().getPnfsId();
        }
    }

    private class RemoveRequestImpl extends AbstractRequest<URI> implements RemoveRequest
    {
        private final URI uri;

        RemoveRequestImpl(NearlineStorage storage, URI uri)
        {
            super(storage);
            this.uri = uri;
        }

        public URI getUri()
        {
            return uri;
        }

        public long getDeadline()
        {
            return Long.MAX_VALUE;
        }

        public void failed(Throwable cause)
        {
            done(cause);
        }

        public void completed(Void result)
        {
            done(null);
        }

        private void done(Throwable cause)
        {
            removeRequests.removeAndCallback(uri, cause);
        }

        @Override
        public String toString()
        {
            return super.toString() + " " + uri;
        }
    }

    @Command(name = "rh set timeout",
            hint = "set restore timeout",
            usage = "Set restore timeout for the HSM script. When the timeout expires " +
                    "the HSM script is killed.")
    class RestoreSetTimeoutCommand implements Callable<String>
    {
        @Argument(metaVar = "seconds")
        long timeout;

        @Override
        public String call()
        {
            synchronized (NearlineStorageHandler.this) {
                _maxRestoreRun = TimeUnit.SECONDS.toMillis(timeout);
            }
            return "";
        }
    }

    @Command(name = "rh kill",
            hint = "kill restore request",
            usage = "Remove an HSM restore request.")
    class RestoreKillCommand implements Callable<String>
    {
        @Argument
        PnfsId pnfsId;

        @Override
        public String call() throws NoSuchElementException, IllegalStateException
        {
            stageRequests.cancel(pnfsId);
            return "Kill initialized";
        }
    }

    @Command(name = "rh ls",
            hint = "list restore queue",
            usage = "List the HSM requests on the restore queue.\n\n" +
                    "The columns in the output show: job id, job status, pnfs id, request counter, " +
                    "and request submission time.")
    class RestoreListCommand implements Callable<String>
    {
        @Override
        public String call()
        {
            return stageRequests.printJobQueue();
        }
    }

    @Command(name = "st set timeout",
            hint = "set store timeout",
            usage = "Set store timeout for the HSM script. When the timeout expires " +
                    "the HSM script is killed.")
    class StoreSetTimeoutCommand implements Callable<String>
    {
        @Argument(metaVar = "seconds")
        long timeout;

        @Override
        public String call()
        {
            synchronized (NearlineStorageHandler.this) {
                _maxStoreRun = TimeUnit.SECONDS.toMillis(timeout);
            }
            return "";
        }
    }

    @Command(name = "st kill",
            hint = "kill store request",
            usage = "Remove an HSM store request.")
    class StoreKillCommand implements Callable<String>
    {
        @Argument
        PnfsId pnfsId;

        @Override
        public String call() throws NoSuchElementException, IllegalStateException
        {
            flushRequests.cancel(pnfsId);
            return "Kill initialized";
        }
    }

    @Command(name = "st ls",
            hint = "list store queue",
            usage = "List the HSM requests on the store queue.\n\n" +
                    "The columns in the output show: job id, job status, pnfs id, request counter, " +
                    "and request submission time.")
    class StoreListCommand implements Callable<String>
    {
        @Override
        public String call()
        {
            return flushRequests.printJobQueue();
        }
    }

    @Command(name = "rm set timeout",
            hint = "set tape remove timeout",
            usage = "Set remove timeout for the HSM script. When the timeout expires " +
                    "the HSM script is killed.")
    class RemoveSetTimeoutCommand implements Callable<String>
    {
        @Argument(metaVar = "seconds")
        long timeout;

        @Override
        public String call()
        {
            synchronized (NearlineStorageHandler.this) {
                _maxRemoveRun = TimeUnit.SECONDS.toMillis(timeout);
            }
            return "";
        }
    }

    @Command(name = "rm ls",
            hint = "list store queue",
            usage = "List the HSM requests on the remove queue.\n\n" +
                    "The columns in the output show: job id, job status, pnfs id, request counter, " +
                    "and request submission time.")
    class RemoveListCommand implements Callable<String>
    {
        @Override
        public String call()
        {
            return removeRequests.printJobQueue();
        }
    }

    @Command(name = "rh restore",
            hint = "restore file from tape",
            usage = "Restore a file from tape.")
    class RestoreCommand extends DelayedReply implements Callable<Serializable>, CompletionHandler<Void, PnfsId>
    {
        @Argument
        PnfsId pnfsId;

        @Option(name = "block",
                usage = "Block the shell until the restore has completed. This " +
                        "option is only relevant when debugging as the shell " +
                        "would usually time out before a real HSM is able to " +
                        "restore a file.")
        boolean block;

        @Override
        public void completed(Void result, PnfsId pnfsId)
        {
            reply("Fetched " + pnfsId);
        }

        @Override
        public void failed(Throwable exc, PnfsId pnfsId)
        {
            reply("Failed to fetch " + pnfsId + ": " + exc);
        }

        @Override
        public Serializable call()
        {
            /* We need to fetch the storage info and we don't want to
             * block the message thread while waiting for the reply.
             */
            Thread t = new Thread("rh restore") {
                @Override
                public void run() {
                    try {
                        FileAttributes attributes = pnfs.getStorageInfoByPnfsId(pnfsId).getFileAttributes();
                        stage(hsm, attributes, block ? this : new NopCompletionHandler<>());
                    } catch (CacheException e) {
                        failed(e, pnfsId);
                    }
                }
            };
            t.start();

            return block ? this : "Fetch request queued";
        }
    }

    private static <B extends A, A> Function<B,A> cast()
    {
        return new Function<B, A>()
        {
            @Override
            public A apply(B element)
            {
                return element;
            }
        };
    }
}
