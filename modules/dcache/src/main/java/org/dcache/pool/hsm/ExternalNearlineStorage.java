package org.dcache.pool.hsm;

import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.HsmRunSystem;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.StorageInfo;
import diskCacheV111.vehicles.StorageInfos;

import org.dcache.commons.util.NDC;
import org.dcache.util.Checksum;
import org.dcache.vehicles.FileAttributes;

import static com.google.common.base.Predicates.in;
import static com.google.common.base.Predicates.not;
import static java.util.Arrays.asList;

public class ExternalNearlineStorage extends AbstractBlockingNearlineStorage
{
    private static final Logger LOGGER =
            LoggerFactory.getLogger(ExternalNearlineStorage.class);

    private static final int MAX_LINES = 200;
    public static final String COMMAND = "command";
    public static final String CONCURRENT_PUTS = "c:puts";
    public static final String CONCURRENT_GETS = "c:gets";
    public static final String CONCURRENT_REMOVES = "c:removes";
    private static final int DEFAULT_FLUSH_THREADS = 100;
    private static final int DEFAULT_STAGE_THREADS = 100;
    private static final int DEFAULT_REMOVE_THREADS = 100;
    private static final Collection<String> PROPERTIES = asList(COMMAND, CONCURRENT_PUTS, CONCURRENT_GETS, CONCURRENT_REMOVES);

    private final ThreadPoolExecutor flushExecutor =
            new ThreadPoolExecutor(DEFAULT_FLUSH_THREADS, DEFAULT_FLUSH_THREADS, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
    private final ThreadPoolExecutor stageExecutor =
            new ThreadPoolExecutor(DEFAULT_STAGE_THREADS, DEFAULT_STAGE_THREADS, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
    private final ThreadPoolExecutor removeExecutor =
            new ThreadPoolExecutor(DEFAULT_REMOVE_THREADS, DEFAULT_REMOVE_THREADS, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
    private final String type;
    private final String name;

    private long _maxStoreRun = TimeUnit.HOURS.toMillis(4);
    private long _maxRestoreRun = TimeUnit.HOURS.toMillis(4);
    private long _maxRemoveRun = TimeUnit.HOURS.toMillis(4);

    private volatile String command;

    public ExternalNearlineStorage(String type, String name)
    {
        this.type = type;
        this.name = name;
    }

    private class FetchTask extends Task<Set<Checksum>, StageRequest>
    {
        public FetchTask(StageRequest request)
        {
            super(request);
        }

        private String getCommand(File file, FileAttributes attributes)
        {
            StorageInfo storageInfo = StorageInfos.extractFrom(attributes);
            StringBuilder sb = new StringBuilder(command);
            sb.append(" -si=").append(storageInfo.toString());
            for (URI location: storageInfo.locations()) {
                if (location.getScheme().equals(type) && location.getAuthority().equals(name)) {
                    sb.append(" -uri=").append(location.toString());
                }
            }
            sb.append(" get").append(" ").
                    append(attributes.getPnfsId()).append(" ").
                    append(file.getPath());
            LOGGER.debug("COMMAND: {}", sb);
            return sb.toString();
        }

        @Override
        public Set<Checksum> call() throws Exception
        {
            FileAttributes attributes = request.getFileAttributes();
            NDC.push(attributes.getPnfsId().toString());
            try {
                LOGGER.trace("Fetch thread started");
                String fetchCommand = getCommand(request.getFile(), attributes);
                request.allocate().get();
                new HsmRunSystem(fetchCommand, MAX_LINES, _maxRestoreRun).execute();
                Set<Checksum> checksums = readChecksumFromHsm();
                LOGGER.info("File successfully restored from tape");
                return checksums;
            } catch (CacheException e) {
                LOGGER.error(e.toString());
                throw e;
            } catch (IOException e) {
                LOGGER.error("Process got an I/O error: {}", e.toString());
                throw new CacheException(2, "I/O Error: " + e.getMessage(), e);
            } catch (IllegalThreadStateException  e) {
                LOGGER.error("Cannot stop process: {}", e.toString());
                throw new CacheException(3, e.getMessage(), e);
            } finally {
                NDC.pop();
            }
        }

        private Set<Checksum> readChecksumFromHsm()
                throws IOException
        {
            File file = new File(request.getFile().getCanonicalPath() + ".crcval");
            try {
                if (file.exists()) {
                    try {
                        String firstLine = Files.readFirstLine(file, Charsets.US_ASCII);
                        if (firstLine != null) {
                            Checksum checksum = Checksum.parseChecksum("1:" + firstLine);
                            return Collections.singleton(checksum);
                        }
                    } finally {
                        file.delete();
                    }
                }
            } catch (FileNotFoundException e) {
                /* Should not happen unless somebody else is removing
                 * the file before we got a chance to read it.
                 */
                throw Throwables.propagate(e);
            }
            return Collections.emptySet();
        }
    }

    private class StoreTask extends Task<Set<URI>, FlushRequest>
    {
        public StoreTask(FlushRequest request)
        {
            super(request);
        }

        private String getCommand(File file, FileAttributes fileAttributes)
        {
            StorageInfo storageInfo = StorageInfos.extractFrom(fileAttributes);
            StringBuilder sb = new StringBuilder(command);
            sb.append(" -si=").append(storageInfo.toString());
            sb.append(" put").append(" ").
                    append(fileAttributes.getPnfsId()).append(" ").
                    append(file.getPath());
            LOGGER.debug("COMMAND: {}", sb);
            return sb.toString();
        }

        @Override
        public Set<URI> call() throws Exception
        {
            FileAttributes fileAttributes = request.getFileAttributes();
            PnfsId pnfsId = fileAttributes.getPnfsId();
            NDC.push(pnfsId.toString());
            try {
                Set<URI> locations = new HashSet<>();
                String storeCommand = getCommand(request.getFile(), request.getFileAttributes());
                String output = new HsmRunSystem(storeCommand, MAX_LINES, _maxStoreRun).execute();
                for (String uri : Splitter.on("\n").trimResults().omitEmptyStrings().split(output)) {
                    try {
                        locations.add(new URI(uri));
                    } catch (URISyntaxException e) {
                        LOGGER.error("HSM script produced BAD URI: {}", uri);
                        throw new CacheException(2, e.getMessage(), e);
                    }
                }

                return locations;
            } catch (CacheException e) {
                LOGGER.error("Error while flushing to tape: {}", e.toString());
                throw e;
            } catch (IOException e) {
                LOGGER.error("Process got an I/O error: {}", e.toString());
                throw new CacheException(2, "I/O Error: " + e.getMessage(), e);
            } catch (IllegalThreadStateException e) {
                LOGGER.error("Cannot stop process: {}", e.toString());
                throw new CacheException(3, e.getMessage(), e);
            } finally {
                NDC.pop();
            }
        }
    }

    private class RemoveTask extends Task<Void, RemoveRequest>
    {
        RemoveTask(RemoveRequest request)
        {
            super(request);
        }

        private String getCommand(URI uri)
        {
            return command + " -uri=" + uri + " remove";
        }

        @Override
        public Void call() throws Exception
        {
            try {
                new HsmRunSystem(getCommand(request.getUri()), MAX_LINES, _maxRemoveRun).execute();
                return null;
            } catch (CancellationException e) {
                /* Somebody cancelled the future, even though we are the
                 * only one holding a reference to it. Must be a bug.
                 */
                throw new RuntimeException("Bug: ExternalTask was unexpectedly cancelled", e);
            }
        }
    }

    @Override
    public void flush(Iterable<FlushRequest> requests)
    {
        for (FlushRequest request : requests) {
            flushExecutor.execute(new StoreTask(request));
        }
    }

    @Override
    public void stage(Iterable<StageRequest> requests)
    {
        for (StageRequest request : requests) {
            stageExecutor.submit(new FetchTask(request));
        }
    }

    @Override
    public void remove(Iterable<RemoveRequest> requests)
    {
        for (RemoveRequest request : requests) {
            removeExecutor.submit(new RemoveTask(request));
        }
    }

    @Override
    public synchronized void configure(Map<String, String> properties)
    {
        if (!properties.containsKey(COMMAND)) {
            throw new IllegalArgumentException("command option must be defined");
        }
        command = buildCommand(properties);

        configureThreadPoolSize(flushExecutor, properties.get(CONCURRENT_PUTS), 1);
        configureThreadPoolSize(stageExecutor, properties.get(CONCURRENT_GETS), 1);
        configureThreadPoolSize(removeExecutor, properties.get(CONCURRENT_REMOVES), 1);
    }

    private void configureThreadPoolSize(ThreadPoolExecutor executor, String configuration, int defaultValue)
    {
        int n = (configuration != null) ? Integer.parseInt(configuration) : defaultValue;
        executor.setMaximumPoolSize(n);
        executor.setCorePoolSize(n);
    }

    private String buildCommand(Map<String, String> properties)
    {
        StringBuilder command = new StringBuilder();
        command.append(properties.get(COMMAND)).append(" ");
        for (Map.Entry<String,String> attr : Maps.filterKeys(properties, not(in(PROPERTIES))).entrySet()) {
            String key = attr.getKey();
            String val = attr.getValue();
            command.append(" -").append(key);
            if (!Strings.isNullOrEmpty(val)) {
                command.append("=").append(val);
            }
        }
        return command.toString();
    }

    @Override
    public void shutdown()
    {
        super.shutdown();
        flushExecutor.shutdown();
        stageExecutor.shutdown();
        removeExecutor.shutdown();
    }
}
