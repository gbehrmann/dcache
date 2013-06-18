package diskCacheV111.util ;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

import javax.annotation.PreDestroy;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import dmg.util.Args;
import dmg.util.Formats;

import org.dcache.cells.CellCommandListener;
import org.dcache.cells.CellSetupProvider;
import org.dcache.pool.hsm.NearlineStorage;
import org.dcache.pool.hsm.NearlineStorageProvider;

import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.unmodifiableIterable;
import static java.util.Collections.unmodifiableSet;

/**
 * An HsmSet encapsulates information about attached HSMs. The HsmSet
 * also acts as a cell command interpreter, allowing the user to
 * add, remove or alter the information.
 *
 * Each HSM has a case sensitive instance name which uniquely
 * identifies this particular tape system throughout dCache. Notice
 * that multiple pools can be attached to the same HSM. In that case
 * the instance name must be the same at each pool.
 *
 * An HSM also has a type, e.g. OSM or Enstore. The type is not case
 * sensitive.  Traditionally, the type was called the HSM name. It is
 * important not to confuse the type with the instance name.
 *
 * Earlier versions of dCache did not specify an instance name. For
 * compatibility, the type may serve as an instance name.
 */
public class HsmSet
    implements CellCommandListener,
               CellSetupProvider
{
    private static final ServiceLoader<NearlineStorageProvider> PROVIDERS =
            ServiceLoader.load(NearlineStorageProvider.class);
    private static final String DEFAULT_PROVIDER = "external";

    private final ConcurrentMap<String, HsmInfo> _hsm = Maps.newConcurrentMap();

    private NearlineStorageProvider findProvider(String name)
    {
        for (NearlineStorageProvider provider : PROVIDERS) {
            if (provider.getName().equals(provider)) {
                return provider;
            }
        }
        throw new IllegalArgumentException("No such nearline storage provider: " + name);
    }

    /**
     * Information about a particular HSM instance.
     */
    public class HsmInfo
    {
        private final String _type;
        private final String _instance;
        private final Map<String,String> _attr = new HashMap<>();
        private final NearlineStorageProvider _provider;
        private NearlineStorage _nearlineStorage;

        /**
         * Constructs an HsmInfo object.
         *
         * @param instance A unique instance name.
         * @param type     The HSM type, e.g. OSM or enstore.
         */
        public HsmInfo(String instance, String type, String provider)
        {
            _instance = instance;
            _type = type.toLowerCase();
            _provider = findProvider(provider);
        }

        /**
         * Returns the instance name.
         */
        public String getInstance()
        {
            return _instance;
        }

        /**
         * Returns the HSM type (a.k.a. HSM name).
         */
        public String getType()
        {
            return _type;
        }

        /**
         * Returns the value of an attribute. Returns null if the
         * attribute has not been defined.
         *
         * @param attribute An attribute name
         */
        public synchronized String getAttribute(String attribute)
        {
           return _attr.get(attribute);
        }

        /**
         * Removes an attribute.
         *
         * @param attribute An attribute name
         */
        public synchronized void unsetAttribute(String attribute)
        {
            _attr.remove(attribute);
            if (_nearlineStorage != null) {
                _nearlineStorage.configure(_attr);
            }
        }

        /**
         * Sets an attribute to a value.
         *
         * @param attribute An attribute name
         * @param value     A value string
         */
        public synchronized void setAttribute(String attribute, String value)
        {
           _attr.put(attribute, value);
            if (_nearlineStorage != null) {
                _nearlineStorage.configure(_attr);
            }
        }

        /**
         * Returns the set of attributes.
         */
        public synchronized Iterable<Map.Entry<String, String>> attributes()
        {
            return new ArrayList<>(_attr.entrySet());
        }

        public synchronized NearlineStorage getNearlineStorage()
        {
            if (_nearlineStorage == null) {
                _nearlineStorage = _provider.createNearlineStorage(_type, _instance);
                _nearlineStorage.configure(_attr);
            }
            return _nearlineStorage;
        }

        public synchronized void shutdown()
        {
            if (_nearlineStorage != null) {
                _nearlineStorage.shutdown();
            }
        }
    }

    /**
     * Returns the set of HSMs.
     *
     * Notice that the set returned does not implement Serializable.
     */
    public Set<String> getHsmInstances()
    {
        return unmodifiableSet(_hsm.keySet());
    }

    /**
     * Returns information about the named HSM. Return null if no HSM
     * with this instance name was defined.
     *
     * @param instance An HSM instance name.
     */
    public HsmInfo getHsmInfoByName(String instance)
    {
       return _hsm.get(instance);
    }


    /**
     * Returns all HSMs of a given type.
     *
     * @param type An HSM type name.
     */
    public Iterable<HsmInfo> getHsmInfoByType(final String type)
    {
        return unmodifiableIterable(filter(_hsm.values(),
                new Predicate<HsmInfo>()
                {
                    @Override
                    public boolean apply(HsmInfo hsm)
                    {
                        return hsm.getType().equals(type);
                    }
                }));
    }

    public NearlineStorage getNearlineStorageByType(String type)
    {
        HsmInfo info = Iterables.getFirst(getHsmInfoByType(type), null);
        return (info != null) ? info.getNearlineStorage() : null;
    }

    /**
     * Removes any information about the named HSM.
     *
     * @param instance An HSM instance name.
     */
    private void removeInfo(String instance)
    {
        HsmInfo info = _hsm.remove(instance);
        if (info != null) {
            info.shutdown();
        }
    }

    /**
     * Scans an argument set for options and applies those as
     * attributes to an HsmInfo object.
     */
    private void scanOptions(HsmInfo info, Args args)
    {
        for (Map.Entry<String,String> e: args.options().entries()) {
            String optName  = e.getKey();
            String optValue = e.getValue();

            info.setAttribute(optName, optValue == null ? "" : optValue);
        }
    }

    /**
     * Scans an argument set for options and removes and unsets those
     * attributes in the given HsmInfo object.
     */
    private void scanOptionsUnset(HsmInfo info, Args args)
    {
        for (String optName: args.options().keySet()) {
            info.unsetAttribute(optName);
        }
    }

    public static final String hh_hsm_create = "<type> [<name> [<provider>]] [-<key>=<value>] ...";
    public String ac_hsm_create_$_1_3(Args args)
    {
        String type = args.argv(0);
        String instance = (args.argc() == 1) ? type : args.argv(1);
        String provider = (args.argc() == 3) ? args.argv(2) : DEFAULT_PROVIDER;
        HsmInfo info = new HsmInfo(instance, type, provider);
        scanOptions(info, args);
        if (_hsm.putIfAbsent(instance, info) != info) {
            throw new IllegalArgumentException("Nearline storage already exists: " + instance);
        }
        return "";
    }

    public static final String hh_hsm_set = "<name> [-<key>=<value>] ...";
    public String ac_hsm_set_$_1_3(Args args)
    {
        String instance = args.argv(0);
        HsmInfo info = getHsmInfoByName(instance);
        if (info == null) {
            throw new IllegalArgumentException("No such nearline storage: " + instance);
        }
        scanOptions(info, args);
        return "";
    }

    public static final String hh_hsm_unset = "<name> [-<key>] ...";
    public String ac_hsm_unset_$_1(Args args)
    {
       String instance = args.argv(0);
       HsmInfo info = getHsmInfoByName(instance);
       if (info == null) {
           throw new IllegalArgumentException("No such nearline storage: " + instance);
       }
       scanOptionsUnset(info, args);
       return "";
    }

    public static final String hh_hsm_ls = "[<name>] ...";
    public String ac_hsm_ls_$_0_99(Args args)
    {
       StringBuilder sb = new StringBuilder();
       if (args.argc() > 0) {
          for (int i = 0; i < args.argc(); i++) {
             printInfos(sb, args.argv(i));
          }
       } else {
           for (String name : _hsm.keySet()) {
               printInfos(sb, name);
           }
       }
       return sb.toString();
    }

    public static final String hh_hsm_remove = "<name>";
    public String ac_hsm_remove_$_1(Args args)
    {
       removeInfo(args.argv(0));
       return "";
    }

    // hsm ls -l

    @Override
    public void printSetup(PrintWriter pw)
    {
        for (HsmInfo info : _hsm.values()) {
            for (Map.Entry<String,String> entry : info.attributes()) {
                pw.print("hsm set ");
                pw.print(info.getType());
                pw.print(" ");
                pw.print(info.getInstance());
                pw.print(" -");
                pw.print(entry.getKey());
                pw.print("=");
                pw.println(entry.getValue() == null ? "-" : entry.getValue());
            }
        }
    }

    @Override
    public void beforeSetup() {}

    @Override
    public void afterSetup() {}

    @PreDestroy
    public void shutdown()
    {
        for (HsmInfo info : _hsm.values()) {
            info.shutdown();
        }
    }

    private void printInfos(StringBuilder sb, String instance)
    {
        assert instance != null;

        HsmInfo info = getHsmInfoByName(instance);
        if (info == null) {
            sb.append(instance).append(" not found\n");
        } else {
            sb.append(instance).append("(").append(info.getType())
                    .append(")\n");
            for (Map.Entry<String,String> entry : info.attributes()) {
                String attrName  = entry.getKey();
                String attrValue = entry.getValue();
                sb.append("   ").
                    append(Formats.field(attrName,20,Formats.LEFT)).
                    append(attrValue == null ? "<set>" : attrValue).
                    append("\n");
            }
        }
    }
}
