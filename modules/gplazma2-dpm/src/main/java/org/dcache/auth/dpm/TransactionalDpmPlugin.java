package org.dcache.auth.dpm;

import liquibase.Liquibase;
import liquibase.changelog.ChangeSet;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.LiquibaseException;
import liquibase.exception.MigrationFailedException;
import liquibase.resource.ClassLoaderResourceAccessor;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.orm.jdo.JdoTransactionManager;
import org.springframework.orm.jdo.TransactionAwarePersistenceManagerFactoryProxy;
import org.springframework.transaction.TransactionStatus;

import javax.jdo.JDOHelper;
import javax.jdo.PersistenceManagerFactory;

import java.security.Principal;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.dcache.auth.dpm.transaction.FallibleTransactionCallback;
import org.dcache.auth.dpm.transaction.FallibleTransactionCallbackWithoutResult;
import org.dcache.auth.dpm.transaction.FallibleTransactionTemplate;
import org.dcache.gplazma.AuthenticationException;
import org.dcache.gplazma.NoSuchPrincipalException;
import org.dcache.gplazma.plugins.GPlazmaIdentityPlugin;
import org.dcache.gplazma.plugins.GPlazmaMappingPlugin;
import org.dcache.gplazma.plugins.GPlazmaSessionPlugin;

/**
 * Decorator for DpmPlugin.
 *
 * Provides transaction demarcation and database initialization.
 */
public class TransactionalDpmPlugin
    implements GPlazmaMappingPlugin, GPlazmaSessionPlugin, GPlazmaIdentityPlugin
{
    private final String _dbDriver;
    private final String _dbUrl;
    private final String _dbUsername;
    private final String _dbPassword;
    private final boolean _dbSchemaAuto;
    private final String _dbChangelog;
    private final DpmPlugin _dpmPlugin;

    private FallibleTransactionTemplate _tx;
    private JdoDao _dao;

    public TransactionalDpmPlugin(Properties properties)
    {
        _dbDriver = properties.getProperty("gplazma.dpm.db.driver");
        _dbUrl = properties.getProperty("gplazma.dpm.db.url");
        _dbUsername = properties.getProperty("gplazma.dpm.db.user");
        _dbPassword = properties.getProperty("gplazma.dpm.db.password");
        _dbSchemaAuto =
            Boolean.valueOf(properties.getProperty("gplazma.dpm.db.schema.auto"));
        _dbChangelog = properties.getProperty("gplazma.dpm.db.schema.changelog");

        createSchema();
        createJdo();

        _dpmPlugin = new DpmPlugin(properties, _dao);
    }

    private void createSchema()
    {
        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setDriverClassName(_dbDriver);
        dataSource.setUrl(_dbUrl);
        dataSource.setUsername(_dbUsername);
        dataSource.setPassword(_dbPassword);

        new JdbcTemplate(dataSource).execute(new ConnectionCallback<Void>()
        {
            public Void doInConnection(Connection c)
                    throws SQLException, DataAccessException
            {
                try {
                    Liquibase liquibase =
                            new Liquibase(_dbChangelog,
                                    new ClassLoaderResourceAccessor(),
                                    new JdbcConnection(c));
                    if (_dbSchemaAuto) {
                        liquibase.update(null);
                    } else {
                        List<ChangeSet> changeSets =
                                liquibase.listUnrunChangeSets(null);
                        if (!changeSets.isEmpty()) {
                            throw new MigrationFailedException(changeSets.get(0),
                                    "Automatic schema migration is disabled. Please apply missing changes.");
                        }
                    }
                } catch (LiquibaseException e) {
                    throw new SQLException("Schema migration failed", e);
                }
                return null;
            }
        });
    }

    private void createJdo()
    {
        Map<String, String> properties = new HashMap<>();
        properties.put("javax.jdo.PersistenceManagerFactoryClass",
                       "org.datanucleus.api.jdo.JDOPersistenceManagerFactory");
        properties.put("javax.jdo.option.ConnectionURL", _dbUrl);
        properties.put("javax.jdo.option.ConnectionUserName", _dbUsername);
        properties.put("javax.jdo.option.ConnectionPassword", _dbPassword);
        properties.put("javax.jdo.option.ConnectionDriverName", _dbDriver);
        properties.put("datanucleus.PersistenceUnitName", "DPM");
        properties.put("datanucleus.connectionPoolingType", "BoneCp");
        properties.put("datanucleus.connectionPool.maxPoolSize", "10");
        properties.put("datanucleus.connectionPool.minPoolSize", "1");
        properties.put("datanucleus.connectionPool.initialPoolSize", "1");
        properties.put("datanucleus.connectionPool.maxStatements", "200");
        properties.put("datanucleus.cache.level2.type", "none");

        PersistenceManagerFactory pmf =
            JDOHelper.getPersistenceManagerFactory(properties);

        _tx = new FallibleTransactionTemplate(new JdoTransactionManager(pmf));

        TransactionAwarePersistenceManagerFactoryProxy pmfProxy =
            new TransactionAwarePersistenceManagerFactoryProxy();
        pmfProxy.setAllowCreate(false);
        pmfProxy.setTargetPersistenceManagerFactory(pmf);

        _dao = new JdoDao();
        _dao.setPersistenceManagerFactory(pmfProxy.getObject());
    }

    @Override
    public void map(final Set<Principal> principals)
        throws AuthenticationException
    {
        _tx.execute(new FallibleTransactionCallbackWithoutResult<AuthenticationException>() {
            @Override
            protected void doInFallibleTransactionWithoutResult(TransactionStatus status)
                    throws AuthenticationException
            {
                _dpmPlugin.map(principals);
            }
        });
    }

    /**
     * Forward mapping.
     * @param principal
     * @return mapped principal
     * @throws org.dcache.gplazma.NoSuchPrincipalException if mapping does not exists.
     */
    @Override
    public Principal map(final Principal principal) throws NoSuchPrincipalException
    {
        return _tx.execute(new FallibleTransactionCallback<Principal, NoSuchPrincipalException>()  {
            @Override
            public Principal doInFallibleTransaction(TransactionStatus status)
                    throws NoSuchPrincipalException
            {
                return _dpmPlugin.map(principal);
            }
        });
    }

    /**
     * Reverse mapping. The resulting {@link Set} MUST contain only principals on which <code>map</code>
     * call will return the provided <code>principal</code>
     * @param principal
     * @return non empty {@link Set} of equivalent principals.
     * @throws NoSuchPrincipalException if mapping does not exists.
     */
    @Override
    public Set<Principal> reverseMap(final Principal principal) throws NoSuchPrincipalException
    {
        return _tx.execute(new FallibleTransactionCallback<Set<Principal>, NoSuchPrincipalException>() {
            public Set<Principal> doInFallibleTransaction(TransactionStatus status)
                    throws NoSuchPrincipalException
            {
                return _dpmPlugin.reverseMap(principal);
            }
        });
    }

    @Override
    public void session(final Set<Principal> authorizedPrincipals,
                        final Set<Object> attrib)
        throws AuthenticationException
    {
        _tx.execute(new FallibleTransactionCallbackWithoutResult<AuthenticationException>() {
            @Override
            public void doInFallibleTransactionWithoutResult(TransactionStatus status)
                    throws AuthenticationException
            {
                _dpmPlugin.session(authorizedPrincipals, attrib);
            }
        });
    }
}
