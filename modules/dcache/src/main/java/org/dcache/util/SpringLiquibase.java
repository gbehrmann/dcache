package org.dcache.util;

import liquibase.Liquibase;
import liquibase.changelog.ChangeSet;
import liquibase.exception.LiquibaseException;
import liquibase.exception.MigrationFailedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

public class SpringLiquibase
    extends liquibase.integration.spring.SpringLiquibase
{
    private final static Logger _log =
        LoggerFactory.getLogger(SpringLiquibase.class);

    private boolean _shouldUpdate = true;

    public void setShouldUpdate(boolean shouldUpdate)
    {
        _shouldUpdate = shouldUpdate;
    }

    public boolean getShouldUpdate()
    {
        return _shouldUpdate;
    }

    @Override
    public void afterPropertiesSet()
    {
        run();
    }

    public void run()
    {
        getJdbcTemplate().execute(new SchemaMigrator());
    }

    protected JdbcTemplate getJdbcTemplate()
    {
        return new JdbcTemplate(getDataSource());
    }

    private class SchemaMigrator implements ConnectionCallback<Void>
    {
        @Override
        public Void doInConnection(Connection c)
            throws SQLException, DataAccessException
        {
            try {
                Liquibase liquibase = createLiquibase(c);
                try {
                    if (_shouldUpdate) {
                        liquibase.update(getContexts());
                    } else {
                        List<ChangeSet> changeSets =
                            liquibase.listUnrunChangeSets(getContexts());
                        if (!changeSets.isEmpty()) {
                            throw new MigrationFailedException(changeSets.get(0),
                                                               "Automatic schema migration is disabled. Please apply missing changes.");
                        }
                    }
                } finally {
                    liquibase.forceReleaseLocks();
                }
            } catch (LiquibaseException e) {
                throw new SQLException("Schema migration failed", e);
            }
            return null;
        }
    }
}
