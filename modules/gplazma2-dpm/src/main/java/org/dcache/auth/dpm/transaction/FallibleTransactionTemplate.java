package org.dcache.auth.dpm.transaction;

import com.google.common.reflect.TypeToken;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.TransactionSystemException;
import org.springframework.transaction.support.TransactionTemplate;

import java.lang.reflect.UndeclaredThrowableException;

public class FallibleTransactionTemplate extends TransactionTemplate
{
    public FallibleTransactionTemplate()
    {
    }

    public FallibleTransactionTemplate(PlatformTransactionManager transactionManager)
    {
        super(transactionManager);
    }

    public FallibleTransactionTemplate(PlatformTransactionManager transactionManager,
                                       TransactionDefinition transactionDefinition)
    {
        super(transactionManager, transactionDefinition);
    }

    public <T, E extends Exception> T execute(FallibleTransactionCallback<T, E> action)
            throws E
    {
        PlatformTransactionManager transactionManager = getTransactionManager();
        TransactionStatus status = transactionManager.getTransaction(this);
        T result;
        try {
            result = action.doInFallibleTransaction(status);
        }
        catch (RuntimeException ex) {
            // Transactional code threw application exception -> rollback
            rollbackOnException(status, ex);
            throw ex;
        }
        catch (Error err) {
            // Transactional code threw error -> rollback
            rollbackOnException(status, err);
            throw err;
        }
        catch (Exception ex) {
            TypeToken<E> exceptionClass = action.getExceptionClass();
            if (exceptionClass.isAssignableFrom(ex.getClass())) {
                transactionManager.commit(status);
                throw (E) ex;
            }
            // Transactional code threw unexpected exception -> rollback
            rollbackOnException(status, ex);
            throw new UndeclaredThrowableException(ex, "TransactionCallback threw undeclared checked exception");
        }
        transactionManager.commit(status);
        return result;
    }

    /**
     * @see org.springframework.transaction.support.TransactionTemplate#rollbackOnException(org.springframework.transaction.TransactionStatus, Throwable)
     */
    private void rollbackOnException(TransactionStatus status, Throwable ex) throws TransactionException {
        logger.debug("Initiating transaction rollback on application exception", ex);
        try {
            getTransactionManager().rollback(status);
        }
        catch (TransactionSystemException ex2) {
            logger.error("Application exception overridden by rollback exception", ex);
            ex2.initApplicationException(ex);
            throw ex2;
        }
        catch (RuntimeException ex2) {
            logger.error("Application exception overridden by rollback exception", ex);
            throw ex2;
        }
        catch (Error err) {
            logger.error("Application exception overridden by rollback error", ex);
            throw err;
        }
    }
}
