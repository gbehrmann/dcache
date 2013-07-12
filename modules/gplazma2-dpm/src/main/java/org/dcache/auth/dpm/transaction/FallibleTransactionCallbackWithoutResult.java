package org.dcache.auth.dpm.transaction;

import org.springframework.transaction.TransactionStatus;

public abstract class FallibleTransactionCallbackWithoutResult<E extends Exception> extends FallibleTransactionCallback<Void, E>
{
    @Override
    protected Void doInFallibleTransaction(TransactionStatus status) throws E
    {
        doInFallibleTransactionWithoutResult(status);
        return null;
    }

    protected abstract void doInFallibleTransactionWithoutResult(TransactionStatus status) throws E;
}
