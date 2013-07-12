package org.dcache.auth.dpm.transaction;

import com.google.common.reflect.TypeToken;
import org.springframework.transaction.TransactionStatus;

public abstract class FallibleTransactionCallback<T, E extends Exception>
{
    private final TypeToken<E> _exceptionType = new TypeToken<E>(getClass()) {};

    protected abstract T doInFallibleTransaction(TransactionStatus status) throws E;

    protected TypeToken<E> getExceptionClass()
    {
        return _exceptionType;
    }
}
