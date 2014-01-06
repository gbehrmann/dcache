package org.dcache.util.aspects;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.transaction.PlatformTransactionManager;

/**
 * BeanPostProcessor to configure PerInstanceAnnotationTransactionAspect instances.
 */
public class PerInstanceAnnotationTransactionBeanPostProcessor implements BeanPostProcessor, BeanFactoryAware
{
    private PlatformTransactionManager txManager;
    private BeanFactory beanFactory;

    @Override
    public Object postProcessBeforeInitialization(Object bean, String beanName) throws BeansException
    {
        if (bean instanceof PerInstanceAnnotationTransactionAspect.HasTransactional) {
            PerInstanceAnnotationTransactionAspect aspect = PerInstanceAnnotationTransactionAspect.aspectOf(bean);
            aspect.setTransactionManager(txManager);
            aspect.setBeanFactory(beanFactory);
        }
        return bean;
    }

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException
    {
        return bean;
    }

    @Override
    public void setBeanFactory(BeanFactory beanFactory) throws BeansException
    {
        this.beanFactory = beanFactory;
    }

    public void setTransactionManager(PlatformTransactionManager txManager)
    {
        this.txManager = txManager;
    }
}
