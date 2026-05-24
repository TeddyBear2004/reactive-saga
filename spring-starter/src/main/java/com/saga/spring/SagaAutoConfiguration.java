package com.saga.spring;

import com.saga.SagaEngine;
import com.saga.engine.memory.InMemorySagaEngine;
import com.saga.lifecycle.SagaLifecycleObserver;
import com.saga.lock.SagaLockRepository;
import com.saga.lock.SagaLockService;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

/**
 * Spring Boot auto-configuration for the saga engine.
 *
 * <p>Always registers a {@link SagaEngine} bean ({@link InMemorySagaEngine} by default).
 * Registers a {@link SagaLockService} bean if a {@link SagaLockRepository} bean is present.
 * When a {@link SagaLifecycleObserver} or {@link SagaLockService} bean is present, it is
 * automatically wired into the engine.
 *
 * <p>{@link SagaLockRepository} is an infrastructure port — the application must provide an
 * implementation for its chosen datastore (e.g. R2DBC, MongoDB).
 */
@AutoConfiguration
@EnableConfigurationProperties(SagaProperties.class)
public class SagaAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public SagaEngine sagaEngine(ObjectProvider<SagaLockService> lockServiceProvider,
                                 ObjectProvider<SagaLifecycleObserver> observerProvider) {
        SagaEngine engine = InMemorySagaEngine.create();
        SagaLifecycleObserver observer = observerProvider.getIfAvailable();
        SagaLockService lockService = lockServiceProvider.getIfAvailable();
        if (observer != null) engine = engine.withObserver(observer);
        return lockService != null ? engine.withLockService(lockService) : engine;
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(name = "saga.observer.logging.enabled", matchIfMissing = true)
    public SagaLifecycleObserver loggingSagaLifecycleObserver() {
        return new LoggingSagaLifecycleObserver();
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnBean(SagaLockRepository.class)
    public SagaLockService sagaLockService(SagaLockRepository repository) {
        return SagaLockService.create(repository);
    }

}
