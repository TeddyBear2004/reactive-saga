package com.saga.spring;

import com.saga.SagaEngine;
import com.saga.lock.SagaLockRepository;
import com.saga.lock.SagaLockService;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;

/**
 * Spring Boot auto-configuration for the saga engine.
 *
 * <p>Always registers an {@link SagaEngine} bean (in-memory strategy by default).
 * Registers a {@link SagaLockService} bean if a {@link SagaLockRepository} bean is present.
 * When both are present the engine is automatically configured with the lock service.
 */
@AutoConfiguration
public class SagaAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public SagaEngine sagaEngine(ObjectProvider<SagaLockService> lockServiceProvider) {
        SagaEngine engine = SagaEngine.inMemory();
        SagaLockService lockService = lockServiceProvider.getIfAvailable();
        return lockService != null ? engine.withLockService(lockService) : engine;
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnBean(SagaLockRepository.class)
    public SagaLockService sagaLockService(SagaLockRepository repository) {
        return SagaLockService.create(repository);
    }

}
