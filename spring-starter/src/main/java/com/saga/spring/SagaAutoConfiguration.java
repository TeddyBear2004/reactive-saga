package com.saga.spring;

import com.saga.lock.SagaLockRepository;
import com.saga.lock.SagaLockService;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;

/**
 * Spring Boot auto-configuration for the saga engine.
 *
 * <p>Registers a {@link SagaLockService} bean if a {@link SagaLockRepository} bean is present in
 * the application context and no {@link SagaLockService} bean has been defined manually.
 */
@AutoConfiguration
public class SagaAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnBean(SagaLockRepository.class)
    public SagaLockService sagaLockService(SagaLockRepository repository) {
        return SagaLockService.create(repository);
    }

}
