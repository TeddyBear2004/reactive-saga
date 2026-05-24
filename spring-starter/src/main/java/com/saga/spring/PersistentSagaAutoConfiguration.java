package com.saga.spring;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.saga.SagaEngine;
import com.saga.engine.persistent.PersistentSagaEngine;
import com.saga.engine.persistent.SagaExecutionRepository;
import com.saga.engine.persistent.SagaStateSerializer;
import com.saga.lifecycle.SagaLifecycleObserver;
import com.saga.lock.SagaLockService;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;

/**
 * Spring Boot auto-configuration for the {@link PersistentSagaEngine}.
 *
 * <p>Activates when:
 * <ol>
 *   <li>{@code engines:persistent} is on the classpath (i.e. the user has declared the dependency)</li>
 *   <li>A {@link SagaExecutionRepository} bean is present in the application context</li>
 * </ol>
 *
 * <p>When active, this configuration takes precedence over {@link SagaAutoConfiguration}'s
 * in-memory default via {@link AutoConfigureBefore}.
 *
 * <p>If no {@link SagaStateSerializer} bean is provided, one is auto-registered using the
 * application context's {@link ObjectMapper} with FQCN type info (requires Jackson to be present).
 *
 * <p>{@link SagaExecutionRepository} is an infrastructure port — the application must provide
 * an implementation for its chosen datastore (e.g. R2DBC, MongoDB, Redis):
 * <pre>{@code
 * @Bean
 * public SagaExecutionRepository sagaExecutionRepository(MyDataStore store) {
 *     return new MyDataStoreSagaExecutionRepository(store);
 * }
 * }</pre>
 */
@AutoConfiguration
@AutoConfigureBefore(SagaAutoConfiguration.class)
@ConditionalOnClass(PersistentSagaEngine.class)
@ConditionalOnBean(SagaExecutionRepository.class)
public class PersistentSagaAutoConfiguration {

    /**
     * Registers a Jackson-based {@link SagaStateSerializer} using FQCN type info.
     * Only activated when Jackson's {@link ObjectMapper} is on the classpath and no custom
     * {@link SagaStateSerializer} bean has been declared.
     */
    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnClass(ObjectMapper.class)
    public SagaStateSerializer sagaStateSerializer(ObjectProvider<ObjectMapper> objectMapperProvider) {
        ObjectMapper mapper = objectMapperProvider.getIfAvailable(ObjectMapper::new);
        return SagaStateSerializer.jacksonWithFqcn(mapper);
    }

    /**
     * Registers a {@link PersistentSagaEngine} bean as the active {@link SagaEngine}.
     * Requires a {@link SagaStateSerializer} bean (either user-provided or auto-configured above).
     */
    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnBean(SagaStateSerializer.class)
    public SagaEngine sagaEngine(SagaExecutionRepository repository,
                                 SagaStateSerializer serializer,
                                 ObjectProvider<SagaLockService> lockServiceProvider,
                                 ObjectProvider<SagaLifecycleObserver> observerProvider) {
        SagaEngine engine = new PersistentSagaEngine(repository, serializer);
        SagaLockService lockService = lockServiceProvider.getIfAvailable();
        SagaLifecycleObserver observer = observerProvider.getIfAvailable();
        if (observer != null) engine = engine.withObserver(observer);
        if (lockService != null) engine = engine.withLockService(lockService);
        return engine;
    }

}
