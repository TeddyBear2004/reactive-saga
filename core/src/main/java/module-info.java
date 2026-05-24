module com.saga.core {
    requires reactor.core;
    requires org.slf4j;
    requires static org.jspecify;
    requires static lombok;

    exports com.saga;
    exports com.saga.step;
    exports com.saga.lifecycle;
    exports com.saga.lock;
}
