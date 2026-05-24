module com.saga.core {
    requires reactor.core;
    requires org.slf4j;
    requires static org.jspecify;
    requires static lombok;
    requires org.reactivestreams;

    exports hamburg.engelmann.saga;
    exports hamburg.engelmann.saga.step;
    exports hamburg.engelmann.saga.lifecycle;
    exports hamburg.engelmann.saga.lock;
}
