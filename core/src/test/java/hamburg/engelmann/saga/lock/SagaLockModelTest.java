package hamburg.engelmann.saga.lock;

import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

class SagaLockModelTest {

    // ── LockableResourceId ─────────────────────────────────────────────────────

    record OrderId(UUID value) implements LockableResourceId {}
    record Order(UUID value) implements LockableResourceId {}

    @Test
    void lockableResourceId_resourceType_stripsIdSuffix() {
        assertThat(new OrderId(UUID.randomUUID()).resourceType()).isEqualTo("Order");
    }

    @Test
    void lockableResourceId_resourceType_withoutIdSuffix_returnsSimpleName() {
        assertThat(new Order(UUID.randomUUID()).resourceType()).isEqualTo("Order");
    }

    @Test
    void lockableResourceId_resourceId_returnsUuidString() {
        UUID id = UUID.randomUUID();
        assertThat(new OrderId(id).resourceId()).isEqualTo(id.toString());
    }

    @Test
    void lockableResourceId_getId_returnsSelf() {
        OrderId orderId = new OrderId(UUID.randomUUID());
        assertThat(orderId.getId()).isSameAs(orderId);
    }

    // ── SagaLockId ─────────────────────────────────────────────────────────────

    @Test
    void sagaLockId_constructor_nullValue_throwsNPE() {
        assertThatNullPointerException()
                .isThrownBy(() -> new SagaLockId(null))
                .withMessage("SagaLockId value cannot be null");
    }

    @Test
    void sagaLockId_toString_returnsUuidString() {
        UUID uuid = UUID.randomUUID();
        assertThat(new SagaLockId(uuid).toString()).isEqualTo(uuid.toString());
    }

    @Test
    void sagaLockId_equalityBasedOnValue() {
        UUID uuid = UUID.randomUUID();
        assertThat(new SagaLockId(uuid)).isEqualTo(new SagaLockId(uuid));
        assertThat(new SagaLockId(uuid)).isNotEqualTo(new SagaLockId(UUID.randomUUID()));
    }

    // ── SagaLockContext ────────────────────────────────────────────────────────

    @Test
    void sagaLockContext_forSaga_setsNameAndRandomId() {
        SagaLockContext ctx1 = SagaLockContext.forSaga("my-saga");
        SagaLockContext ctx2 = SagaLockContext.forSaga("my-saga");

        assertThat(ctx1.sagaName()).isEqualTo("my-saga");
        assertThat(ctx1.sagaId()).isNotNull();
        assertThat(ctx1.userId()).isNull();
        assertThat(ctx1.sagaId()).isNotEqualTo(ctx2.sagaId());
    }

    @Test
    void sagaLockContext_withUserId_updatesUserId_keepsSagaId() {
        SagaLockContext ctx = SagaLockContext.forSaga("my-saga");
        SagaLockContext withUser = ctx.withUserId("user-42");

        assertThat(withUser.userId()).isEqualTo("user-42");
        assertThat(withUser.sagaId()).isEqualTo(ctx.sagaId());
        assertThat(withUser.sagaName()).isEqualTo("my-saga");
    }

    @Test
    void sagaLockContext_withUserId_null_setsNullUserId() {
        SagaLockContext ctx = SagaLockContext.forSaga("saga").withUserId("u1");
        assertThat(ctx.withUserId(null).userId()).isNull();
    }

    // ── ResourceAlreadyLockedException ─────────────────────────────────────────

    @Test
    void resourceAlreadyLockedException_message_containsTypeAndId() {
        ResourceAlreadyLockedException ex = new ResourceAlreadyLockedException("Order", "abc-123");
        assertThat(ex.getMessage())
                .contains("Order")
                .contains("abc-123");
    }

    @Test
    void resourceAlreadyLockedException_isRuntimeException() {
        assertThat(new ResourceAlreadyLockedException("T", "id"))
                .isInstanceOf(RuntimeException.class);
    }
}
