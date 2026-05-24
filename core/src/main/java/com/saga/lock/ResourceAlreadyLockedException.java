package com.saga.lock;

public class ResourceAlreadyLockedException extends RuntimeException {

    public ResourceAlreadyLockedException(String resourceType, String resourceId) {
        super("Resource is already locked: type=" + resourceType + ", id=" + resourceId);
    }

}
