package com.rackspace.papi.service.datastore.impl.redundant.notification.out;

import com.rackspace.papi.service.datastore.impl.redundant.data.Operation;
import com.rackspace.papi.service.datastore.impl.redundant.data.Subscriber;
import java.io.IOException;

public interface Notifier {

    void addSubscriber(Subscriber subscriber);
    void notifyNode(Operation operation, Subscriber subscriber, String key, byte[] data, int ttl) throws IOException;
    void notifyNode(Operation operation, Subscriber subscriber, String[] keys, byte[][] data, int[] ttl) throws IOException;
    void notifyAllNodes(Operation operation, String key, byte[] data, int ttl) throws IOException;
    void notifyAllNodes(Operation operation, String key, byte[] data) throws IOException;
    void notifyAllNodes(Operation operation, String key) throws IOException;
    void removeSubscriber(Subscriber subscriber);
    
}
