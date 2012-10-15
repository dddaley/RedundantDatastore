package com.rackspace.papi.service.datastore.impl.redundant.notification.out;

import com.rackspace.papi.commons.util.io.ObjectSerializer;
import com.rackspace.papi.service.datastore.impl.redundant.data.Message;
import com.rackspace.papi.service.datastore.impl.redundant.data.Operation;
import com.rackspace.papi.service.datastore.impl.redundant.data.Subscriber;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.HashSet;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UpdateNotifier implements Notifier {

    private static final Logger LOG = LoggerFactory.getLogger(UpdateNotifier.class);
    private final Set<Subscriber> subscribers;

    public UpdateNotifier() {
        this.subscribers = new HashSet<Subscriber>();
    }

    @Override
    public void addSubscriber(Subscriber subscriber) {
        subscribers.add(subscriber);
    }

    @Override
    public void removeSubscriber(Subscriber subscriber) {
        try {
            for (Subscriber s : subscribers) {
                if (s.equals(subscriber)) {
                    try {
                        s.close();
                    } catch (IOException ex) {
                        LOG.warn("Error closing socket", ex);
                    }
                }
            }
        } finally {
            subscribers.remove(subscriber);
        }
    }

    public void notifyNode(Subscriber subscriber, byte[] messageData) {
        OutputStream out = null;
        try {
            //socket = new Socket(subscriber.getHost(), subscriber.getPort());
            Socket socket = subscriber.getSocket();
            out = new BufferedOutputStream(socket.getOutputStream());
            out.write(messageData);
            out.flush();
        } catch (IOException ex) {
            LOG.error("Error notifying node: " + subscriber.getHost() + ":" + subscriber.getPort(), ex);
            removeSubscriber(subscriber);
            if (out != null) {
                try {
                    out.close();
                } catch (IOException ioex) {
                    LOG.warn("Error closing output stream", ioex);
                }
            }
        }
    }

    @Override
    public void notifyNode(Operation operation, Subscriber subscriber, String key, byte[] data, int ttl) throws IOException {
        Message message = new Message(operation, key, data, ttl);
        byte[] messageData = ObjectSerializer.instance().writeObject(message);
        notifyNode(subscriber, messageData);

    }

    @Override
    public void notifyNode(Operation operation, Subscriber subscriber, String[] keys, byte[][] data, int[] ttl) throws IOException {
        Message message = new Message(operation, keys, data, ttl);
        byte[] messageData = ObjectSerializer.instance().writeObject(message);
        notifyNode(subscriber, messageData);

    }

    @Override
    public void notifyAllNodes(Operation operation, String key, byte[] data, int ttl) throws IOException {
        Message message = new Message(operation, key, data, ttl);
        byte[] messageData = ObjectSerializer.instance().writeObject(message);
        for (Subscriber subscriber : subscribers) {
            notifyNode(subscriber, messageData);
        }
    }

    @Override
    public void notifyAllNodes(Operation operation, String key, byte[] data) throws IOException {
        notifyAllNodes(operation, key, data, 0);
    }

    @Override
    public void notifyAllNodes(Operation operation, String key) throws IOException {
        notifyAllNodes(operation, key, null, 0);
    }
}
