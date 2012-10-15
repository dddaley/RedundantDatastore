package com.rackspace.papi.service.datastore.impl.redundant;

import com.rackspace.papi.service.datastore.impl.redundant.notification.out.Notifier;
import com.rackspace.papi.service.datastore.impl.redundant.data.Operation;
import com.rackspace.papi.service.datastore.impl.redundant.data.Message;
import com.rackspace.papi.service.datastore.impl.redundant.data.Subscriber;
import com.rackspace.papi.commons.util.io.ObjectSerializer;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.UUID;
import java.util.logging.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SubscriptionListener implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(SubscriptionListener.class);
    private static final int BUFFER_SIZE = 1024 * 8;
    private static final int SOCKET_TIMEOUT = 1000;
    private final MulticastSocket socket;
    private final InetAddress group;
    private final byte[] buffer;
    private boolean done;
    private String tcpHost;
    private int tcpPort;
    private final int groupPort;
    private final Notifier notifier;
    private final RedundantDatastore datastore;
    private final boolean synched;
    private final UUID id;

    SubscriptionListener(RedundantDatastore datastore, Notifier notifier, String multicastAddress, int multicastPort) throws UnknownHostException, IOException {
        this.group = InetAddress.getByName(multicastAddress);
        this.groupPort = multicastPort;
        this.socket = new MulticastSocket(multicastPort);
        this.socket.joinGroup(group);
        this.buffer = new byte[BUFFER_SIZE];
        this.notifier = notifier;
        this.datastore = datastore;
        socket.setSoTimeout(SOCKET_TIMEOUT);
        socket.setReceiveBufferSize(BUFFER_SIZE);
        done = false;
        synched = false;
        id = UUID.randomUUID();
    }

    public void announce(Message message) {
        try {
            byte[] messageData = ObjectSerializer.instance().writeObject(message);
            DatagramPacket messagePacket = new DatagramPacket(messageData, messageData.length, group, groupPort);
            socket.send(messagePacket);
        } catch (IOException ex) {
            LOG.error("Unable to send multicast announcement", ex);
        }
    }

    public void join(String host, int port) {
        this.tcpHost = host;
        this.tcpPort = port;
        try {
            byte[] subscriberData = ObjectSerializer.instance().writeObject(new Subscriber(host, port));
            announce(new Message(Operation.JOINING, id.toString(), subscriberData, 0));
        } catch (IOException ex) {
            LOG.error("Unable to serialize subscriber information", ex);
        }
    }

    public void sendSyncRequest(String targetId) {
        try {
            Subscriber subscriber = new Subscriber(tcpHost, tcpPort);
            byte[] subscriberData = ObjectSerializer.instance().writeObject(subscriber);
            announce(new Message(Operation.SYNC, targetId, id.toString(), subscriberData, 0));
        } catch (IOException ex) {
            LOG.error("Unable to serialize subscriber information", ex);
        }

    }

    public void listening() {
        try {
            Subscriber subscriber = new Subscriber(tcpHost, tcpPort);
            byte[] subscriberData = ObjectSerializer.instance().writeObject(subscriber);
            announce(new Message(Operation.LISTENING, id.toString(), subscriberData, 0));
        } catch (IOException ex) {
            LOG.error("Unable to serialize subscriber information", ex);
        }

    }

    public void leaving() {
        try {
            Subscriber subscriber = new Subscriber(tcpHost, tcpPort);
            byte[] subscriberData = ObjectSerializer.instance().writeObject(subscriber);
            announce(new Message(Operation.LEAVING, id.toString(), subscriberData, 0));
        } catch (IOException ex) {
            LOG.error("Unable to serialize subscriber information", ex);
        }

    }

    private void receivedAnnouncement(String key, String targetId, Operation operation, Subscriber subscriber) {
        if (tcpHost.equalsIgnoreCase(subscriber.getHost()) && subscriber.getPort() == tcpPort) {
            return;
        }

        switch (operation) {
            case JOINING:
                notifier.addSubscriber(subscriber);
                listening();
                break;
            case SYNC:
                if (id.toString().equals(targetId)) {
                    try {
                        datastore.sync(subscriber);
                    } catch (IOException ex) {
                        LOG.error("Error synching with remote node", ex);
                    }
                }
                break;
            case LISTENING:
                if (!synched) {
                    sendSyncRequest(key);
                }
                notifier.addSubscriber(subscriber);
                break;
            case LEAVING:
                notifier.removeSubscriber(subscriber);
                break;
        }

        LOG.debug("Received " + operation.name() + " Request From: " + subscriber.getHost() + ":" + subscriber.getPort());
    }

    @Override
    public void run() {
        while (!done) {
            try {
                DatagramPacket recv = new DatagramPacket(buffer, BUFFER_SIZE);
                socket.receive(recv);
                Message message = (Message) ObjectSerializer.instance().readObject(recv.getData());
                receivedAnnouncement(message.getKey(), message.getTargetId(), message.getOperation(), (Subscriber) ObjectSerializer.instance().readObject(message.getData()));
            } catch (SocketTimeoutException ex) {
                // ignore
            } catch (ClassNotFoundException ex) {
                LOG.error("Unable to deserialize multicast message", ex);
            } catch (IOException ex) {
                LOG.error("Unable to deserialize multicast message", ex);
            }
        }

        leaving();

        LOG.info("Exiting subscription listener thread");
        try {
            socket.leaveGroup(group);
            socket.close();
        } catch (IOException ex) {
            LOG.error("Unable to leave multicast group", ex);
        }
    }

    public void unsubscribe() {
        done = true;
    }
}
