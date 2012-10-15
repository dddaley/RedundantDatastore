package com.rackspace.papi.service.datastore.impl.redundant.data;

import java.io.IOException;
import java.io.Serializable;
import java.net.Socket;
import java.net.UnknownHostException;

public class Subscriber implements Serializable {

    private static final int TIMEOUT = 1000;
    private final String host;
    private final int port;
    private Socket socket;

    public Subscriber(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public void setSocket(Socket socket) {
        this.socket = socket;
    }

    public Socket getSocket() throws UnknownHostException, IOException {
        if (socket == null) {
            socket = new Socket(host, port);
            socket.setKeepAlive(true);
            socket.setSoTimeout(TIMEOUT);
        }
        return socket;
    }

    public void close() throws IOException {
        if (socket != null) {
            socket.close();
        }
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof Subscriber)) {
            return false;
        }

        Subscriber subscriber = (Subscriber) other;

        return getHost().equalsIgnoreCase(subscriber.getHost()) && getPort() == subscriber.getPort();
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 29 * hash + (this.getHost() != null ? this.getHost().hashCode() : 0);
        hash = 29 * hash + this.getPort();
        return hash;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }
}
