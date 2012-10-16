package org.openrepose.datastore;

import com.rackspace.papi.service.datastore.StoredElement;
import com.rackspace.papi.service.datastore.impl.redundant.RedundantDatastore;
import com.rackspace.papi.service.datastore.impl.redundant.data.Subscriber;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.config.CacheConfiguration;
import net.sf.ehcache.config.Configuration;

public class App {

    public static void main(String[] args) throws UnknownHostException, IOException {
        String address = "228.5.6.7";
        //String address = "ff02::1234:1234:1234:1234";
        int port = 6789;
        String nic = "*";
        final Configuration defaultConfiguration = new Configuration();
        defaultConfiguration.setDefaultCacheConfiguration(new CacheConfiguration().diskPersistent(false));
        defaultConfiguration.setUpdateCheck(false);
        CacheManager ehCacheManager = new CacheManager(defaultConfiguration);

        if (args.length > 0) {
            address = args[0];
        }

        if (args.length > 1) {
            port = Integer.parseInt(args[1]);
        }
        
        /*
        if (args.length > 2) {
            nic = args[2];
        }
        */

        Set<Subscriber> subscribers = new HashSet<Subscriber>();

        int index = 2;
        while (args.length > index + 1) {
            Subscriber subscriber = new Subscriber(args[index], -1, Integer.parseInt(args[index + 1]));
            subscribers.add(subscriber);
            index += 2;
        }

        Cache cache = new Cache("Cache" + Math.round(1000 * Math.random()), 20000, false, false, 5, 2);
        ehCacheManager.addCache(cache);
        RedundantDatastore datastore = new RedundantDatastore(subscribers, nic, address, port, cache);
        datastore.joinGroup();

        Scanner in = new Scanner(System.in);
        boolean process = true;
        while (process) {
            String line = in.nextLine();
            String data[] = line.split(" ");

            switch (data.length) {
                case 1:
                    if (data[0].contains("quit")) {
                        process = false;
                    } else if (data[0].length() > 0) {
                        StoredElement get = datastore.get(data[0]);
                        String value = "null";
                        if (!get.elementIsNull()) {
                            value = new String(get.elementBytes());
                        }
                        System.out.println(data[0] + ": " + value);
                    }
                    break;
                case 2:
                    System.out.println("Storing: " + data[0] + " --> " + data[1]);
                    datastore.put(data[0], data[1].getBytes(), 100, TimeUnit.SECONDS);
                    break;
                case 3:
                    System.out.println("Removing: " + data[1]);
                    datastore.remove(data[1]);
                    break;

            }
        }
        
        datastore.leaveGroup();

    }
}
