package org.openrepose.datastore;

import com.rackspace.papi.service.datastore.StoredElement;
import com.rackspace.papi.service.datastore.impl.redundant.RedundantDatastore;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;
import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.config.CacheConfiguration;
import net.sf.ehcache.config.Configuration;

public class App {

    private static String address = "228.5.6.7";
    private static int port = 6789;

    public static void main(String[] args) throws UnknownHostException, IOException {
        final Configuration defaultConfiguration = new Configuration();
        defaultConfiguration.setDefaultCacheConfiguration(new CacheConfiguration().diskPersistent(false));
        defaultConfiguration.setUpdateCheck(false);
        CacheManager ehCacheManager = new CacheManager(defaultConfiguration);

        Cache cache = new Cache("Cache" + Math.round(1000 * Math.random()), 20000, false, false, 5, 2);
        ehCacheManager.addCache(cache);
        RedundantDatastore datastore = new RedundantDatastore(address, port, cache);
        datastore.joinMulticastGroup();

        Scanner in = new Scanner(System.in);
        while (true) {
            String line = in.nextLine();
            String data[] = line.split(" ");

            switch (data.length) {
                case 1:
                    StoredElement get = datastore.get(data[0]);
                    String value = "null";
                    if (!get.elementIsNull()) {
                        value = new String(get.elementBytes());
                    }
                    System.out.println(data[0] + ": " + value);
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

    }
}
