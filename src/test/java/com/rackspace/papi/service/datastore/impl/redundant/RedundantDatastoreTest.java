package com.rackspace.papi.service.datastore.impl.redundant;

import com.rackspace.papi.service.datastore.impl.redundant.impl.RedundantDatastoreImpl;
import com.rackspace.papi.service.datastore.StoredElement;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.config.CacheConfiguration;
import net.sf.ehcache.config.Configuration;
import org.junit.After;
import org.junit.AfterClass;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;

@Ignore
@RunWith(Enclosed.class)
public class RedundantDatastoreTest {

    public static class When {

        private static final int NODES = 5;
        private String address = "228.5.6.7";
        private int port = 6789;
        private static List<Cache> caches;
        private ArrayList<RedundantDatastoreImpl> datastores;
        private static CacheManager ehCacheManager;

        @BeforeClass
        public static void setupCache() {
            final Configuration defaultConfiguration = new Configuration();
            defaultConfiguration.setDefaultCacheConfiguration(new CacheConfiguration().diskPersistent(false));
            defaultConfiguration.setUpdateCheck(false);

            ehCacheManager = new CacheManager(defaultConfiguration);
            caches = new ArrayList<Cache>();
            
            for (int i = 0; i < NODES; i++) {
                Cache cache = new Cache("Test" + i, 20000, false, false, 5, 2);
                caches.add(cache);
                ehCacheManager.addCache(cache);
            }

        }

        @AfterClass
        public static void shutdownCache() {
            ehCacheManager.shutdown();
        }

        @Before
        public void setUp() throws UnknownHostException, IOException, InterruptedException {

            datastores = new ArrayList<RedundantDatastoreImpl>();

            for (int i = 0; i < NODES; i++) {
                datastores.add(new RedundantDatastoreImpl(address, port, caches.get(i)));
            }

            for (RedundantDatastoreImpl datastore : datastores) {
                datastore.joinGroup();
            }


            Thread.sleep(100);
        }

        @After
        public void cleanup() throws InterruptedException {
            for (RedundantDatastoreImpl datastore : datastores) {
                datastore.leaveGroup();
            }
            
            Thread.sleep(100);
        }

        //@Ignore
        @Test
        public void should() throws InterruptedException {
            for (int i = 0; i < 10 * NODES; i++) {
                byte[] data = ("Test:" + (i % NODES)).getBytes();
                RedundantDatastoreImpl datastore = datastores.get(i % NODES);
                datastore.put("key" + i, data, 10, TimeUnit.MINUTES);
            }
            
            Thread.sleep(1000);
            
            for (int i = 0; i < 10 * NODES; i++) {
                String data = ("Test:" + (i % NODES));
                for (RedundantDatastoreImpl datastore : datastores) {
                    StoredElement get = datastore.get("key" + i);
                    assertFalse(get.elementIsNull());
                    assertEquals(data, new String(get.elementBytes()));
                }
            }
        }
    }
}
