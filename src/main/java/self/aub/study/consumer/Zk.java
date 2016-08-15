package self.aub.study.consumer;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.nio.charset.StandardCharsets;

/**
 * Created by liujinxin on 16/7/29.
 */
public class Zk {
    public static void main(String[] args) throws Exception {
        String zookeeper = "127.0.0.1:2181";

        ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(10000, 3);
        final CuratorFramework client = CuratorFrameworkFactory.builder().connectString(zookeeper).retryPolicy(retryPolicy).build();
        client.start();
        String errorsPath = "/";
        TreeCache treeCache = new TreeCache(client, errorsPath);
        treeCache.start();
        treeCache.getListenable().addListener(new TreeCacheListener() {

            @Override
            public void childEvent(CuratorFramework curatorFramework, TreeCacheEvent treeCacheEvent) throws Exception {
                TreeCacheEvent.Type type = treeCacheEvent.getType();
                long ctime = treeCacheEvent.getData().getStat().getCtime();
                String data = new String(treeCacheEvent.getData().getData(), StandardCharsets.UTF_8);
                String path = treeCacheEvent.getData().getPath();
                System.out.printf("type = %s, ctime = %d, path = %s, data = %s\n", type.name(), ctime, path, data);
            }
        });

        Thread.sleep(Integer.MAX_VALUE);
    }
}
