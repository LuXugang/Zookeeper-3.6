package example;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Lu Xugang
 * @date 2020/10/15 2:27 下午
 */
public class CreateDataTest implements Watcher, Closeable {
    private final ZooKeeper zooKeeper;

    public CreateDataTest(String hostPort, int sessionTimeout) throws Exception{
        this.zooKeeper = new ZooKeeper(hostPort, sessionTimeout, this);
    }

    public void process(WatchedEvent event) {
        String path = event.getPath();
        System.out.println("path:"+path+" eventType: "+event.getState().name()+"");
    }
    public void createData(String data, String path, List<ACL> acl, CreateMode createMode) throws Exception{
       zooKeeper.create(path, data.getBytes(StandardCharsets.UTF_8), acl, createMode);
    }

    public void close() throws IOException {
        try {
            zooKeeper.close();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public Stat isNodeExist(String path) throws Exception{
        return zooKeeper.exists(path, true);
    }

    public void updateNode(String path, String newData, int version) throws Exception{
        zooKeeper.setData(path, newData.getBytes(StandardCharsets.UTF_8), version);
    }

    public void  tryCreateParentNode(String path) throws Exception{
       if(zooKeeper.exists(path, false) == null){
           zooKeeper.create(path, path.getBytes(Charset.defaultCharset()), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
       }
    }

    public static void main(String[] args) throws Exception{
        CreateDataTest createDataTest = new CreateDataTest("localhost:2181", 30000000);
        int count = 0;
        String parentNode = "/Chris";
        createDataTest.tryCreateParentNode(parentNode);
        String path;
        try {
            while (count++ < 10000){
                path = "/Chris/" + count;
                Stat stat = createDataTest.isNodeExist(path);
                if(stat == null){
                    createDataTest.createData("data" +count, path, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                }else {
                    createDataTest.updateNode(path, "data" + count, -1);
                }
                Thread.sleep(10000);
            }
        } finally {
            createDataTest.close();
        }
    }
}
