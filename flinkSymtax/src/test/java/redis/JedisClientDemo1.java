package redis;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class JedisClientDemo1 {
//    使用的是redis集群，不能使用redis对象去连接，会报redis.clients.jedis.exceptions.JedisMovedDataException异常
//    private Jedis jedis;
    private JedisCluster jedisCluster;


    @Before
    public void setup(){
        Set<HostAndPort> hostAndPortSet = new HashSet<>();
        HostAndPort hostAndPort_7001 = new HostAndPort("192.168.158.138", 7001);
        HostAndPort hostAndPort_7002 = new HostAndPort("192.168.158.138", 7002);
        HostAndPort hostAndPort_7003 = new HostAndPort("192.168.158.138", 7003);
        HostAndPort hostAndPort_7004 = new HostAndPort("192.168.158.138", 7004);
        HostAndPort hostAndPort_7005 = new HostAndPort("192.168.158.136", 7005);
        HostAndPort hostAndPort_7006 = new HostAndPort("192.168.158.136", 7006);
        hostAndPortSet.add(hostAndPort_7001);
        hostAndPortSet.add(hostAndPort_7002);
        hostAndPortSet.add(hostAndPort_7003);
        hostAndPortSet.add(hostAndPort_7004);
        hostAndPortSet.add(hostAndPort_7005);
        hostAndPortSet.add(hostAndPort_7006);
        jedisCluster = new JedisCluster(hostAndPortSet, 5000);

    }

    @Test
    public void testGetKey(){
        String password = jedisCluster.hget("hadoop", "password");
        System.out.println(password);
        String response = jedisCluster.set("myName", "root");
        // 返回成功OK
        System.out.println(response);
    }



    @After
    public void cleanUp() throws IOException {
        jedisCluster.close();
    }
}
