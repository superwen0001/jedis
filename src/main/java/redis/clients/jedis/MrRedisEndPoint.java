package redis.clients.jedis;

import com.alibaba.fastjson.JSONObject;
import org.apache.http.HttpVersion;
import org.apache.http.client.fluent.Request;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.exceptions.JedisException;

import java.io.IOException;

/**
 * Created by wenshiliang on 2017/5/11.
 */
public class MrRedisEndPoint {
    protected Logger log = LoggerFactory.getLogger(getClass());
    private final String[] urls;

    public MrRedisEndPoint(String... url) {
        this.urls = url;
    }


    public String doGet(String url) throws IOException {
        return Request.Get(url)
                .useExpectContinue()
                .version(HttpVersion.HTTP_1_1)
//                .bodyString(param, ContentType.APPLICATION_JSON)
                .connectTimeout(3000)
                .socketTimeout(3000)
                .execute()
                .returnContent()
                .asString();
    }

    public HostAndPort getMaster(String redisInstanceName){
        HostAndPort master = null;
        String result = null;
        for (String url : urls) {
            //http://192.168.1.80:5656/v1/STATUS/TestInstance
            String getUrl = url + "/v1/STATUS/"+ redisInstanceName;
            try {
                result = doGet(getUrl);
                log.debug(result);
                break;
                //{"Name":"TestInstance","Type":"MS","Status":"DELETED","Capacity":100,"Master":null,"Slaves":null}

                /*
                {"Name":"test1","Type":"MS","Status":"RUNNING","Capacity":200,"Master":{"IP":"192.168.3.37",
                 "Port":"6380","MemoryCapacity":200,"MemoryUsed":1908208,"Uptime":62303,"ClientsConnected":1,
                 "LastSyncedToMaster":0},"Slaves":[{"IP":"192.168.1.37","Port":"6380","MemoryCapacity":200,
                 "MemoryUsed":853768,"Uptime":62299,"ClientsConnected":2,"LastSyncedToMaster":5},
                 {"IP":"192.168.3.37","Port":"6381","MemoryCapacity":200,"MemoryUsed":839144,"Uptime":62296,
                 "ClientsConnected":2,"LastSyncedToMaster":6}]}
                */
            } catch (IOException e) {
                log.error("",e);
            }
        }

        if(result==null){
            throw new JedisException("mr redis 无法访问 "+JSONObject.toJSONString(urls)+",或者不存在redis实例:"+redisInstanceName);
        }else{
//            try{
            JSONObject jsonObject = JSONObject.parseObject(result);
            String status = jsonObject.getString("Status");
            if("DELETED".equals(status)){
                throw new JedisException("redis instance is deleted");
            }else if("RUNNING".equals(status)){
                //redis 实例运行中
                JSONObject masterJson = jsonObject.getJSONObject("Master");
                master = new HostAndPort(masterJson.getString("IP"),masterJson.getIntValue("Port"));
            }

        }
        return master;
    }

    public static void main(String[] args) {

        MrRedisEndPoint mrRedisEndPoint = new MrRedisEndPoint("http://192.168.1.80:5656");

        HostAndPort master = mrRedisEndPoint.getMaster("test");
        System.out.println(master);
    }


//    private static class
}
