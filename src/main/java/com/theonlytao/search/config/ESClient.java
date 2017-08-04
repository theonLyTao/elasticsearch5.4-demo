package com.theonlytao.search.config;

import lombok.NoArgsConstructor;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;

/**
 * Created by T T on 2017/6/27.
 */
@NoArgsConstructor
public class ESClient {
    public static class Builder {
        //集群名称
        private String cluster_name;
        //集群地址,形如"192.168.107.100:9300,192.168.107.101:9300"
        private String esHosts;
        //是否自动嗅探整个集群的状态，把集群中其它机器的ip地址加到客户端中
        private Boolean client_transport_sniff = false;
        //设置为true以忽略连接节点的群集名称验证。
        private Boolean ignore_cluster_name = false;
        //等待ping返回结果超时时间，默认5s
        private String ping_timeout;
        //刷新节点链表时间间隔，默认5s
        private String nodes_sampler_interval;

        private Client client;

        public Builder cluster_name(String cluster_name) {
            this.cluster_name = cluster_name;
            return this;
        }

        public Builder esHosts(String esHosts) {
            this.esHosts = esHosts;
            return this;
        }

        public Builder client_transport_sniff(Boolean client_transport_sniff) {
            this.client_transport_sniff = client_transport_sniff;
            return this;
        }

        public Builder ignore_cluster_name(Boolean ignore_cluster_name) {
            this.ignore_cluster_name = ignore_cluster_name;
            return this;
        }

        public Builder ping_timeout(String ping_timeout) {
            this.ping_timeout = ping_timeout;
            return this;
        }

        public Builder nodes_sampler_interval(String nodes_sampler_interval) {
            this.nodes_sampler_interval = nodes_sampler_interval;
            return this;
        }

        public Client build() {
            Settings.Builder builder = Settings.builder();
            if (cluster_name != null) {
                builder.put("cluster.name", cluster_name);
            }
            if(client_transport_sniff){
                builder.put("client.transport.sniff",true);
            }
            if(ignore_cluster_name){
                builder.put("client.transport.ignore_cluster_name",true);
            }
            if(ping_timeout != null){
                builder.put("client.transport.ping_timeout",ping_timeout);
            }
            if(nodes_sampler_interval != null){
                builder.put("client.transport.nodes_sampler_interval",nodes_sampler_interval);
            }

            Settings settings = builder.build();
            try {
                if (cluster_name == null) {
                    throw new IllegalArgumentException("cluster_name can't be null");
                }
                if (esHosts == null) {
                    throw new IllegalArgumentException("esHosts can't be null");
                }
                String[] nodes = esHosts.split(",");
                InetSocketTransportAddress[] inetSocketTransportAddresses = new InetSocketTransportAddress[nodes.length];
                for (int i = 0; i < nodes.length; i++) {
                    String[] hostPort = nodes[i].split(":");
                    inetSocketTransportAddresses[i] = new InetSocketTransportAddress(InetAddress.getByName(hostPort[0]), Integer.valueOf(hostPort[1]));
                }
                client = new PreBuiltTransportClient(settings).addTransportAddresses(inetSocketTransportAddresses);
            } catch (Exception e) {
                return null;
            }
            return client;
        }
    }
}
