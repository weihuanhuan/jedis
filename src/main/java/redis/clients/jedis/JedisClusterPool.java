package redis.clients.jedis;

import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.util.Pool;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocketFactory;
import java.util.Set;

public class JedisClusterPool extends Pool<JedisCluster> {

    public JedisClusterPool(final GenericObjectPoolConfig poolConfig,
                            Set<HostAndPort> nodes) {
        this(poolConfig, new JedisClusterFactory(nodes));
    }

    public JedisClusterPool(final GenericObjectPoolConfig poolConfig,
                            Set<HostAndPort> nodes, int timeout) {
        this(poolConfig, new JedisClusterFactory(nodes, timeout));
    }

    public JedisClusterPool(final GenericObjectPoolConfig poolConfig,
                            Set<HostAndPort> nodes, int timeout, int maxAttempts) {
        this(poolConfig, new JedisClusterFactory(nodes, timeout, maxAttempts));
    }

    public JedisClusterPool(final GenericObjectPoolConfig poolConfig,
                            Set<HostAndPort> nodes, final GenericObjectPoolConfig jedisClusterPoolConfig) {
        this(poolConfig, new JedisClusterFactory(nodes, jedisClusterPoolConfig));
    }

    public JedisClusterPool(final GenericObjectPoolConfig poolConfig,
                            Set<HostAndPort> nodes, int timeout, final GenericObjectPoolConfig jedisClusterPoolConfig) {
        this(poolConfig, new JedisClusterFactory(nodes, timeout, jedisClusterPoolConfig));
    }

    public JedisClusterPool(final GenericObjectPoolConfig poolConfig,
                            Set<HostAndPort> jedisClusterNode, int timeout, int maxAttempts,
                            final GenericObjectPoolConfig jedisClusterPoolConfig) {
        this(poolConfig, new JedisClusterFactory(jedisClusterNode, timeout, maxAttempts, jedisClusterPoolConfig));
    }

    public JedisClusterPool(final GenericObjectPoolConfig poolConfig,
                            Set<HostAndPort> jedisClusterNode, int connectionTimeout, int soTimeout, int maxAttempts,
                            final GenericObjectPoolConfig jedisClusterPoolConfig) {
        this(poolConfig, new JedisClusterFactory(jedisClusterNode, connectionTimeout, soTimeout, maxAttempts, jedisClusterPoolConfig));
    }

    public JedisClusterPool(final GenericObjectPoolConfig poolConfig,
                            Set<HostAndPort> jedisClusterNode, int connectionTimeout, int soTimeout, int maxAttempts,
                            String password, final GenericObjectPoolConfig jedisClusterPoolConfig) {
        this(poolConfig, new JedisClusterFactory(jedisClusterNode, connectionTimeout, soTimeout, maxAttempts,
                password, jedisClusterPoolConfig));
    }

    public JedisClusterPool(final GenericObjectPoolConfig poolConfig,
                            Set<HostAndPort> jedisClusterNode, int connectionTimeout, int soTimeout, int maxAttempts,
                            String password, String clientName, final GenericObjectPoolConfig jedisClusterPoolConfig) {
        this(poolConfig, new JedisClusterFactory(jedisClusterNode, connectionTimeout, soTimeout, maxAttempts, password, clientName, jedisClusterPoolConfig));
    }

    public JedisClusterPool(final GenericObjectPoolConfig poolConfig,
                            Set<HostAndPort> jedisClusterNode, int connectionTimeout, int soTimeout, int maxAttempts,
                            String user, String password, String clientName, final GenericObjectPoolConfig jedisClusterPoolConfig) {
        this(poolConfig, new JedisClusterFactory(jedisClusterNode, connectionTimeout, soTimeout, maxAttempts,
                user, password, clientName, jedisClusterPoolConfig));
    }

    public JedisClusterPool(final GenericObjectPoolConfig poolConfig,
                            Set<HostAndPort> jedisClusterNode, int connectionTimeout, int soTimeout, int maxAttempts,
                            String password, String clientName, final GenericObjectPoolConfig jedisClusterPoolConfig,
                            boolean ssl) {
        this(poolConfig, new JedisClusterFactory(jedisClusterNode, connectionTimeout, soTimeout, maxAttempts,
                password, clientName, jedisClusterPoolConfig,
                ssl));
    }

    public JedisClusterPool(final GenericObjectPoolConfig poolConfig,
                            Set<HostAndPort> jedisClusterNode, int connectionTimeout, int soTimeout, int maxAttempts,
                            String user, String password, String clientName, final GenericObjectPoolConfig jedisClusterPoolConfig,
                            boolean ssl) {
        this(poolConfig, new JedisClusterFactory(jedisClusterNode, connectionTimeout, soTimeout, maxAttempts,
                user, password, clientName, jedisClusterPoolConfig,
                ssl));
    }

    public JedisClusterPool(final GenericObjectPoolConfig poolConfig,
                            Set<HostAndPort> jedisClusterNode, int connectionTimeout, int soTimeout, int maxAttempts,
                            String password, String clientName, final GenericObjectPoolConfig jedisClusterPoolConfig,
                            boolean ssl, SSLSocketFactory sslSocketFactory, SSLParameters sslParameters,
                            HostnameVerifier hostnameVerifier, JedisClusterHostAndPortMap hostAndPortMap) {
        this(poolConfig, new JedisClusterFactory(jedisClusterNode, connectionTimeout, soTimeout, maxAttempts,
                password, clientName, jedisClusterPoolConfig,
                ssl, sslSocketFactory, sslParameters,
                hostnameVerifier, hostAndPortMap));
    }

    public JedisClusterPool(final GenericObjectPoolConfig poolConfig,
                            Set<HostAndPort> jedisClusterNode, int connectionTimeout, int soTimeout, int maxAttempts,
                            String user, String password, String clientName, final GenericObjectPoolConfig jedisClusterPoolConfig,
                            boolean ssl, SSLSocketFactory sslSocketFactory, SSLParameters sslParameters,
                            HostnameVerifier hostnameVerifier, JedisClusterHostAndPortMap hostAndPortMap) {
        this(poolConfig, new JedisClusterFactory(jedisClusterNode, connectionTimeout, soTimeout, maxAttempts,
                user, password, clientName, jedisClusterPoolConfig,
                ssl, sslSocketFactory, sslParameters,
                hostnameVerifier, hostAndPortMap));
    }

    public JedisClusterPool(GenericObjectPoolConfig poolConfig, PooledObjectFactory<JedisCluster> factory) {
        super(poolConfig, factory);
    }

    @Override
    public JedisCluster getResource() {
        JedisCluster resource = super.getResource();
        // record pool for logic close
        if (resource != null) {
            resource.setJedisClusterPool(this);
        }
        return resource;
    }

    @Override
    public void returnResource(JedisCluster resource) {
        // reset pool for recycle resource
        if (resource != null) {
            resource.setJedisClusterPool(null);
        }
        super.returnResource(resource);
    }

}
