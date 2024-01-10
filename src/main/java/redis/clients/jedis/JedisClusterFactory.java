package redis.clients.jedis;

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocketFactory;
import java.util.Set;

class JedisClusterFactory implements PooledObjectFactory<JedisCluster> {

    private final Set<HostAndPort> jedisClusterNode;
    private final int connectionTimeout;
    private final int soTimeout;
    private final int maxAttempts;
    private final String user;
    private final String password;
    private final String clientName;
    private final GenericObjectPoolConfig poolConfig;
    private final boolean ssl;
    private final SSLSocketFactory sslSocketFactory;
    private final SSLParameters sslParameters;
    private final HostnameVerifier hostnameVerifier;
    private final JedisClusterHostAndPortMap hostAndPortMap;

    public JedisClusterFactory(Set<HostAndPort> nodes) {
        this(nodes, JedisCluster.DEFAULT_TIMEOUT);
    }

    public JedisClusterFactory(Set<HostAndPort> nodes, int timeout) {
        this(nodes, timeout, JedisCluster.DEFAULT_MAX_ATTEMPTS);
    }

    public JedisClusterFactory(Set<HostAndPort> nodes, int timeout, int maxAttempts) {
        this(nodes, timeout, maxAttempts, new GenericObjectPoolConfig());
    }

    public JedisClusterFactory(Set<HostAndPort> nodes, final GenericObjectPoolConfig poolConfig) {
        this(nodes, JedisCluster.DEFAULT_TIMEOUT, JedisCluster.DEFAULT_MAX_ATTEMPTS, poolConfig);
    }

    public JedisClusterFactory(Set<HostAndPort> nodes, int timeout, final GenericObjectPoolConfig poolConfig) {
        this(nodes, timeout, JedisCluster.DEFAULT_MAX_ATTEMPTS, poolConfig);
    }

    public JedisClusterFactory(Set<HostAndPort> jedisClusterNode, int timeout, int maxAttempts,
                               final GenericObjectPoolConfig poolConfig) {
        this(jedisClusterNode, JedisCluster.DEFAULT_TIMEOUT, timeout, maxAttempts, poolConfig);
    }

    public JedisClusterFactory(Set<HostAndPort> jedisClusterNode, int connectionTimeout, int soTimeout, int maxAttempts,
                               final GenericObjectPoolConfig poolConfig) {
        this(jedisClusterNode, connectionTimeout, soTimeout, maxAttempts,
                null, poolConfig);
    }

    public JedisClusterFactory(Set<HostAndPort> jedisClusterNode, int connectionTimeout, int soTimeout, int maxAttempts,
                               String password, final GenericObjectPoolConfig poolConfig) {
        this(jedisClusterNode, connectionTimeout, soTimeout, maxAttempts,
                password, null, poolConfig);
    }

    public JedisClusterFactory(Set<HostAndPort> jedisClusterNode, int connectionTimeout, int soTimeout, int maxAttempts,
                               String password, String clientName, final GenericObjectPoolConfig poolConfig) {
        this(jedisClusterNode, connectionTimeout, soTimeout, maxAttempts,
                password, null, clientName, poolConfig);
    }

    public JedisClusterFactory(Set<HostAndPort> jedisClusterNode, int connectionTimeout, int soTimeout, int maxAttempts,
                               String user, String password, String clientName, final GenericObjectPoolConfig poolConfig) {
        this(jedisClusterNode, connectionTimeout, soTimeout, maxAttempts,
                user, password, clientName, poolConfig,
                false, null, null,
                null, null);
    }

    public JedisClusterFactory(Set<HostAndPort> jedisClusterNode, int connectionTimeout, int soTimeout, int maxAttempts,
                               String password, String clientName, final GenericObjectPoolConfig poolConfig,
                               boolean ssl) {
        this(jedisClusterNode, connectionTimeout, soTimeout, maxAttempts,
                null, password, clientName, poolConfig,
                ssl);
    }

    public JedisClusterFactory(Set<HostAndPort> jedisClusterNode, int connectionTimeout, int soTimeout, int maxAttempts,
                               String user, String password, String clientName, final GenericObjectPoolConfig poolConfig,
                               boolean ssl) {
        this(jedisClusterNode, connectionTimeout, soTimeout, maxAttempts,
                user, password, clientName, poolConfig,
                ssl, null, null,
                null, null);
    }

    public JedisClusterFactory(Set<HostAndPort> jedisClusterNode, int connectionTimeout, int soTimeout, int maxAttempts,
                               String password, String clientName, final GenericObjectPoolConfig poolConfig,
                               boolean ssl, SSLSocketFactory sslSocketFactory, SSLParameters sslParameters,
                               HostnameVerifier hostnameVerifier, JedisClusterHostAndPortMap hostAndPortMap) {
        this(jedisClusterNode, connectionTimeout, soTimeout, maxAttempts,
                null, password, clientName, poolConfig,
                ssl, sslSocketFactory, sslParameters,
                hostnameVerifier, hostAndPortMap);
    }

    public JedisClusterFactory(Set<HostAndPort> jedisClusterNode, int connectionTimeout, int soTimeout, int maxAttempts,
                               String user, String password, String clientName, final GenericObjectPoolConfig poolConfig,
                               boolean ssl, SSLSocketFactory sslSocketFactory, SSLParameters sslParameters,
                               HostnameVerifier hostnameVerifier, JedisClusterHostAndPortMap hostAndPortMap) {
        this.jedisClusterNode = jedisClusterNode;
        this.connectionTimeout = connectionTimeout;
        this.soTimeout = soTimeout;
        this.maxAttempts = maxAttempts;
        this.user = user;
        this.password = password;
        this.clientName = clientName;
        this.poolConfig = poolConfig;
        this.ssl = ssl;
        this.sslSocketFactory = sslSocketFactory;
        this.sslParameters = sslParameters;
        this.hostnameVerifier = hostnameVerifier;
        this.hostAndPortMap = hostAndPortMap;
    }

    @Override
    public PooledObject<JedisCluster> makeObject() throws Exception {
        final JedisCluster jedisCluster = new JedisCluster(jedisClusterNode, connectionTimeout, soTimeout,
                maxAttempts, user, password, clientName, poolConfig,
                ssl, sslSocketFactory, sslParameters, hostnameVerifier, hostAndPortMap);
        return new DefaultPooledObject<>(jedisCluster);
    }

    @Override
    public void destroyObject(PooledObject<JedisCluster> pooledJedis) throws Exception {
        final JedisCluster jedisCluster = pooledJedis.getObject();
        try {
            if (jedisCluster != null) {
                jedisCluster.close();
            }
        } catch (Exception e) {
            //do nothing
        }
    }

    @Override
    public void activateObject(PooledObject<JedisCluster> pooledJedis) throws Exception {
        //do nothing
    }

    @Override
    public void passivateObject(PooledObject<JedisCluster> pooledJedis) throws Exception {
        //do nothing
    }

    @Override
    public boolean validateObject(PooledObject<JedisCluster> pooledJedis) {
        //do nothing
        return true;
    }

}