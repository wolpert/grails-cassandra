package codehead;

import me.prettyprint.cassandra.service.CassandraClientPool
import me.prettyprint.cassandra.service.CassandraClientPoolFactory
import me.prettyprint.cassandra.service.CassandraClient
import me.prettyprint.cassandra.service.Keyspace
import org.apache.cassandra.service.Column;
import org.apache.cassandra.service.ColumnPath;

import static me.prettyprint.cassandra.utils.StringUtils.bytes;
import static me.prettyprint.cassandra.utils.StringUtils.string;
class CassandraService {

    boolean transactional = true

    def servers=["localhost:9160"]
    def defaultKeyspace="Keyspace1"

    def execute(keyspaceName=defaultKeyspace,block){
        CassandraClientPool pool = CassandraClientPoolFactory.INSTANCE.get();
        CassandraClient client = pool.borrowClient(servers);

        try {
            Keyspace keyspace = client.getKeyspace(keyspaceName)
            return block(keyspace)
        } finally {
            pool.releaseClient(client);
        }
    }

    def columnPathValue(cf,name,key){
        def cp = new ColumnPath(cf,null,bytes(name))
        return execute{ keyspace ->
            string(keyspace.getColumn(key,cp).getValue())
        }
    }

    // sets the new value for this column path, returning the old one
    // if there was one
    def setColumnPathValue(cf,name,key,value){
        def cp = new ColumnPath(cf,null,bytes(name))
        return execute{ keyspace ->
            def old_value = null
            try {old_value = string(keyspace.getColumn(key,cp).getValue())} catch(Throwable t) {} // TODO, only catch not found
            keyspace.insert(key, cp, bytes(value))
            old_value
        }

    }

}
