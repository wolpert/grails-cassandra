package codehead;

import me.prettyprint.cassandra.service.CassandraClientPool
import me.prettyprint.cassandra.service.CassandraClientPoolFactory
import me.prettyprint.cassandra.service.CassandraClient
import me.prettyprint.cassandra.service.Keyspace

import static me.prettyprint.cassandra.utils.StringUtils.bytes;
import static me.prettyprint.cassandra.utils.StringUtils.string;

import org.apache.cassandra.service.Column;
import org.apache.cassandra.service.ColumnPath;
import org.apache.cassandra.service.NotFoundException;

class CassandraService {

    boolean transactional = true

    def servers=["localhost:9160"]
    def defaultKeyspace="Keyspace1"
    def hideNotFoundExceptions=true

    /**
     * Excetues the closure giving it the keyspace as an argument
     */
    def execute(keyspaceName=defaultKeyspace,block){
        CassandraClientPool pool = CassandraClientPoolFactory.INSTANCE.get();
        CassandraClient client = pool.borrowClient(servers);
        try {
            Keyspace keyspace = client.getKeyspace(keyspaceName)
            return exceptionCatcher(){block(keyspace)} // Return from here? All good
        } finally {
            pool.releaseClient(client);
        }
    }

    /**
     * Calls the closure, hiding any NotFoundExceptions only if hideNotFoundExceptions=true
     */
    def exceptionCatcher(block){
        try {
            return block()
        } catch (NotFoundException nfe){
            if (hideNotFoundExceptions){
                return null;
            } else {
                throw(nfe)
            }
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
            try {old_value = string(keyspace.getColumn(key,cp).getValue())} catch(NotFoundException nfe) {} // Only catch the expected exception
            keyspace.insert(key, cp, bytes(value)) // I do not think this can cause a NotFoundException...
            old_value
        }

    }

}
