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
        //CassandraClient client = pool.borrowClient("localhost", 9160);
        CassandraClient client = pool.borrowClient(servers);
        // A load balanced version would look like this:
        // CassandraClient client = pool.borrowClient(new String[] {"cas1:9160", "cas2:9160", "cas3:9160"});

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

}
