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
import org.apache.cassandra.service.ColumnParent
import org.apache.cassandra.service.SliceRange
import org.apache.cassandra.service.SliceRange
import org.apache.cassandra.service.SlicePredicate
import org.apache.cassandra.service.ConsistencyLevel

class CassandraService {

    boolean transactional = true

    def servers=["localhost:9160"]
    def defaultKeyspace="Keyspace1"
    def hideNotFoundExceptions=true

    /**
     * Executes the block passing in the available client
     */
    def acquireClient(block){
        log.debug("[acquireClient]")
        CassandraClientPool pool = CassandraClientPoolFactory.INSTANCE.get();
        CassandraClient client = pool.borrowClient(servers);
        try {
            return block(client)
        } finally {
            pool.releaseClient(client)
        }
    }

    /**
     * Executes the closure giving it the keyspace as an argument
     */
    def execute(keyspaceName=defaultKeyspace,block){
        log.debug("[execute] $keyspaceName")
        acquireClient {  client ->
            Keyspace keyspace = client.getKeyspace(keyspaceName);
            return block(keyspace)
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

    /**
     * This will return the value for a specific column family.
     * superColumnName==null
     *   columnFamilyName:{key:{columnName = columnValue}}
     * superColumnName!=null
     *   columnFamilyName:{key:{superColumnName:{columnName = columnValue}}}
     */
    def getColumnValue(columnFamilyName,key,superColumnName=null,columnName){
        log.debug("[getColumnValue] $columnFamilyName $key $superColumnName $columnName")
        def columnPath = getColumnPath(columnFamilyName,superColumnName,columnName)
        execute { keyspace ->
            return exceptionCatcher{string(keyspace.getColumn(key,columnPath).getValue())}
        }
    }

    /**
     * This will set the value for a specific column family.
     * superColumnName==null
     *   columnFamilyName:{key:{columnName = columnValue}}
     * superColumnName!=null
     *   columnFamilyName:{key:{superColumnName:{columnName = columnValue}}}
     */
    def setColumnValue(columnFamilyName,key,superColumnName=null,columnName,value){
        log.debug("[setColumnValue] $columnFamilyName $key $superColumnName $columnName $value")
        def columnPath = getColumnPath(columnFamilyName,superColumnName,columnName)
        execute { keyspace ->
            def old_value =  null
            try {old_value = string(keyspace.getColumn(key,columnPath).getValue())} catch (NotFoundException nfe){;}
            keyspace.insert(key, columnPath, bytes(value))
            return old_value
        }
    }

    /**
     * //supercolumnname/columnfamilyname/key?colname=colval&colname=colval
     */
    def getColumnPath(columnFamilyName,superColumnName=null,name){
        log.debug("[getColumnPath]: $columnFamilyName $superColumnName $name")
        if(superColumnName!=null){
            // This feels backwards...
            return new ColumnPath(columnFamilyName,bytes(superColumnName),bytes(name))
        } else {
            return new ColumnPath(columnFamilyName,null,bytes(name))
        }
    }



}
