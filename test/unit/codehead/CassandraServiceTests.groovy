package codehead;
import java.nio.ByteBuffer;

import grails.test.*
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.MutationResult;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.ColumnQuery;
import me.prettyprint.hector.api.query.QueryResult;

import org.apache.cassandra.thrift.NotFoundException;

class CassandraServiceTests extends GrailsUnitTestCase {

    def cassandraService

    protected void setUp() {
        super.setUp()
		mockLogging(CassandraService,true)
		cassandraService = new CassandraService()
    }

    protected void tearDown() {
        super.tearDown()
    }
	
	void testExceptionBlocker(){
		def result = cassandraService.exceptionCatcher {
			throw new NotFoundException()
		}
		assertNull(result) // and if you got not error, its all good.
	}
	
	/**
	 * The standard hector test, before the service refactoring...
	 */
	void testSetGetDeleteOldStyle(){
		
		String cf = "Standard1"
		String method = "testSetGetDeleteOldStyle"
		String key = "${method}_Key"
		String name = "${method}_Name"
		String value = "${method}_Value"
		
		Mutator<String> m = HFactory.createMutator(cassandraService.keyspace(),cassandraService.serializer(String.class));
		MutationResult mr = m.insert(key, cf, HFactory.createColumn(name, value, cassandraService.serializer(String.class), cassandraService.serializer(String.class)));
		
		ColumnQuery<String, String, String> q = HFactory.createColumnQuery(cassandraService.keyspace(), cassandraService.serializer(String.class), cassandraService.serializer(String.class), cassandraService.serializer(String.class))
		q.setName(name).setColumnFamily(cf)
		QueryResult<HColumn<String, String>> r = q.setKey(key).execute();
		assertNotNull(r);

		HColumn<String, String> c = r.get();
		assertNotNull(c);
		String foundvalue = c.getValue();
		assertEquals(value, foundvalue);
		String foundname = c.getName();
		assertEquals(name, foundname);
		assertEquals(q, r.getQuery());
		
		m = HFactory.createMutator(cassandraService.keyspace(), cassandraService.serializer(String.class));
		MutationResult mr2 = m.delete(key, cf, name, cassandraService.serializer(String.class));
		
		// get already removed value
		ColumnQuery<String, String, String> q2 = HFactory.createColumnQuery(cassandraService.keyspace(), cassandraService.serializer(String.class), cassandraService.serializer(String.class), cassandraService.serializer(String.class));
		q2.setName(name).setColumnFamily(cf);
		QueryResult<HColumn<String, String>> r2 = q2.setKey(key).execute();
		assertNotNull(r2);
		assertNull("Value should have been deleted", r2.get())
	}
	/**
	 * The standard hector test, before the service refactoring...
	 */
	void testSetGetDeleteGroovyStyle(){
		
		String cf = "Standard1"
		String method = "testSetGetDeleteGroovyStyle"
		String key = "${method}_Key"
		String name = "${method}_Name"
		String value = "${method}_Value"
		
		cassandraService.setValue(key,cf,name,value)
		
		def result = cassandraService.getValue(key,cf,name,String.class)
		assertNotNull(result);
		assertEquals(value,result)

		cassandraService.deleteValue(key,cf,name)
		assertNull("Value should have been deleted", cassandraService.getValue(key,cf,name,String.class))
	}

}