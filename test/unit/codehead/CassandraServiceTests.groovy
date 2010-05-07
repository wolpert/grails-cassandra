package codehead;

import grails.test.*
import org.apache.cassandra.thrift.NotFoundException;

/**
 * mockFor requires a groovy object, where the keyspace I'm mocking is
 * a java one. So this class is just a holder.
 */
class MockKeyspace {
}

class CassandraServiceTests extends GrailsUnitTestCase {

    def cassandraService

    protected void setUp() {
        super.setUp()
    }

    protected void tearDown() {
        super.tearDown()
    }

    void testMock() {
        def mockKeyspaceImpl = mockFor(MockKeyspace)
        mockKeyspaceImpl.demand.getName(1..1){ -> return "mock keyspace"}
        def cassandraService = getCassandraService(mockKeyspaceImpl.createMock())

        // run test
        def str = cassandraService.execute{keyspace ->keyspace.getName()}
        assertEquals("mock keyspace", str)
    }

    void testBytesArrayConvertOnBytesArray(){
    	cassandraService = new CassandraService()
    	def obj = new byte[2];
    	obj[0]=5
    	obj[1]=6
    	def result = cassandraService.bytesConvert(obj)
    	assertEquals obj, result
    	assertEquals obj[0], result[0]
    	assertEquals obj[1], result[1]
    }
    
    void testBytesArrayConvertOnString(){
    	cassandraService = new CassandraService()
    	def obj = "this is a string"
    	def result = cassandraService.bytesConvert(obj)
    	assertEquals(byte[].class,result.class)
    	assertEquals(obj,new String(result))
    }
    
    void testExceptionCatcherWithNotFoundException(){
        def mockKeyspaceImpl = mockFor(MockKeyspace)
        mockKeyspaceImpl.demand.getColumn(1..1){arg1, arg2 -> throw new NotFoundException()}
        def cassandraService = getCassandraService(mockKeyspaceImpl.createMock())
        cassandraService.hideNotFoundExceptions=true
        assertNull cassandraService.getColumnValue('colFamName','key','columnName')
    }

    void testExceptionCatcherWithRealException(){
        def mockKeyspaceImpl = mockFor(MockKeyspace)
        mockKeyspaceImpl.demand.getColumn(1..1){arg1, arg2 -> throw new Exception()}
        def cassandraService = getCassandraService(mockKeyspaceImpl.createMock())
        cassandraService.hideNotFoundExceptions=true
        shouldFail{
            cassandraService.getColumnValue('colFamName','key','columnName')
        }
    }

    /**
     * Returns a cassandra service that uess the keyspace for its execute
     */
    def getCassandraService(keyspace){
        cassandraService = new CassandraService()
        mockLogging(CassandraService)
        def emc = new ExpandoMetaClass(CassandraService.class, false)
        emc.execute = { it(keyspace)  }

        emc.initialize()
        cassandraService.metaClass = emc
        return cassandraService
    }

}