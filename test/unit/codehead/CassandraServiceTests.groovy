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