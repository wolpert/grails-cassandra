
beans = {
    cassandraService(codehead.CassandraService){
		servers="localhost:9160"
		keyspaceName="Keyspace1"
		clusterName="Main"
        hideNotFoundExceptions=true
    }
}
