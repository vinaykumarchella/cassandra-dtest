from dtest import Tester, debug

from cassandra.concurrent import execute_concurrent_with_args

from jmxutils import JolokiaAgent, make_mbean

class TestUpgradeIndexSummary(Tester):

    def test_upgrade_index_summary(self):
        cluster = self.cluster
        cluster.populate(1)
        node = cluster.nodelist()[0]
        node.set_install_dir(version='2.0.12')
        cluster.start()

        session = self.patient_cql_connection(node)
        session.execute("CREATE KEYSPACE testindexsummary WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}")
        session.set_keyspace("testindexsummary")
        session.execute("CREATE TABLE test (k int PRIMARY KEY, v int")

        insert_statement = session.prepare("INSERT INTO test (k, v) VALUES (? , ?)")
        execute_concurrent_with_args(insert_statement, [(i, i) for i in range(128 * 128)])

        session.cluster.shutdown()

        node.drain()
        node.watch_log_for("DRAINED")
        node.set_install_dir(version='git:cassandra-2.1')
        debug("Set new cassandra dir for %s: %s" % (node.name, node.get_install_dir()))

        # setup log4j / logback again (necessary moving from 2.0 -> 2.1)
        node.set_log_level("INFO")
        node.start()

        session = self.patient_cql_connection(node)

        mbean = make_mbean('db', 'IndexSummaries')
        with JolokiaAgent(node) as jmx:
            avg_interval = jmx.read_attribute(mbean, 'AverageIndexInterval')
            self.assertEqual(128.0, avg_interval)

            # force downsampling of the index summary (if it were allowed)
            jmx.write_attribute(mbean, 'MemoryPoolCapacityInMB', 0)
            jmx.execute_method(mbean, 'redistributeSummaries')

            avg_interval = jmx.read_attribute(mbean, 'AverageIndexInterval')
            self.assertEqual(128.0, avg_interval)
