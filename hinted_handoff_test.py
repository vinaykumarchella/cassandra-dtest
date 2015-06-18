from dtest import Tester, debug, DISABLE_VNODES
from ccmlib.node import Node, NodeError, TimeoutError
from cassandra import ConsistencyLevel, Unavailable, ReadTimeout
from cassandra.query import SimpleStatement
from tools import since, InterruptBootstrap
from time import time, sleep
from assertions import assert_none, assert_invalid
from jmxutils import make_mbean, JolokiaAgent, remove_perf_disable_shared_mem


class TestHintedHandoff(Tester):

    def check_delivery(self, node):
        node2_mbean = make_mbean('metrics', type='HintedHandoffManager', name='Hints_created-127.0.0.2')
        node3_mbean = make_mbean('metrics', type='HintedHandoffManager', name='Hints_created-127.0.0.3')
        handedoff = False
        timeout = time() + 90.00
        while not handedoff and time() < timeout:
            with JolokiaAgent(node) as jmx:
                try:
                    node2 = jmx.read_attribute(node2_mbean, 'Count')
                    node3 = jmx.read_attribute(node3_mbean, 'Count')
                    if node2 == "10200" and node3 == "10200":
                        handedoff = True
                    else:
                        debug(node3)
                        debug(node2)
                        self.fail()
                except Exception,e:
                    debug(str(e))
        return handedoff

    @since('3.0')
    def simple_functionality_test(self):
        """
        Simple Test - check hints are delivered, hint file created/removed
        - bring up 2 node cluster with rf = 2
        - take down node 2
        - write data to node 1, data should include updates and deletes (so to test mutations on same key)
        - check that flat file is created
        - bring up node 2
        - force/allow hint delivery
        - check that flat file is deleted
        - take down node 1 and verify data using cl=one
        """
        cluster = self.cluster
        cluster.populate(3)
        [node1, node2, node3] = cluster.nodelist()

        remove_perf_disable_shared_mem(node1)
        cluster.start(wait_for_binary_proto=True)

        #create ks and table with rf 3
        cursor = self.patient_cql_connection(node1)
        ksq = "CREATE KEYSPACE hhtest WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor':3}"
        cfq = "CREATE TABLE hhtest.hhtab(key int primary key, val int);"
        cursor.execute(ksq)
        cursor.execute(cfq)

        node2.stop(wait_other_notice=True, gently=False)
        node3.stop(wait_other_notice=True, gently=False)

        numhints=10000
        for x in range(0, numhints):
            insq = "INSERT INTO hhtest.hhtab(key,val) VALUES ({key}, {key})".format(key=str(x))
            cursor.execute(insq)

        for x in range(0, 100):
            delq = "DELETE FROM hhtest.hhtab WHERE key = {key}".format(key=str(x))
            cursor.execute(delq)

        for x in range(100, 200):
            updateq = "INSERT INTO hhtest.hhtab(key,val) VALUES ({key}, 100000)".format(key=str(x))
            cursor.execute(updateq)

        node2.start()
        node3.start()

        handedoff = self.check_delivery(node1)

        self.assertTrue(handedoff)

        node1.stop(gently=False)

        for node in [node2,node3]:
            othernode = [x for x in [node2, node3] if not node == x][0]
            othernode.stop(gently=False)
            cursor = self.patient_cql_connection(node)
            for x in range(200, numhints):
                query = "SELECT val from hhtest.hhtab WHERE key={key};".format(key=str(x))
                results = cursor.execute(query)
                self.assertEqual(results[0][0], x)
            for x in range(100, 200):
                query = "SELECT val from hhtest.hhtab WHERE key={key};".format(key=str(x))
                results = cursor.execute(query)
                self.assertEqual(results[0][0], 100000)
            for x in range(0, 100):
                query = "SELECT val from hhtest.hhtab WHERE key={key};".format(key=str(x))
                assert_none(cursor, query)
            othernode.start()

    @since('3.0')
    def upgrade_versions_test(self):
        """
        tests upgrading node with existing hints
        - bring up 3 node cluster with rf=3 - cassandra-2.2
        - take down node2, node3
        - write data to node 1, allowing hints to build up (check using system.hints)
        - take down node 1
        - upgrade node 1 to cassandra-3.0
        - bring up node 2 & 3, wait for hint delivery
        - bring down node 1, query node 2 at cl=one to verify all data is present
        - similarly query node 3
        """

        cluster = self.cluster
        cluster.set_install_dir(version="2.2")
        cluster.populate(3).start()
        [node1, node2, node3] = cluster.nodelist()

        #create ks and table with rf 3
        cursor = self.patient_cql_connection(node1)
        ksq = "CREATE KEYSPACE hhtest WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor':3}"
        cfq = "CREATE TABLE hhtest.hhtab(key int primary key, val int);"
        cursor.execute(ksq)
        cursor.execute(cfq)

        node2.stop(wait_other_notice=True, gently=False)
        node3.stop(wait_other_notice=True, gently=False)

        numhints=10000
        for x in range(0, numhints):
            insq = "INSERT INTO hhtest.hhtab(key,val) VALUES ({key}, {key})".format(key=str(x))
            cursor.execute(insq)

        for x in range(0, 100):
            delq = "DELETE FROM hhtest.hhtab WHERE key = {key}".format(key=str(x))
            cursor.execute(delq)

        for x in range(100, 200):
            updateq = "INSERT INTO hhtest.hhtab(key,val) VALUES ({key}, 100000)".format(key=str(x))
            cursor.execute(updateq)

        node1.stop(gently=False)
        node1.set_install_dir(version = '3.0')
        node1.start()

        #check system.hints truncated
        assert_invalid(cursor, "SELECT * FROM system.hints LIMIT 10")

        node2.set_install_dir(version = '3.0')
        node3.set_install_dir(version = '3.0')

        node2.start()
        node3.start()

        handedoff = self.check_delivery(node1)

        self.assertTrue(handedoff)

        node1.stop(gently=False)

        for node in [node2,node3]:
            othernode = [x for x in [node2, node3] if not node == x][0]
            othernode.stop(gently)
            cursor = self.patient_cql_connection(node)
            for x in range(200, numhints):
                query = "SELECT val from hhtest.hhtab WHERE key={key};".format(key=str(x))
                results = cursor.execute(query)
                self.assertEqual(results[0][0], x)
            for x in range(100, 200):
                query = "SELECT val from hhtest.hhtab WHERE key={key};".format(key=str(x))
                results = cursor.execute(query)
                self.assertEqual(results[0][0], 100000)
            for x in range(0, 100):
                query = "SELECT val from hhtest.hhtab WHERE key={key};".format(key=str(x))
                assert_none(cursor, query)
            othernode.start(wait=True)

    @since('3.0')
    def nodetool_commands_test(self):
        """
        Check nodetool pausehandoff, resumehandoff, sethintedhandoffthrottlekb, statushandoff
        - bring up 3 node cluster with rf = 3
        - sethintedhandoffthrottlekb to low kb
        - take down node, write data to others
        - bring up node, force hint delivery
        - check statushandoff is reporting accurate stats
        - try pausing handoff, check status again to ensure, resume handoff
        - verify data
        """
        cluster = self.cluster
        cluster.populate(3).start()
        [node1, node2, node3] = cluster.nodelist()

        #create ks and table with rf 3
        cursor = self.patient_cql_connection(node1)
        ksq = "CREATE KEYSPACE hhtest WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor':3}"
        cfq = "CREATE TABLE hhtest.hhtab(key int primary key, val int);"
        cursor.execute(ksq)
        cursor.execute(cfq)

        node1.nodetool("sethintedhandoffthrottlekb 10")

        node3.stop(gently=False)

        numhints = 1000     
        for x in range(0, numhints):
            insq = SimpleStatement("INSERT INTO hhtest.hhtab(key,val) VALUES ({key}, {key})".format(key=str(x)), consistency_level=ConsistencyLevel.TWO)
            cursor.execute(insq)
        
        node3.start()

        notstarted = True
        while notstarted:
            output = node1.nodetool("statushandoff", capture_output=True)
            if "running" in output:
                notstarted = False

        node1.nodetool("pausehandoff")

        output = node1.nodetool("statushandoff", capture_output=True)
        self.assertTrue("paused" in output)
        
        node1.nodetool("resumehandoff")

        output = node1.nodetool("statushandoff", capture_output=True)
        self.assertTrue("running" in output)

        handedoff, pending_nodes = self.check_delivery(node1)
        self.assertTrue(handedoff, msg=pending_nodes)