"""
Tests related to upgrading of schema.
"""
from nose.tools import assert_not_in

from thrift_bindings.v22.ttypes import (CfDef, Column, ColumnDef,
                                        ColumnOrSuperColumn, ColumnParent,
                                        Deletion, Mutation, SlicePredicate,
                                        SliceRange)
from thrift_tests import get_thrift_client
from tools import since
from upgrade_base import UPGRADE_TEST_RUN, SingleNodeUpgradeTester
from upgrade_manifest import build_upgrade_pairs


class TestThriftSchemaUpgrade(SingleNodeUpgradeTester):
    def test_cassandra_11315(self):
        session = self.prepare(start_rpc=True)
        node = self.cluster.nodelist()[0]
        host, port = node.network_interfaces['thrift']

        client = get_thrift_client(host, port)
        client.transport.open()
        client.set_keyspace('ks')

        # create a CF with mixed static and dynamic cols
        column_defs = [ColumnDef('static1', 'Int32Type', None, None, None)]
        cfdef = CfDef(
            keyspace='ks',
            name='cf',
            column_type='Standard',
            comparator_type='AsciiType',
            key_validation_class='AsciiType',
            default_validation_class='AsciiType',
            column_metadata=column_defs)
        client.system_add_column_family(cfdef)

        session = self.do_upgrade(session)

        client = get_thrift_client(host, port)
        client.transport.open()
        client.set_keyspace('ks')

    def test_cassandra_12147(self):
        pass

    def test_cassandra_12023(self):
        pass

specs = [dict(UPGRADE_PATH=p, __test__=UPGRADE_TEST_RUN)
         for p in build_upgrade_pairs()]

for spec in specs:
    suffix = 'Nodes1RF1_{pathname}'.format(pathname=spec['UPGRADE_PATH'].name)
    gen_class_name = TestThriftSchemaUpgrade.__name__ + suffix
    assert_not_in(gen_class_name, globals())
    globals()[gen_class_name] = type(gen_class_name, (TestThriftSchemaUpgrade,), spec)
