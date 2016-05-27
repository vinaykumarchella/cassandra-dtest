import re

from cassandra import (ConsistencyLevel, InvalidRequest, ReadFailure,
                       ReadTimeout, Unauthorized, Unavailable, WriteFailure,
                       WriteTimeout)
from cassandra.query import SimpleStatement

from tools import rows_to_list


def assert_unavailable(fun, *args):
    """
    Attempt to execute a function, and assert Unavailable exception is raised.
    @param fun Function to be executed
    @param *args Arguments to be passed to the function
    """
    try:
        if len(args) == 0:
            fun(None)
        else:
            fun(*args)
    except (Unavailable, WriteTimeout, WriteFailure, ReadTimeout, ReadFailure) as e:
        pass
    except Exception as e:
        assert False, "Expecting unavailable exception, got: " + str(e)
    else:
        assert False, "Expecting unavailable exception but no exception was raised"


def assert_invalid(session, query, matching=None, expected=InvalidRequest):
    """
    Attempt to issue a query and assert that the query is invalid.
    @param session Session to use
    @param query Invalid query to run
    @param matching Optional error message string contained within excepted exception
    @param expected Exception expected to be raised by the invalid query
    """
    try:
        res = session.execute(query)
        assert False, "Expecting query to be invalid: got {}".format(res)
    except AssertionError as e:
        raise e
    except expected as e:
        msg = str(e)
        if matching is not None:
            assert re.search(matching, msg), "Error message does not contain " + matching + " (error = " + msg + ")"


def assert_unauthorized(session, query, message):
    """
    Attempt to issue a query, and assert Unauthorized is raised.
    @param session Session to use
    @param query Unauthorized query to run
    @param message Expected error message
    """
    assert_invalid(session, query, message, Unauthorized)


def assert_one(session, query, expected, cl=ConsistencyLevel.ONE):
    """
    Assert query returns one row.
    @param session Session to use
    @param query Query to run
    @param expected Expected results from query
    @param cl Optional Consistency Level setting. Default ONE
    """
    simple_query = SimpleStatement(query, consistency_level=cl)
    res = session.execute(simple_query)
    list_res = rows_to_list(res)
    assert list_res == [expected], "Expected {} from {}, but got {}".format([expected], query, list_res)


def assert_none(session, query, cl=ConsistencyLevel.ONE):
    """
    Assert query returns nothing
    @param session Session to use
    @param query Query to run
    @param cl Optional Consistency Level setting. Default ONE
    """
    simple_query = SimpleStatement(query, consistency_level=cl)
    res = session.execute(simple_query)
    list_res = rows_to_list(res)
    assert list_res == [], "Expected nothing from {}, but got {}".format(query, list_res)


def assert_all(session, query, expected, cl=ConsistencyLevel.ONE, ignore_order=False):
    """
    Assert query returns all expected items optionally in the correct order
    @param session Session in use
    @param query Query to run
    @param expected Expected results from query
    @param cl Optional Consistency Level setting. Default ONE
    @param ignore_order Optional boolean flag determining whether response is ordered
    """
    simple_query = SimpleStatement(query, consistency_level=cl)
    res = session.execute(simple_query)
    list_res = rows_to_list(res)
    if ignore_order:
        expected = sorted(expected)
        list_res = sorted(list_res)
    assert list_res == expected, "Expected {} from {}, but got {}".format(expected, query, list_res)


def assert_almost_equal(*args, error=0.16, error_message=''):
    """
    Assert variable number of arguments all fall within a margin of error.
    @params *args variable number of numerical arguments to check
    @params error Optional margin of error. Default 0.16
    @params error_message Optional error message to print. Default ''
    """
    vmax = max(args)
    vmin = min(args)
    assert vmin > vmax * (1.0 - error) or vmin == vmax, "values not within {.2f}% of the max: {} ({})".format(error * 100, args, error_message)


def assert_row_count(session, table_name, expected):
    """ Function to validate the row count expected in table_name """

    query = "SELECT count(*) FROM {};".format(table_name)
    res = session.execute(query)
    count = res[0][0]
    assert count == expected, "Expected a row count of {} in table '{}', but got {}".format(
        expected, table_name, count
    )


def assert_crc_check_chance_equal(session, table, expected, ks="ks", view=False):
    """
    driver still doesn't support top-level crc_check_chance property,
    so let's fetch directly from system_schema
    """
    if view:
        assert_one(session,
                   "SELECT crc_check_chance from system_schema.views WHERE keyspace_name = 'ks' AND "
                   "view_name = '{table}';".format(table=table),
                   [expected])
    else:
        assert_one(session,
                   "SELECT crc_check_chance from system_schema.tables WHERE keyspace_name = 'ks' AND "
                   "table_name = '{table}';".format(table=table),
                   [expected])
