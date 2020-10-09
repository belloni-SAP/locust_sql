import time
import sys
from locust import User, task, between
from config import default_info_hana

try:
    from hdbcli import dbapi as hana_driver
except ImportError:
    import pyhdb as hana_driver


class HanaClient():
    def __init__(self, environment):
        self._connection = None
        self._cursor = None
        self._environment = environment

    def connect(self):
        connection_config = default_info_hana
        self._connection = hana_driver.connect(
            address=connection_config['host'],
            port=connection_config['port'],
            user=connection_config['user'],
            password=connection_config['password'],
        )
        self._cursor = self._connection.cursor()

    def disconnect(self):
        try:
            self._connection.close()  # Throws if connection is already closed
        except Exception:
            pass

    def execute(self, callback, req_type, name):
        """Executes the callback. The callback receives the pyhdb cursor as first argument.
        :param callback: callback
        :param req_type: Type of request
        :param name: Name of the executed statement
        :returns: Return value of the callback
        """
        start_time = time.time()
        try:
            # print("Calling '{}'".format(callback))
            result = callback(self._cursor, self._connection)
        except Exception as e:  # what type??
            # print("Exception ==> {}".format(str(e)))
            total_time = int((time.time() - start_time) * 1000)
            self._environment.events.request_failure.fire(
                request_type=req_type,
                name=name, response_time=total_time,
                exception=e, response_length=0)
        else:
            # print("OK ==> {}".format(result))
            total_time = int((time.time() - start_time) * 1000)
            self._environment.events.request_success.fire(
                request_type=req_type,
                name=name, response_time=total_time,
                response_length=0)
            return result


class HanaUser_base(User):
    abstract = True  # Locust doesn't instantiate this class

    # The parent parameter is used by locust internally
    def __init__(self, parent):
        super(HanaUser_base, self).__init__(parent)
        # Client needs a reference to the environment to register events for the stats
        self._client = HanaClient(self.environment)
        self._client.connect()

    def __del__(self):
        self._client.disconnect()


# ============================================================================ #

class GenericSqlTest(HanaUser_base):
    wait_time = between(0, 0)

    def __init__(self, parent):
        super().__init__(parent)
        self._list_queries = [
            ["0", """SELECT "Food_1"."device" AS "device" FROM "Food_1" GROUP BY "Food_1"."device" ORDER BY "device" ASC ;"""]
        ]
        self._index = 0
        self._global_count = 0

    def on_start(self):
        for q in self._list_queries:
            print(q[0], " ===> ", q[1])

    @task
    def run_query(self):
        def many_select_impl(cursor, connection):
            query = self._list_queries[self._index][1]
            print("Running query {}".format(query))
            cursor.execute(query)

        start_time = time.time()
        result = self._client.execute(
            many_select_impl,
            'SQL',
            self._list_queries[self._index][0]
        )
        self._index += 1
        if self._index >= len(self._list_queries):
            self._index = 0

        t = int((time.time() - start_time) * 1000)
        print("  sending request {} - {} ms)\n".format(self._global_count, t),
              file=sys.stderr, end='')
        self._global_count += 1

