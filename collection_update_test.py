import time
from cassandra.query import SimpleStatement
from cassandra import ConsistencyLevel
from cassandra.query import named_tuple_factory
from dtest import Tester, create_ks
from threading import Thread
from tools.assertions import assert_invalid

class TestCollectionUpdates(Tester):
    def test_update_collections(self):
        keyspaceStatement = """
            CREATE KEYSPACE collection_update WITH replication = { 'class':'SimpleStrategy', 'replication_factor':3 } AND DURABLE_WRITES = true;
            """
        udtStatement = """
            CREATE TYPE collection_update.name (
            first text,
            last text
            );
            """
        tableStatement = '''
            CREATE TABLE collection_update.collections (
                id int,
                words list<text>,
                names list<frozen<name>>,
                PRIMARY KEY (id)
                );
            '''
        insertStatement = """
            INSERT INTO collection_update.collections (id, words, names)
            VALUES ({id},[{words}],[{names}])
            """

        #create schema
        cluster = self.cluster
        cluster.populate(3).start(wait_for_binary_proto=True)
        node1, node2, node3 = cluster.nodelist()
        session = self.patient_cql_connection(node1)
        session.execute(keyspaceStatement)
        session.execute(udtStatement)
        session.execute(tableStatement)
        # Make sure the schema propagate
        time.sleep(2)

        class Worker(Thread):

            def __init__(self, tester):
                Thread.__init__(self)
                self.tester = tester


            def run(self):
                # insert initial data
                words = "'the', 'first', 'set', 'of', 'words'"
                names = "{first: 'andrew', last: 'dtest'},{first: 'stephanie', last: 'dtest'}"
                for x in xrange(200):
                    session.execute(insertStatement.format(id=x, words=words, names=names))

                # update with new lists
                words = "'the', 'second', 'set', 'of', 'words'"
                names = "{first: 'charles', last: 'dtest'},{first: 'anne', last: 'dtest'}"
                for y in xrange(100):
                    for x in xrange(200):
                        stmt = SimpleStatement(insertStatement.format(id=x, words=words, names=names), consistency_level=ConsistencyLevel.ONE )
                        session.execute(stmt)

                    # check for too many entries
                    session.row_factory = named_tuple_factory
                    rows = session.execute("select * from collection_update.collections")
                    i = 1
                    for row in rows:
                        self.tester.assertEquals(5,len(row.words))
                        self.tester.assertEquals(2,len(row.names))
                        i += 1
                    print "rows checked " + str(i)

        workers = []
        for n in range(0,5):
            workers.append(Worker(self))

        for w in workers:
            w.start()

        for w in workers:
            w.join()
