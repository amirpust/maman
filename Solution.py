import string
from typing import List
import Utility.DBConnector as Connector
from Utility.ReturnValue import ReturnValue
from Utility.Exceptions import DatabaseException
from Business.Query import Query
from Business.RAM import RAM
from Business.Disk import Disk
from psycopg2 import sql


def createTables():
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute("BEGIN;\
                         CREATE TABLE Queries\
                             (QueryID INTEGER NOT NULL PRIMARY KEY,\
                              QueryPurpose TEXT NOT NULL,\
                              QuerySize INTEGER NOT NULL,\
                              CHECK(QueryID > 0),\
                              CHECK(QuerySize >= 0));\
                         CREATE TABLE Disks\
                             (DiskID INTEGER NOT NULL PRIMARY KEY,\
                              DiskCompany TEXT NOT NULL,\
                              DiskSpeed INTEGER NOT NULL,\
                              DiskFreeSpace INTEGER NOT NULL,\
                              DiskCostPerByte INTEGER NOT NULL,\
                              CHECK(DiskID > 0),\
                              CHECK(DiskSpeed > 0),\
                              CHECK(DiskCostPerByte > 0),\
                              CHECK(DiskFreeSpace >= 0));\
                         CREATE TABLE Rams\
                             (RamID INTEGER NOT NULL PRIMARY KEY,\
                              RamSize INTEGER NOT NULL,\
                              RamCompany TEXT NOT NULL,\
                              CHECK(RamID > 0),\
                              CHECK(RamSize > 0));\
                         CREATE TABLE QueriesOnDisks\
                             (QueryID INTEGER REFERENCES Queries\
                              ON DELETE CASCADE,\
                              DiskID INTEGER REFERENCES Disks\
                              ON DELETE CASCADE,\
                              Cost INTEGER, \
                              PRIMARY KEY(QueryID, DiskID),\
                              CHECK(Cost >= 0));\
                         CREATE TABLE RamsOnDisks\
                             (RamID INTEGER REFERENCES Rams\
                              ON DELETE CASCADE,\
                              DiskID INTEGER REFERENCES Disks\
                              ON DELETE CASCADE,\
                              RamSize INTEGER,\
                              PRIMARY KEY(RamID, DiskID),\
                              CHECK(RamSize >= 0));\
                         CREATE VIEW QueriesCanBeAddedOnDisks AS \
                         (SELECT Q.QueryID, D.DiskID, Q.QuerySize FROM Queries Q, Disks D WHERE QuerySize <= DiskFreeSpace);\
                    COMMIT;")
        conn.commit()
    except Exception as e:
        print(e)
    finally:
        # will happen any way after code try termination or exception handling
        conn.close()


def clearTables():
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute("BEGIN;\
                         DELETE FROM Queries;\
                         DELETE FROM Disks;\
                         DELETE FROM Rams;\
                         DELETE FROM QueriesOnDisks;\
                         DELETE FROM RamsOnDisks;\
                      COMMIT;")
        conn.commit()
    except Exception as e:
        print(e)
    finally:
        # will happen any way after code try termination or exception handling
        conn.close()


def dropTables():
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute("BEGIN;\
                         DROP TABLE IF EXISTS Queries CASCADE;\
                         DROP TABLE IF EXISTS Disks CASCADE;\
                         DROP TABLE IF EXISTS Rams CASCADE;\
                         DROP TABLE IF EXISTS QueriesOnDisks CASCADE;\
                         DROP TABLE IF EXISTS RamsOnDisks CASCADE;\
                         DROP VIEW IF EXISTS QueriesCanBeAddedOnDisks;\
                      COMMIT;")
        conn.commit()
    except Exception as e:
        print(e)
    finally:
        # will happen any way after code try termination or exception handling
        conn.close()


def addQuery(queryToInsert: Query) -> ReturnValue:
    conn = None
    res = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "INSERT INTO Queries(QueryID, QueryPurpose, QuerySize) VALUES({queryID}, {queryPurpose}, {querySize})").format(
            queryID=sql.Literal(queryToInsert.getQueryID()),
            queryPurpose=sql.Literal(queryToInsert.getPurpose()),
            querySize=sql.Literal(queryToInsert.getSize()))
        rows_effected, _ = conn.execute(query)
        conn.commit()
    except DatabaseException.UNIQUE_VIOLATION as e:
        res = ReturnValue.ALREADY_EXISTS
    except DatabaseException.NOT_NULL_VIOLATION as e:
        res = ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        res = ReturnValue.BAD_PARAMS
    except Exception as e:
        res = ReturnValue.ERROR
    finally:
        conn.close()

    return res


def getQueryProfile(queryID: int) -> Query:
    conn = None
    result = Query.badQuery()
    try:
        conn = Connector.DBConnector()
        rows_effected, returnedResultSet = conn.execute(
            sql.SQL("SELECT * FROM Queries WHERE QueryID = {QueryID}").format(
                QueryID=sql.Literal(queryID)))
        assert rows_effected <= 1  # at most 1 query is returned
        if rows_effected == 1:
            resultItem = returnedResultSet.__getitem__(0)
            result = Query(
                resultItem[returnedResultSet.cols_header[0]],
                resultItem[returnedResultSet.cols_header[1]],
                resultItem[returnedResultSet.cols_header[2]])
    except Exception as e:
        print(e)
    finally:
        conn.close()
        return result


def deleteQuery(query: Query) -> ReturnValue:
    conn = None
    res = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        conn.execute(
            sql.SQL("BEGIN;\
                        UPDATE Disks SET DiskFreeSpace = DiskFreeSpace +\
                        {QuerySize} WHERE DiskID IN (SELECT DiskID FROM QueriesOnDisks WHERE QueryID = {QueryID});\
                        DELETE FROM Queries WHERE QueryID = {QueryID};\
                     COMMIT;").format(QueryID=sql.Literal(query.getQueryID()), QuerySize=sql.Literal(query.getSize())))
        conn.commit()
    except DatabaseException.NOT_NULL_VIOLATION as e:
        res = ReturnValue.OK
        conn.rollback()
    except Exception as e:
        print(e)
        res = ReturnValue.ERROR
    finally:
        conn.close()

    return res


def addDisk(disk: Disk) -> ReturnValue:
    conn = None
    res = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "INSERT INTO Disks(DiskID, DiskCompany, DiskSpeed, DiskFreeSpace, DiskCostPerByte) VALUES({diskID}, {diskCompany}, {diskSpeed}, {diskFreeSpace}, {diskCostPerByte})").format(
            diskID=sql.Literal(disk.getDiskID()), diskCompany=sql.Literal(disk.getCompany()),
            diskSpeed=sql.Literal(disk.getSpeed()),
            diskFreeSpace=sql.Literal(disk.getFreeSpace()), diskCostPerByte=sql.Literal(disk.getCost()))

        rows_effected, _ = conn.execute(query)
        conn.commit()
    except DatabaseException.UNIQUE_VIOLATION as e:
        res = ReturnValue.ALREADY_EXISTS
    except DatabaseException.NOT_NULL_VIOLATION as e:
        res = ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        res = ReturnValue.BAD_PARAMS
    except Exception as e:
        res = ReturnValue.ERROR
    finally:
        conn.close()

    return res


def getDiskProfile(diskID: int) -> Disk:
    conn = None
    result = Disk.badDisk()
    try:
        conn = Connector.DBConnector()
        rows_effected, returnedResultSet = conn.execute(
            sql.SQL("SELECT * FROM Disks WHERE DiskID={diskID}").format(
                diskID=sql.Literal(diskID)), True)
        assert rows_effected <= 1  # at most 1 query is returned
        if rows_effected == 1:
            resultItem = returnedResultSet.__getitem__(0)
            result = Disk(
                resultItem[returnedResultSet.cols_header[0]],
                resultItem[returnedResultSet.cols_header[1]],
                resultItem[returnedResultSet.cols_header[2]],
                resultItem[returnedResultSet.cols_header[3]],
                resultItem[returnedResultSet.cols_header[4]])
    except Exception as e:
        print(e)
    finally:
        # will happen any way after code try termination or exception handling
        conn.close()

    return result


def deleteDisk(diskID: int) -> ReturnValue:
    res = ReturnValue.OK
    conn = None
    try:
        conn = Connector.DBConnector()
        rows_effected, _ = conn.execute(
            sql.SQL("DELETE FROM Disks WHERE DiskID = {DiskID}").format(DiskID=sql.Literal(diskID)))
        conn.commit()
        if rows_effected == 0:
            return ReturnValue.NOT_EXISTS
    except Exception as e:
        print(e)
        res = ReturnValue.ERROR
    finally:
        conn.close()

    return res


def addRAM(ramToInsert: RAM) -> ReturnValue:
    conn = None
    res = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "INSERT INTO Rams(RamID, RamSize, RamCompany) VALUES({ramID}, {ramSize}, {ramCompany})").format(
            ramID=sql.Literal(ramToInsert.getRamID()), ramSize=sql.Literal(ramToInsert.getSize()),
            ramCompany=sql.Literal(ramToInsert.getCompany()))
        rows_effected, _ = conn.execute(query)
        conn.commit()
    except DatabaseException.UNIQUE_VIOLATION as e:
        res = ReturnValue.ALREADY_EXISTS
    except DatabaseException.NOT_NULL_VIOLATION as e:
        res = ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        res = ReturnValue.BAD_PARAMS
    except Exception as e:
        res = ReturnValue.ERROR
    finally:
        conn.close()

    return res


def getRAMProfile(ramID: int) -> RAM:
    conn = None
    result = RAM.badRAM()
    try:
        conn = Connector.DBConnector()
        rows_effected, returnedResultSet = conn.execute(
            sql.SQL("SELECT * FROM Rams WHERE RamID = {RamID}").format(
                RamID=sql.Literal(ramID)))
        assert rows_effected <= 1  # at most 1 query is returned
        if rows_effected == 1:
            resultItem = returnedResultSet.__getitem__(0)
            result = RAM(
                resultItem[returnedResultSet.cols_header[0]],
                resultItem[returnedResultSet.cols_header[2]],
                resultItem[returnedResultSet.cols_header[1]])
    except Exception as e:
        print(e)
    finally:
        conn.close()

    return result


def deleteRAM(ramID: int) -> ReturnValue:
    conn = None
    res = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        rows_effected, _ = conn.execute(
            sql.SQL("DELETE FROM Rams WHERE RamID = {RamID}").format(RamID=sql.Literal(ramID)))
        conn.commit()
        if rows_effected == 0:
            return ReturnValue.NOT_EXISTS
    except Exception as e:
        print(e)
        res = ReturnValue.ERROR
    finally:
        conn.close()

    return res


def addDiskAndQuery(disk: Disk, queryToInsert: Query) -> ReturnValue:
    conn = None
    res = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "BEGIN;\
                INSERT INTO Disks(DiskID, DiskCompany, DiskSpeed, DiskFreeSpace, DiskCostPerByte)\
                    VALUES({diskID}, {diskCompany}, {diskSpeed}, {diskFreeSpace}, {diskCostPerByte});\
                INSERT INTO Queries(QueryID, QueryPurpose, QuerySize)\
                    VALUES({queryID}, {queryPurpose}, {querySize});\
             COMMIT;").format(
            diskID=sql.Literal(disk.getDiskID()),
            diskCompany=sql.Literal(disk.getCompany()),
            diskSpeed=sql.Literal(disk.getSpeed()),
            diskFreeSpace=sql.Literal(disk.getFreeSpace()),
            diskCostPerByte=sql.Literal(disk.getCost()),
            queryID=sql.Literal(queryToInsert.getQueryID()),
            queryPurpose=sql.Literal(queryToInsert.getPurpose()),
            querySize=sql.Literal(queryToInsert.getSize()))

        rows_effected, _ = conn.execute(query)
        conn.commit()
    except DatabaseException.UNIQUE_VIOLATION as e:
        conn.rollback()
        res = ReturnValue.ALREADY_EXISTS
    except DatabaseException.NOT_NULL_VIOLATION as e:
        conn.rollback()
        res = ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        conn.rollback()
        res = ReturnValue.BAD_PARAMS
    except Exception as e:
        print(e)
        conn.rollback()
        res = ReturnValue.ERROR
    finally:
        conn.close()

    return res


# checked should be working
def addQueryToDisk(query: Query, diskID: int) -> ReturnValue:
    conn = None
    res = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "BEGIN;\
                INSERT INTO QueriesOnDisks(QueryID, DiskID, Cost) VALUES ({queryID}, {diskID}, {querySize} * \
                (SELECT DiskCostPerByte FROM Disks WHERE DiskID = {diskID}));\
                UPDATE Disks SET DiskFreeSpace = DiskFreeSpace - {querySize} WHERE DiskID = {diskID};\
            COMMIT;").format(diskID=sql.Literal(diskID), queryID=sql.Literal(query.getQueryID()),
                             querySize=sql.Literal(query.getSize()))

        rows_effected, _ = conn.execute(query)
        conn.commit()
    except DatabaseException.UNIQUE_VIOLATION as e:
        res = ReturnValue.ALREADY_EXISTS
    except DatabaseException.NOT_NULL_VIOLATION as e:
        res = ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        res = ReturnValue.BAD_PARAMS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        res = ReturnValue.NOT_EXISTS
    except Exception as e:
        print(e)
        res = ReturnValue.ERROR
    finally:
        conn.close()

    return res


# checked should be working
def removeQueryFromDisk(query: Query, diskID: int) -> ReturnValue:
    conn = None
    res = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("BEGIN;\
        UPDATE Disks SET DiskFreeSpace = DiskFreeSpace + (SELECT QuerySize FROM Queries WHERE QueryID = (SELECT QueryID FROM QueriesOnDisks WHERE QueryID = {queryID} AND DiskID = {diskID})) WHERE DiskID = {diskID};\
        DELETE FROM QueriesOnDisks WHERE DiskID = {diskID} AND QueryID = {queryID};\
        COMMIT;").format(
            diskID=sql.Literal(diskID),
            queryID=sql.Literal(query.getQueryID()), querySize=sql.Literal(query.getSize()))

        rows_effected, _ = conn.execute(query)
        conn.commit()

    except DatabaseException.NOT_NULL_VIOLATION as e:
        res = ReturnValue.OK
        conn.rollback()
    except Exception as e:
        res = ReturnValue.ERROR
        conn.rollback()
    finally:
        conn.close()

    return res


# checked should be working
def addRAMToDisk(ramID: int, diskID: int) -> ReturnValue:
    conn = None
    res = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("INSERT INTO RamsOnDisks(RamID, DiskID,RamSize) VALUES \
        ({ramID}, {diskID}, (SELECT RamSize FROM Rams WHERE ramID = {ramID}))").format(
            diskID=sql.Literal(diskID),
            ramID=sql.Literal(ramID))

        rows_effected, _ = conn.execute(query)
        conn.commit()
    except DatabaseException.UNIQUE_VIOLATION as e:
        res = ReturnValue.ALREADY_EXISTS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        res = ReturnValue.NOT_EXISTS
    except Exception as e:
        print(e)
        res = ReturnValue.ERROR
    finally:
        conn.close()

    return res


# checked should be working
def removeRAMFromDisk(ramID: int, diskID: int) -> ReturnValue:
    conn = None
    res = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE FROM RamsOnDisks WHERE DiskID = {diskID} AND RamID = {ramID}").format(
            diskID=sql.Literal(diskID),
            ramID=sql.Literal(ramID))

        rows_effected, _ = conn.execute(query)
        conn.commit()

        if rows_effected == 0:
            res = ReturnValue.NOT_EXISTS
    except Exception as e:
        res = ReturnValue.ERROR
    finally:
        conn.close()

    return res


# checked should be working
def averageSizeQueriesOnDisk(diskID: int) -> float:
    conn = None
    res = 0
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "SELECT COALESCE(AVG(QuerySize), 0) FROM Queries WHERE QueryID IN (SELECT QueryID FROM QueriesOnDisks WHERE DiskID = {diskID})").format(
            diskID=sql.Literal(diskID))

        rows_effected, resultSet = conn.execute(query)
        conn.commit()
        res = resultSet[0]['coalesce']
    except Exception as e:
        res = -1
    finally:
        conn.close()

    return res


# checked should be working
def diskTotalRAM(diskID: int) -> int:
    conn = None
    result = 0
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "SELECT COALESCE(SUM(RamSize), 0) FROM RamsOnDisks WHERE DiskID = {diskID}").format(
            diskID=sql.Literal(diskID))
        # query = sql.SQL(
        #     "SELECT COALESCE(SUM(RamSize), 0) FROM Rams WHERE RamID IN (SELECT RamID FROM RamsOnDisks WHERE DiskID = {diskID})").format(
        #     diskID=sql.Literal(diskID))

        rows_effected, resultSet = conn.execute(query)
        conn.commit()
        result = resultSet[0]['coalesce']
    except Exception as e:
        print(e)
        result = -1
    finally:
        conn.close()

    return result


# checked should be working
def getCostForPurpose(purpose: str) -> int:
    conn = None
    result = 0
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "SELECT COALESCE(SUM(Cost), 0) FROM (SELECT * FROM Queries WHERE QueryPurpose={purpose}) AS derived INNER JOIN \
            QueriesOnDisks ON derived.QueryID = QueriesOnDisks.QueryID").format(
            purpose=sql.Literal(purpose))

        rows_effected, resultSet = conn.execute(query)
        conn.commit()
        result = resultSet[0]['coalesce']
    except Exception as e:
        result = -1
    finally:
        conn.close()

    return result


# checked should be working
def getQueriesCanBeAddedToDisk(diskID: int) -> List[int]:
    conn = None
    res = []
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "SELECT QueryID FROM QueriesCanBeAddedOnDisks WHERE DiskID = {diskID} ORDER BY QueryID DESC LIMIT 5").format(
            diskID=sql.Literal(diskID))

        rows_effected, resultSet = conn.execute(query)
        conn.commit()
        res = [i[0] for i in resultSet.rows]
    except Exception as e:
        print(e)

    finally:
        conn.close()
    return res


# checked should be working
def getQueriesCanBeAddedToDiskAndRAM(diskID: int) -> List[int]:
    conn = None
    res = []
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "SELECT QueryID FROM QueriesCanBeAddedOnDisks WHERE DiskId = {diskID} \
            AND (QuerySize <= (SELECT COALESCE(SUM(RamSize), 0) FROM RamsOnDisks WHERE DiskID = {diskID})) ORDER BY QueryID ASC LIMIT 5").format(
            diskID=sql.Literal(diskID))

        rows_effected, resultSet = conn.execute(query)
        conn.commit()
        res = [i[0] for i in resultSet.rows]
    except Exception as e:
        print(e)
        res = []

    finally:
        conn.close()
    return res


# checked should be working
def isCompanyExclusive(diskID: int) -> bool:
    conn = None
    result = False
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "SELECT DISTINCT * FROM ((SELECT ramcompany FROM Rams WHERE\
             (RamID IN (SELECT RamID FROM RamsOnDisks WHERE DiskID = {diskID}))) as rc FULL OUTER JOIN (SELECT DiskCompany FROM Disks WHERE DiskID = {diskID}) as dc ON rc.ramcompany = dc.diskcompany) as a").format(
            diskID=sql.Literal(diskID))
        rows_effected, resultSet = conn.execute(query)
        conn.commit()
        if rows_effected == 1:
            result = True
    except Exception as e:
        result = False
    finally:
        conn.close()
    return result


def getConflictingDisks() -> List[int]:
    conn = None
    result = []
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "SELECT DISTINCT DiskID FROM QueriesOnDisks WHERE QueryID IN (\
             SELECT QueryID FROM QueriesOnDisks GROUP BY QueryID HAVING COUNT(QueryID) > 1 ) ORDER BY DiskID ASC")
        rows_effected, resultSet = conn.execute(query)
        conn.commit()
        result = [i[0] for i in resultSet.rows]
    except Exception as e:
        print(e)
        result = []
    finally:
        conn.close()
    return result


def mostAvailableDisks() -> List[int]:
    conn = None
    result = []
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "SELECT DiskID From Disks Order By () DESC, DiskSpeed DESC, DiskID ASC LIMIT 5")
        rows_effected, resultSet = conn.execute(query)
        conn.commit()
        result = [i[0] for i in resultSet.rows]
    except Exception as e:
        print(e)
        result = []
    finally:
        conn.close()
    return result


def getCloseQueries(queryID: int) -> List[int]:
    conn = None
    result = []
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("").format(
            queryID=sql.Literal(queryID))
        query = sql.SQL("SELECT QueryID FROM\
                        (SELECT * FROM \
                        (SELECT * FROM QueriesOnDisks WHERE \
                         QueryID != {queryID} AND DiskID IN (SELECT DiskID FROM QueriesOnDisks WHERE QueryID = {queryID})) AS foo\
                         FULL JOIN Queries USING(QueryID)) AS FullJoin\
                         WHERE QueryID != {queryID}\
                        GROUP BY QueryID HAVING COALESCE(COUNT(DiskID),0) >= (SELECT (COUNT(*)+1)/2 FROM QueriesOnDisks WHERE QueryID = {queryID})\
                        ORDER BY QueryID ASC LIMIT 10").format(
            queryID=sql.Literal(queryID))
        rows_effected, resultSet = conn.execute(query)
        conn.commit()
        result = [i[0] for i in resultSet.rows]
    except Exception as e:
        result = []
    finally:
        conn.close()
    return result


if __name__ == '__main__':
    dropTables()
    createTables()

    disk1 = Disk(1, "dell", 6, 5, 10)
    disk2 = Disk(2, 'hp', 5, 10, 10)
    disk3 = Disk(3, 'apple', 4, 15, 10)
    disk4 = Disk(4, 'apple', 3, 30, 10)
    disk5 = Disk(5, 'apple', 4, 30, 10)
    disk6 = Disk(6, 'apple', 4, 30, 10)

    query1 = Query(1, 'hey', 2)
    query2 = Query(2, 'bye', 10)
    query3 = Query(3, 'day', 15)
    query4 = Query(4, 'a', 20)
    query5 = Query(5, 'b', 25)
    query6 = Query(6, 'd', 30)

    ram1 = RAM(1, "dell", 2)
    ram2 = RAM(2, "lenovo", 1)
    ram3 = RAM(3, "msi", 10)

    print("adding disks 1,2,3 and queries 1,2,3 to DB should be OK")
    print(addDiskAndQuery(disk1, query1))
    print(addDiskAndQuery(disk2, query2))
    print(addDiskAndQuery(disk3, query3))
    print(addDiskAndQuery(disk4, query4))
    print(addDiskAndQuery(disk5, query5))
    print(addDiskAndQuery(disk6, query6))

    print("adding rams 1,2,3 to DB should be OK")
    print(addRAM(ram1))
    print(addRAM(ram2))
    print(addRAM(ram3))

    print(getDiskProfile(1))
    print()
    print(addQueryToDisk(query1,1))
    print()
    print(getDiskProfile(1))
    print()
    print(deleteQuery(query1))
    print()
    print(getDiskProfile(1))
    print(deleteQuery(query2))

    dropTables()

