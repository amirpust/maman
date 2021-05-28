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
                             (QueryID INTEGER REFERENCES Queries,\
                              DiskID INTEGER REFERENCES Disks\
                              ON DELETE CASCADE,\
                              Cost INTEGER, \
                              PRIMARY KEY(QueryID, DiskID),\
                              CHECK(Cost >= 0));\
                         CREATE TABLE RamsOnDisks\
                             (RamID INTEGER REFERENCES Rams,\
                              DiskID INTEGER REFERENCES Disks\
                              ON DELETE CASCADE,\
                              RamSize INTEGER,\
                              PRIMARY KEY(RamID, DiskID),\
                              CHECK(RamSize >= 0));\
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
                      COMMIT;")
        conn.commit()
    except Exception as e:
        print(e)
    finally:
        # will happen any way after code try termination or exception handling
        conn.close()


def addQuery(queryToInsert: Query) -> ReturnValue:
    conn = None
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
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.NOT_NULL_VIOLATION as e:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        return ReturnValue.BAD_PARAMS
    except Exception as e:
        return ReturnValue.ERROR
    finally:
        conn.close()

    return ReturnValue.OK


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
            result = (
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
    try:
        conn = Connector.DBConnector()
        conn.execute(
            sql.SQL("DELETE FROM Queries WHERE QueryID = {QueryID}").format(QueryID=sql.Literal(query.getQueryID())))
        conn.commit()
    except Exception as e:
        print(e)
        return ReturnValue.ERROR
    finally:
        conn.close()

    return ReturnValue.OK


def addDisk(disk: Disk) -> ReturnValue:
    conn = None
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
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.NOT_NULL_VIOLATION as e:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        return ReturnValue.BAD_PARAMS
    except Exception as e:
        return ReturnValue.ERROR
    finally:
        conn.close()

    return ReturnValue.OK


def getDiskProfile(diskID: int) -> Disk:
    conn = None
    result = Disk.badDisk()
    try:
        conn = Connector.DBConnector()
        rows_effected, returnedResultSet = conn.execute(
            sql.SQL("SELECT * FROM Disks WHERE DiskID={diskID}").format(
                diskID=sql.Literal(diskID)))
        assert rows_effected <= 1  # at most 1 query is returned
        if rows_effected == 1:
            resultItem = returnedResultSet.__getitem__(0)
            result = (
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
    if diskID is None:
        return ReturnValue.OK  # doesn't exist
    conn = None
    try:
        conn = Connector.DBConnector()
        rows_effected, _ = conn.execute(sql.SQL("DELETE FROM Disks WHERE DiskID = {DiskID}").format(DiskID=sql.Literal(diskID)))
        conn.commit()
        if rows_effected == 0:
            return ReturnValue.NOT_EXISTS
    except Exception as e:
        print(e)
        return ReturnValue.ERROR
    finally:
        conn.close()

    return ReturnValue.OK


def addRAM(ramToInsert: RAM) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "INSERT INTO Rams(RamID, RamSize, RamCompany) VALUES({ramID}, {ramSize}, {ramCompany})").format(
            ramID=sql.Literal(ramToInsert.getRamID()), ramSize=sql.Literal(ramToInsert.getSize()),
            ramCompany=sql.Literal(ramToInsert.getCompany()))
        rows_effected, _ = conn.execute(query)
        conn.commit()
    except DatabaseException.UNIQUE_VIOLATION as e:
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.NOT_NULL_VIOLATION as e:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        return ReturnValue.BAD_PARAMS
    except Exception as e:
        return ReturnValue.ERROR
    finally:
        conn.close()

    return ReturnValue.OK


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
            result = (
                resultItem[returnedResultSet.cols_header[0]],
                resultItem[returnedResultSet.cols_header[1]],
                resultItem[returnedResultSet.cols_header[2]])
    except Exception as e:
        print(e)
    finally:
        conn.close()

    return result


def deleteRAM(ramID: int) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        rows_effected, _ = conn.execute(
            sql.SQL("DELETE FROM Rams WHERE RamID = {RamID}").format(RamID=sql.Literal(ramID)))
        conn.commit()
        if rows_effected == 0:
            return ReturnValue.NOT_EXISTS
    except Exception as e:
        print(e)
        return ReturnValue.ERROR
    finally:
        conn.close()

    return ReturnValue.OK


def addDiskAndQuery(disk: Disk, queryToInsert: Query) -> ReturnValue:
    conn = None
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
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.NOT_NULL_VIOLATION as e:
        conn.rollback()
        return ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        conn.rollback()
        return ReturnValue.BAD_PARAMS
    except Exception as e:
        print(e)
        conn.rollback()
        return ReturnValue.ERROR
    finally:
        conn.close()

    return ReturnValue.OK


def addQueryToDisk(query: Query, diskID: int) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "BEGIN;\
                INSERT INTO QueriesOnDisks(QueryID, DiskID, Cost) VALUES ({queryID}, {diskID}, {querySize} * (SELECT DiskCostPerByte FROM Disks WHERE DiskID = {diskID}));\
                UPDATE Disks SET DiskFreeSpace = DiskFreeSpace - {querySize};\
            COMMIT;").format(
            diskID=sql.Literal(diskID),
            queryID=sql.Literal(query.getQueryID()),
            querySize=sql.Literal(query.getSize()))

        rows_effected, _ = conn.execute(query)
        conn.commit()
        if rows_effected == 0:#TODO: NOT COPY
            return ReturnValue.NOT_EXISTS
    except DatabaseException.UNIQUE_VIOLATION as e:
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.NOT_NULL_VIOLATION as e:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        return ReturnValue.BAD_PARAMS
    except Exception as e:
        print(e)
        return ReturnValue.ERROR
    finally:
        conn.close()

    return ReturnValue.OK


def removeQueryFromDisk(query: Query, diskID: int) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE FROM QueriesOnDisks WHERE DiskID = {diskID} AND QueryID = {queryID}").format(
            diskID=sql.Literal(diskID),
            queryID=sql.Literal(query.getQueryID()))

        rows_effected, _ = conn.execute(query)
        conn.commit()
    except Exception as e:
        return ReturnValue.ERROR
    finally:
        conn.close()

    return ReturnValue.OK

def addRAMToDisk(ramID: int, diskID: int) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("INSERT INTO RamsOnDisks(RamID, DiskID,RamSize) VALUES ({ramID}, {diskID}, (SELECT RamSize FROM Rams WHERE ramID))").format(
            diskID=sql.Literal(diskID),
            ramID=sql.Literal(ramID))

        rows_effected, _ = conn.execute(query)
        conn.commit()
    except DatabaseException.UNIQUE_VIOLATION as e:
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.NOT_NULL_VIOLATION as e:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.FORGIEN_KEY_VIOLATION as e:
        return ReturnValue.NOT_EXISTS
    except Exception as e:
        print(e)
        return ReturnValue.ERROR
    finally:
        conn.close()

    return ReturnValue.OK


def removeRAMFromDisk(ramID: int, diskID: int) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE FROM RamsOnDisks WHERE DiskID = {diskID} AND RamID = {ramID}").format(
            diskID=sql.Literal(diskID),
            ramID=sql.Literal(ramID))

        rows_effected, _ = conn.execute(query)
        conn.commit()

        if rows_effected == 0:
            return ReturnValue.NOT_EXISTS
    except Exception as e:
        return ReturnValue.ERROR
    finally:
        conn.close()

    return ReturnValue.OK


def averageSizeQueriesOnDisk(diskID: int) -> float:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT AVG (QuerySize) FROM Queries WHERE QueryID IN (SELECT QueryID FROM QueriesOnDisk WHERE DiskID = {diskID})").format(
            diskID=sql.Literal(diskID))

        rows_effected, resultSet = conn.execute(query)
        conn.commit()
    except Exception as e:
        return 0
    finally:
        conn.close()

    return resultSet[0] #TODO: check how to return the avg

def diskTotalRAM(diskID: int) -> int:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "SELECT SUM (RamSize) FROM Rams WHERE RamID IN (SELECT RamID FROM RamsOnDisks WHERE DiskID = {diskID})").format(
            diskID=sql.Literal(diskID))

        rows_effected, resultSet = conn.execute(query)
        conn.commit()
    except Exception as e:
        return -1
    finally:
        conn.close()

    return resultSet[0]  # TODO: check how to return the sum


def getCostForPurpose(purpose: str) -> int:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "SELECT SUM(Cost) FROM (SELECT QueryID FROM Queries WHERE QueryPurpose = {purpose})INNER JOIN QueriesOnDisk").format(
            purpose=sql.Literal(purpose))

        rows_effected, resultSet = conn.execute(query)
        conn.commit()
    except Exception as e:
        return -1
    finally:
        conn.close()

    return resultSet[0]  # TODO: check how to return the sum


def getQueriesCanBeAddedToDisk(diskID: int) -> List[int]:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "SELECT QueryID FROM Queries WHERE QuerySize <= (SELECT DiskSize FROM Disks WHERE DiskId = {diskID}) ORDER BY QueryID DESC LIMIT 5").format(
            diskID=sql.Literal(diskID))
#TODO: CHECK HOW TO RETURN THE LIST CORRECTLY
        rows_effected, resultSet = conn.execute(query)
        conn.commit()
    except Exception as e:
        return -1
    finally:
        conn.close()

    return []


def getQueriesCanBeAddedToDiskAndRAM(diskID: int) -> List[int]:
    query = sql.SQL(
        "SELECT QueryID FROM Queries WHERE (QuerySize <= (SELECT DiskSize FROM Disks WHERE DiskId = {diskID})) \
        AND (QuerySize <= (SELECT SUM(RamSize) FROM RamsOnDisks WHERE DiskID = {diskID})) ORDER BY QueryID DESC LIMIT 5").format(
        diskID=sql.Literal(diskID))
    # TODO: CHECK HOW TO RETURN THE LIST CORRECTLY
    rows_effected, resultSet = conn.execute(query)
    conn.commit()

    return []


def isCompanyExclusive(diskID: int) -> bool:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "SELECT * FROM Rams WHERE RamID IN (SELECT RamID FORM RamsOnDisk WHERE DiskID = {diskID}) AND\
             RamCompany != (SELECT DiskCompany FROM Disks WHERE DiskID = {diskID})").format(
            diskID=sql.Literal(diskID))
        # TODO: CHECK HOW TO RETURN THE LIST CORRECTLY
        rows_effected, resultSet = conn.execute(query)
        conn.commit()
        if rows_effected == 0:
            return True
    except Exception as e:
        return -1
    finally:
        conn.close()
    return False


def getConflictingDisks() -> List[int]:
    return []


def mostAvailableDisks() -> List[int]:
    return []


def getCloseQueries(queryID: int) -> List[int]:
    return []


if __name__ == '__main__':
    # print("0. Creating all tables")
    dropTables()
    createTables()

    disk1 = Disk(1, "dell", 1, 10, 5)
    disk2 = Disk(2, 'hp', 2, 3, 4)
    disk3 = Disk(3, 'apple', 3, 16, 3)

    query1 = Query(1, 'hey', 5)
    query2 = Query(2, 'bye', 5)
    query3 = Query(3, 'day', 1)

    ram1 = RAM(1, "dell", 5)
    ram2 = RAM(2, "lenovo", 2)
    ram3 = RAM(3, "msi", 10)

    print("adding disks 1,2,3 and queries 1,2,3 to DB should be OK")
    print(addDiskAndQuery(disk1, query1))
    print(addDiskAndQuery(disk2, query2))
    print(addDiskAndQuery(disk3, query3))

    print("adding rams 1,2,3 to DB should be OK")
    print(addRAM(ram1))
    print(addRAM(ram2))
    print(addRAM(ram3))

    print("adding ")

    # print(getQueryProfile(1))
    # print(getDiskProfile(1))
    # print(addQueryToDisk(query1, 1))
    # print(addQueryToDisk(query2, 1))
    # print(deleteDisk(1))
