import pprint
import os

from create_ledger import *
from connect_to_ledger import *
from create_table import *
from create_index import * 
from insert_document import *
from setup import *
from schema import * 






class QLDB_Ledger:
    def __init__(self, ln):
        self.ledgerName = ln
        self.tables = {}

    def setTables(self, tables):
        self.tables = tables
    
    def addTable(self, tableName, indexList):
        self.tables[tableName] = indexList
        
    def printLedger(self):
        print("Ledger Name: %s" % self.ledgerName)
        print("Tables: ")
        pprint.pprint(self.tables)


def create_schema(ledgerName, tableName, indexList):
    try:
        with create_qldb_session(ledgerName) as session:
            session.execute_lambda(lambda x: create_table(x, tableName), 
                                lambda retry_attempt: logger.info('Retrying due to OCC conflict...'))
            logger.info('Tables created successfully.')
    except Exception:
        logger.exception('Errors creating tables.')
    for idx in indexList:
        try:
            with create_qldb_session(ledgerName) as session:
                session.execute_lambda(lambda x: create_index(x, tableName, idx), 
                                lambda retry_attempt: logger.info('Retrying due to OCC conflict...'))
                logger.info('Index created successfully.')
        except Exception:
            logger.exception('Errors creating tables.')


def setupLedger():
    ledger = QLDB_Ledger(LEDGER_NAME)
    ledger.setTables(ML_PIPELINE)

    ledger.printLedger()
    create_ledger(ledger.ledgerName)
    wait_for_active(ledger.ledgerName)
    for tbl, idxList in ledger.tables.items():
        create_schema(ledger.ledgerName, tbl, idxList)




if __name__ == "__main__":
    setupLedger()