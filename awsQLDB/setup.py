import os
os.environ["AWS_PROFILE"] = "Insight"
from create_ledger import *
from connect_to_ledger import *
from create_table import *
from create_index import * 
from insert_document import *


def index_setup(ledgerName, collectionIndex=None, trainIndex=None, predictIndex=None):
    logger.info('Creating indexes on all tables in a single transaction...')
    try:
        for cIndex in collectionIndex:
            with create_qldb_session(ledgerName) as session:
                session.execute_lambda(lambda x: create_index(x, "Collection",
                                                            cIndex),
                                    lambda retry_attempt: logger.info('Retrying due to OCC conflict...'))
                logger.info('Indexes created successfully.')
        for tIndex in trainIndex:
            with create_qldb_session(ledgerName) as session:
                session.execute_lambda(lambda x: create_index(x, "Training",
                                                            tIndex),
                                    lambda retry_attempt: logger.info('Retrying due to OCC conflict...'))
                logger.info('Indexes created successfully.')
        for pIndex in predictIndex:
            with create_qldb_session(ledgerName) as session:
                session.execute_lambda(lambda x: create_index(x, "Prediction",
                                                            pIndex),
                                    lambda retry_attempt: logger.info('Retrying due to OCC conflict...'))
                logger.info('Indexes created successfully.')
    except Exception:
        logger.exception('Unable to create indexes.')


def qldb_setup(ledgerName, collectionIndex=None, trainIndex=None, predictIndex=None):
    create_ledger(ledgerName)
    wait_for_active(ledgerName)
    try:
        with create_qldb_session(ledgerName) as session:
            session.execute_lambda(lambda x: create_table(x, "Collection") and
                                create_table(x, "Training") and
                                create_table(x, "Prediction"),
                                lambda retry_attempt: logger.info('Retrying due to OCC conflict...'))
            logger.info('Tables created successfully.')
    except Exception:
        logger.exception('Errors creating tables.')
    index_setup(ledgerName, collectionIndex, trainIndex, predictIndex)


cIndex = ['GitHash', 'ScrapeTime', 'DataPath', 'OutputHash']
tIndex = ['GitHash', 'InputHash', 'TrainTime', 'Model']
pIndex = ['GitHash', 'InputHash', 'Model', 'PredictionTime', 'OutputHash']
qldb_setup("Demo4Ledger", cIndex, tIndex, pIndex)