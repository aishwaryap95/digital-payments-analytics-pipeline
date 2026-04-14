from src.utils.logging_config import logger
from resources.dev import config

DB_NAME = config.DB_NAME
STAGING_TABLE = config.STAGING_TABLE

# ---------------------------------------------------
# 1. Mark Old Active Files as Failed
# ---------------------------------------------------

def mark_old_active_as_failed(cursor, connection):
    logger.info("Checking for previous active files...")

    update_statement = f"""
    UPDATE {DB_NAME}.{STAGING_TABLE}
    SET status = 'F',
    error_type = 'SYSTEM_FAILURE',
    error_message = 'Job terminated unexpectedly',
    updated_date = NOW()
    WHERE status = 'A'
    """

    cursor.execute(update_statement)
    rows_updated = cursor.rowcount
    connection.commit()

    if rows_updated > 0:
        logger.info("Previous active files marked as Failed. Count: %s", rows_updated)
    else:
        logger.info("No previous active files found. Safe to proceed.")


# ---------------------------------------------------
# 2. Mark File Active
# ---------------------------------------------------

def mark_file_active(cursor, connection, file_name):
    logger.info(f"Marking file {file_name} as Active")

    statement = f"""
    UPDATE {DB_NAME}.{STAGING_TABLE}
    SET status = 'A',
    updated_date = NOW()
    WHERE file_name = %s
    """

    cursor.execute(statement, (file_name,))
    connection.commit()


# ---------------------------------------------------
# 3. Mark File Completed
# ---------------------------------------------------

def mark_file_completed(cursor, connection, file_name):
    logger.info(f"Marking file {file_name} as Completed")

    statement = f"""
    UPDATE {DB_NAME}.{STAGING_TABLE}
    SET status = 'C',
    updated_date = NOW()
    WHERE file_name = %s
    """

    cursor.execute(statement, (file_name,))
    connection.commit()


# ---------------------------------------------------
# 4. Mark File Failed
# ---------------------------------------------------

def mark_file_failed(cursor, connection, file_name, error_message, error_type):

    logger.error(f"Marking file {file_name} as Failed")

    statement = f"""
    UPDATE {DB_NAME}.{STAGING_TABLE}
    SET status = 'F',
    error_type = %s,
    error_message = %s,
    updated_date = NOW()
    WHERE file_name = %s
    """

    cursor.execute(statement, (error_type, error_message, file_name))
    connection.commit()


# ---------------------------------------------------
# 5. Reprocess Failed Files
# ---------------------------------------------------

def reprocess_failed_files(cursor):
    logger.info("Checking failed files for reprocessing")

    statement = f"""
    SELECT file_name 
    FROM {DB_NAME}.{STAGING_TABLE}
    WHERE status = 'F'
    AND error_type IN ('SYSTEM_FAILURE','NETWORK_FAILURE')
    """

    cursor.execute(statement)
    return cursor.fetchall()