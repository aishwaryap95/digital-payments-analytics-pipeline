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

def upsert_file_active(cursor, connection, file_name, file_location):
    logger.info("Marking file '%s' as Active", file_name)

    statement = f"""
    INSERT INTO {DB_NAME}.{STAGING_TABLE}
    (file_name, file_location, created_date, updated_date, status, error_type)
    VALUES (%s, %s, NOW(), NOW(), 'A', NULL)
    
    ON DUPLICATE KEY UPDATE
    status = 'A',
    updated_date = NOW(),
    file_location = VALUES(file_location),
    error_type = NULL
    """

    cursor.execute(statement, (file_name,file_location))
    rows_updated = cursor.rowcount
    connection.commit()

    if rows_updated > 0:
        logger.info("File '%s' successfully marked as Active", file_name)
    else:
        logger.warning("File '%s' not found in staging table. No rows updated.", file_name)


# ---------------------------------------------------
# 3. Mark File Completed
# ---------------------------------------------------

def mark_file_completed(cursor, connection, file_name):
    logger.info(f"Marking file {file_name} as Completed")

    statement = f"""
    UPDATE {DB_NAME}.{STAGING_TABLE}
    SET status = 'C',
    error_type = NULL,
    error_message = NULL,
    updated_date = NOW()
    WHERE file_name = %s
    """

    cursor.execute(statement, (file_name,))
    connection.commit()


# ---------------------------------------------------
# 4. Upsert File Failed
# ---------------------------------------------------

def upsert_file_failed(
        cursor,
        connection,
        file_name,
        file_location,
        error_message,
        error_type):

    logger.error("Upserting file '%s' as Failed | Error Type: %s | Error Message: %s", file_name, error_type, error_message )

    statement = f"""
    INSERT INTO {DB_NAME}.{STAGING_TABLE}
    (
        file_name,
        file_location,
        created_date,
        updated_date,
        status,
        error_type,
        error_message
    )
    VALUES ( %s, %s, NOW(), NOW(), 'F', %s, %s )

    ON DUPLICATE KEY UPDATE
        status = 'F',
        file_location = VALUES(file_location),
        error_type = VALUES(error_type),
        error_message = VALUES(error_message),
        updated_date = NOW()
    """

    cursor.execute(
        statement,
        ( file_name, file_location, error_type, error_message)
    )
    connection.commit()

    logger.error(
        "File '%s' successfully upserted as Failed",
        file_name
    )

# ---------------------------------------------------
# 5. Reprocess Failed Files
# ---------------------------------------------------

def get_retryable_failed_files(cursor):
    query = f"""
        SELECT file_name, file_location
        FROM {DB_NAME}.{STAGING_TABLE}
        WHERE status = 'F'
        AND error_type IN ('SYSTEM_FAILURE', 'NETWORK_FAILURE')
    """
    cursor.execute(query)
    return cursor.fetchall()