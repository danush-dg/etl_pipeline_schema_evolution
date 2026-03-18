# import mysql.connector
# from mysql.connector import Error
# from datetime import datetime
# import logging

# # Configure Python Logging
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
# logger = logging.getLogger(__name__)

# class MySQLETLPipeline:
#     def __init__(self, host, user, password, database):
#         self.config = {
#             'host': host,
#             'user': user,
#             'password': password,
#             'database': database
#         }
#         self.conn = None
#         self.cursor = None

#     def connect(self):
#         try:
#             self.conn = mysql.connector.connect(**self.config)
#             self.cursor = self.conn.cursor(dictionary=True)
#             logger.info("Database connection established.")
#         except Error as e:
#             logger.error(f"Error connecting to MySQL: {e}")
#             raise

#     def disconnect(self):
#         if self.conn and self.conn.is_connected():
#             self.cursor.close()
#             self.conn.close()
#             logger.info("Database connection closed.")

#     def execute_query(self, query, params=None, fetch=False):
#         try:
#             self.cursor.execute(query, params)
#             if fetch:
#                 return self.cursor.fetchall()
#             self.conn.commit()
#             return self.cursor.rowcount
#         except Error as e:
#             self.conn.rollback()
#             logger.error(f"Query execution failed: {e}")
#             raise

#     # 1. Get pipeline configuration
#     def get_control_data(self):
#         return self.execute_query("SELECT * FROM control_table", fetch=True)

#     # 2. Schema check + validation + evolution
#     def check_and_update_schema(self, source_table, staging_table, operational_table):
#         schema_changed = False

#         # Source columns
#         self.cursor.execute(f"SHOW COLUMNS FROM {source_table}")
#         source_cols = {col['Field']: col for col in self.cursor.fetchall()}

#         # Staging columns
#         self.cursor.execute(f"SHOW COLUMNS FROM {staging_table}")
#         staging_cols = {col['Field'] for col in self.cursor.fetchall()}

#         # Operational columns
#         self.cursor.execute(f"SHOW COLUMNS FROM {operational_table}")
#         op_cols = {col['Field'] for col in self.cursor.fetchall()}

#         # VALIDATION (FAIL FAST)
#         # -------------------------
#         missing_in_source = staging_cols - set(source_cols.keys())

#         if missing_in_source:
#             error_msg = f"ERROR: Columns missing in source: {missing_in_source}"
#             logger.error(error_msg)
#             raise Exception(error_msg)

#         # add new columns
#         # -------------------------
#         new_columns = set(source_cols.keys()) - staging_cols

#         if new_columns:
#             logger.info(f"New columns detected: {new_columns}")
#             schema_changed = True

#             for col in new_columns:
#                 col_def = source_cols[col]

#                 # add to staging
#                 self.execute_query(f"""
#                     ALTER TABLE {staging_table} 
#                     ADD COLUMN {col_def['Field']} {col_def['Type']}
#                 """)

#                 # add to operational
#                 self.execute_query(f"""
#                     ALTER TABLE {operational_table} 
#                     ADD COLUMN {col_def['Field']} {col_def['Type']}
#                 """)

#         return 'Yes' if schema_changed else 'No'

#     # 3. Extract incremental data
#     def extract_data(self, source_table, last_loaded):
#         query = f"""
#             SELECT * FROM {source_table}
#             WHERE updated_at >= %s
#             ORDER BY id
#         """
#         return self.execute_query(query, (last_loaded,), fetch=True)

#     # 4. Load to staging
#     def load_staging(self, staging_table, data):
#         if not data:
#             return 0

#         # Clear staging
#         self.execute_query(f"TRUNCATE TABLE {staging_table}")

#         columns = list(data[0].keys())
#         col_names = ', '.join(columns)
#         placeholders = ', '.join(['%s'] * len(columns))

#         insert_query = f"""
#             INSERT INTO {staging_table} ({col_names})
#             VALUES ({placeholders})
#         """

#         values = [list(row.values()) for row in data]
#         self.cursor.executemany(insert_query, values)
#         self.conn.commit()

#         return len(values)

#     # 5. Dynamic UPSERT
#     def upsert_operational(self, op_table, staging_table):
#         self.cursor.execute(f"SHOW COLUMNS FROM {staging_table}")
#         columns = [col['Field'] for col in self.cursor.fetchall()]

#         col_list = ', '.join(columns)

#         # Assuming primary key = id
#         update_clause = ', '.join([
#             f"{col}=VALUES({col})" for col in columns if col != 'id'
#         ])

#         query = f"""
#             INSERT INTO {op_table} ({col_list})
#             SELECT {col_list} FROM {staging_table}
#             ON DUPLICATE KEY UPDATE {update_clause}
#         """

#         self.execute_query(query)

#     # 6. Update control table
#     def update_control_table(self, source_table):
#         query = """
#             UPDATE control_table 
#             SET last_loaded = %s
#             WHERE source_table = %s
#         """
#         self.execute_query(query, (datetime.now(), source_table))

#     # 7. Log execution
#     def log_execution(self, stg_table, row_count, status, schema_change):
#         avg_query = """
#             SELECT AVG(row_loaded) as avg_rows 
#             FROM logging_table 
#             WHERE stg_tablename = %s 
#             AND last_loaded > DATE_SUB(%s, INTERVAL 5 DAY)
#             AND status = 'Success'
#         """
#         result = self.execute_query(avg_query, (stg_table, datetime.now()), fetch=True)

#         avg_rows = result[0]['avg_rows'] if result and result[0]['avg_rows'] else 0
#         alert = 'Yes' if avg_rows > 0 and row_count > (1.1 * float(avg_rows)) else 'No'

#         if alert == 'Yes':
#             logger.warning(f"ALERT: Loaded {row_count} rows, Avg was {avg_rows:.2f}")

#         log_query = """
#             INSERT INTO logging_table 
#             (alert, last_loaded, stg_tablename, row_loaded, status, schema_change)
#             VALUES (%s, %s, %s, %s, %s, %s)
#         """
#         self.execute_query(
#             log_query,
#             (alert, datetime.now(), stg_table, row_count, status, schema_change)
#         )

#     # Main pipeline execution
#     def run_pipeline(self):
#         self.connect()
#         try:
#             for config in self.get_control_data():
#                 src = config['source_table']
#                 stg = config['staging_table']
#                 op = config['operational_table']
#                 last_loaded = config['last_loaded']

#                 logger.info(f"Processing: {src} -> {stg}")

#                 try:
#                     # Schema check + validation
#                     schema_change = self.check_and_update_schema(src, stg, op)

#                     # Extract
#                     data = self.extract_data(src, last_loaded)

#                     if data:
#                         row_count = self.load_staging(stg, data)
#                         self.upsert_operational(op, stg)
#                         self.update_control_table(src)

#                         logger.info(f"Loaded {row_count} rows")
#                         self.log_execution(stg, row_count, 'Success', schema_change)
#                     else:
#                         logger.info("No new data")
#                         self.log_execution(stg, 0, 'Success', schema_change)

#                 except Exception as e:
#                     logger.error(f"Pipeline failed: {e}")
#                     self.log_execution(stg, 0, 'Failed', 'No')

#         finally:
#             self.disconnect()


# if __name__ == "__main__":
#     etl = MySQLETLPipeline('localhost', 'root', '1234', 'db')
#     etl.run_pipeline()

import mysql.connector
from mysql.connector import Error
from datetime import datetime
import logging

# Configure Python Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class MySQLETLPipeline:
    def __init__(self, host, user, password, database):
        self.config = {
            'host': host,
            'user': user,
            'password': password,
            'database': database
        }
        self.conn = None
        self.cursor = None

    def connect(self):
        try:
            self.conn = mysql.connector.connect(**self.config)
            self.cursor = self.conn.cursor(dictionary=True)
            logger.info("Database connection established.")
        except Error as e:
            logger.error(f"Error connecting to MySQL: {e}")
            raise

    def disconnect(self):
        if self.conn and self.conn.is_connected():
            self.cursor.close()
            self.conn.close()
            logger.info("Database connection closed.")

    def execute_query(self, query, params=None, fetch=False):
        try:
            self.cursor.execute(query, params)
            if fetch:
                return self.cursor.fetchall()
            self.conn.commit()
            return self.cursor.rowcount
        except Error as e:
            self.conn.rollback()
            logger.error(f"Query execution failed: {e}")
            raise

    # 1. Get pipeline configuration
    def get_control_data(self):
        return self.execute_query("SELECT * FROM control_table", fetch=True)

    # 2. Schema check + validation + evolution
    def check_and_update_schema(self, source_table, staging_table, operational_table):
        schema_changed = False

        # ---- 1. Get Source Columns ----
        self.cursor.execute(f"SHOW COLUMNS FROM {source_table}")
        source_cols = {col['Field']: col for col in self.cursor.fetchall()}

        # ---- 2. Get Staging Columns ----
        self.cursor.execute(f"SHOW COLUMNS FROM {staging_table}")
        staging_cols = {col['Field']: col for col in self.cursor.fetchall()}

        # ---- 3. Get Operational Columns ----
        self.cursor.execute(f"SHOW COLUMNS FROM {operational_table}")
        op_cols = {col['Field']: col for col in self.cursor.fetchall()}

        # ---- 4. VALIDATION: Missing columns in source ----
        missing_in_source = set(staging_cols.keys()) - set(source_cols.keys())

        if missing_in_source:
            error_msg = f"ERROR: Columns missing in source: {missing_in_source}"
            logger.error(error_msg)
            raise Exception(error_msg)

        # ---- 5. ADD NEW COLUMNS ----
        new_columns = set(source_cols.keys()) - set(staging_cols.keys())

        if new_columns:
            logger.info(f"New columns detected: {new_columns}")
            schema_changed = True

            for col in new_columns:
                col_def = source_cols[col]

                alter_query = f"""
                    ALTER TABLE {{table}}
                    ADD COLUMN {col_def['Field']} {col_def['Type']}
                """

                self.execute_query(alter_query.format(table=staging_table))
                self.execute_query(alter_query.format(table=operational_table))

        # ---- 6. DATATYPE VALIDATION + SAFE EVOLUTION ----
        for col in source_cols:
            if col in staging_cols:

                source_type = source_cols[col]['Type'].lower()
                staging_type = staging_cols[col]['Type'].lower()

                if source_type != staging_type:
                    logger.warning(
                        f"Datatype mismatch for '{col}': "
                        f"Source={source_type}, Staging={staging_type}"
                    )

                    # ---- SAFE CHECK (avoid dangerous conversions) ----
                    safe = False

                    # Allow safe widening conversions
                    if "varchar" in source_type and "varchar" in staging_type:
                        safe = True
                    elif "decimal" in source_type and "decimal" in staging_type:
                        safe = True
                    elif "int" in source_type and "int" in staging_type:
                        safe = True
                    elif "datetime" in source_type and "datetime" in staging_type:
                        safe = True

                    if safe:
                        logger.info(f"Auto-updating datatype for column '{col}'")

                        schema_changed = True

                        # Update staging
                        self.execute_query(f"""
                            ALTER TABLE {staging_table}
                            MODIFY COLUMN {col} {source_cols[col]['Type']}
                        """)

                        # Update operational
                        self.execute_query(f"""
                            ALTER TABLE {operational_table}
                            MODIFY COLUMN {col} {source_cols[col]['Type']}
                        """)
                    else:
                        # Unsafe → FAIL FAST
                        error_msg = (
                            f"Unsafe datatype change for column '{col}': "
                            f"{staging_type} -> {source_type}"
                        )
                        logger.error(error_msg)
                        raise Exception(error_msg)

        return 'Yes' if schema_changed else 'No'

    # 3. Extract incremental data
    def extract_data(self, source_table, last_loaded):
        query = f"""
            SELECT * FROM {source_table}
            WHERE updated_at >= %s
            ORDER BY id
        """
        return self.execute_query(query, (last_loaded,), fetch=True)

    # 4. Load to staging
    def load_staging(self, staging_table, data):
        if not data:
            return 0

        # Clear staging
        self.execute_query(f"TRUNCATE TABLE {staging_table}")

        columns = list(data[0].keys())
        col_names = ', '.join(columns)
        placeholders = ', '.join(['%s'] * len(columns))

        insert_query = f"""
            INSERT INTO {staging_table} ({col_names})
            VALUES ({placeholders})
        """

        values = [list(row.values()) for row in data]
        self.cursor.executemany(insert_query, values)
        self.conn.commit()

        return len(values)

    # 5. Dynamic UPSERT
    def upsert_operational(self, op_table, staging_table):
        self.cursor.execute(f"SHOW COLUMNS FROM {staging_table}")
        columns = [col['Field'] for col in self.cursor.fetchall()]

        col_list = ', '.join(columns)

        # Assuming primary key = id
        update_clause = ', '.join([
            f"{col}=VALUES({col})" for col in columns if col != 'id'
        ])

        query = f"""
            INSERT INTO {op_table} ({col_list})
            SELECT {col_list} FROM {staging_table}
            ON DUPLICATE KEY UPDATE {update_clause}
        """

        self.execute_query(query)

    # 6. Update control table
    def update_control_table(self, source_table):
        query = """
            UPDATE control_table 
            SET last_loaded = %s
            WHERE source_table = %s
        """
        self.execute_query(query, (datetime.now(), source_table))

    # 7. Log execution
    def log_execution(self, stg_table, row_count, status, schema_change):
        avg_query = """
            SELECT AVG(row_loaded) as avg_rows 
            FROM logging_table 
            WHERE stg_tablename = %s 
            AND last_loaded > DATE_SUB(%s, INTERVAL 5 DAY)
            AND status = 'Success'
        """
        result = self.execute_query(avg_query, (stg_table, datetime.now()), fetch=True)

        avg_rows = result[0]['avg_rows'] if result and result[0]['avg_rows'] else 0
        alert = 'Yes' if avg_rows > 0 and row_count > (1.1 * float(avg_rows)) else 'No'

        if alert == 'Yes':
            logger.warning(f"ALERT: Loaded {row_count} rows, Avg was {avg_rows:.2f}")

        log_query = """
            INSERT INTO logging_table 
            (alert, last_loaded, stg_tablename, row_loaded, status, schema_change)
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        self.execute_query(
            log_query,
            (alert, datetime.now(), stg_table, row_count, status, schema_change)
        )

    # Main pipeline execution
    def run_pipeline(self):
        self.connect()
        try:
            for config in self.get_control_data():
                src = config['source_table']
                stg = config['staging_table']
                op = config['operational_table']
                last_loaded = config['last_loaded']

                logger.info(f"Processing: {src} -> {stg}")

                try:
                    # Schema check + validation
                    schema_change = self.check_and_update_schema(src, stg, op)

                    # Extract
                    data = self.extract_data(src, last_loaded)

                    if data:
                        row_count = self.load_staging(stg, data)
                        self.upsert_operational(op, stg)
                        self.update_control_table(src)

                        logger.info(f"Loaded {row_count} rows")
                        self.log_execution(stg, row_count, 'Success', schema_change)
                    else:
                        logger.info("No new data")
                        self.log_execution(stg, 0, 'Success', schema_change)

                except Exception as e:
                    logger.error(f"Pipeline failed: {e}")
                    self.log_execution(stg, 0, 'Failed', 'No')

        finally:
            self.disconnect()


if __name__ == "__main__":
    etl = MySQLETLPipeline('localhost', 'root', '1234', 'db')
    etl.run_pipeline()