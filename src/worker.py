from pathlib import Path
import sqlite3
import json
import sys
from typing import Dict, Any
from query_engine_client import QueryEngineClient
from container_params import ContainerParams, ContainerParamError

def print_table_info(db_path: Path, table_name: str) -> None:
    """
    Connects to an SQLite database and prints the table information
    (columns) for a given table.

    Args:
        db_path: Path to the SQLite database file.
        table_name: The name of the table to inspect.
    """
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()

            # Execute the PRAGMA command
            # Note: PRAGMA statements are not standard SQL, but specific to SQLite
            cursor.execute(f"PRAGMA table_info('{table_name}');")

            # Get column names from the cursor description
            column_names = [description[0] for description in cursor.description]
            print(f"\n--- Columns for table: {table_name} ---")
            print(" | ".join(column_names))
            print("-" * (len(" | ".join(column_names)) + 5)) # Simple separator

            # Fetch all rows and print them
            for row in cursor.fetchall():
                # Convert each item in the row to string for consistent printing
                print(" | ".join(map(str, row)))

    except sqlite3.Error as e:
        print(f"SQLite error when getting table info for '{table_name}': {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

def get_user_data(db_path: Path) -> Dict[str, Dict[str, Any]]:
    """Query the SQLite database and extract user data.

    Args:
        db_path: Path to the SQLite database file

    Returns:
        Dictionary with UserID as keys and user data as values

    Raises:
        Exception: If there's an error connecting to or querying the database
    """
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Query UserID, Source, and Status from the results table
        cursor.execute('SELECT "UserID", "Source", "Status" FROM results')

        # Create a dictionary with UserID as keys and user data as values
        user_data = {}
        for row in cursor.fetchall():
            user_id, source, status = row
            user_data[str(user_id)] = {
                "source": source,
                "status": status
            }

        conn.close()
        return user_data
    except sqlite3.Error as e:
        print(f"SQLite error: {e}")
        raise
    except Exception as e:
        print(f"Error querying database: {e}")
        raise


def get_chat_messages(db_path: Path) -> Dict[str, Dict[str, Any]]:
    """Query the SQLite database and extract all chat messages with proper MessageID keys.

    Args:
        db_path: Path to the SQLite database file

    Returns:
        Dictionary with MessageID as keys and message data as values

    Raises:
        sqlite3.Error: If there's a database error
        Exception: For other unexpected errors
    """
    try:
        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row  # Allows fetching rows as dict-like
            cursor = conn.cursor()

            # Query all messages with their related chat and submission info
            cursor.execute('''
                SELECT SenderID FROM results LIMIT 10
            ''')

            messages = {}
            index_counter = 0
            for row in cursor.fetchall():
                row_dict = dict(row)
                # Use the actual MessageID from schema as the key
                # message_id = row_dict['MessageID']
                # messages[message_id] = row_dict
                messages[str(index_counter)] = row_dict
                index_counter += 1

            return messages

    except sqlite3.Error as e:
        print(f"SQLite error: {e}")
        raise
    except Exception as e:
        print(f"Error querying database: {e}")
        raise

def get_submission_chats(db_path: Path) -> Dict[str, Dict[str, Any]]:
    """Query the SQLite database and extract all submission chats.

    Args:
        db_path: Path to the SQLite database file

    Returns:
        Dictionary with SubmissionChatID as keys and chat data as values

    Raises:
        sqlite3.Error: If there's a database error
        Exception: For other unexpected errors
    """
    try:
        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute('SELECT FirstMessageDate, LastMessageDate, ParticipantCount FROM results')

            chats = {}
            for row in cursor.fetchall():
                row_dict = dict(row)
                chat_id = row_dict['SubmissionChatID']
                chats[chat_id] = row_dict

            return chats

    except sqlite3.Error as e:
        print(f"SQLite error: {e}")
        raise
    except Exception as e:
        print(f"Error querying database: {e}")
        raise

def get_submissions(db_path: Path) -> Dict[str, Dict[str, Any]]:
    """Query the SQLite database and extract all submissions.

    Args:
        db_path: Path to the SQLite database file

    Returns:
        Dictionary with SubmissionID as keys and submission data as values

    Raises:
        sqlite3.Error: If there's a database error
        Exception: For other unexpected errors
    """
    try:
        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            # cursor.execute('SELECT * FROM results')
            cursor.execute('SELECT SubmissionID, UserID, SubmissionDate, SubmissionReference FROM results')

            submissions = {}
            for row in cursor.fetchall():
                row_dict = dict(row)
                submission_id = row_dict['SubmissionID']
                submissions[submission_id] = row_dict

            return submissions

    except sqlite3.Error as e:
        print(f"SQLite error: {e}")
        raise
    except Exception as e:
        print(f"Error querying database: {e}")
        raise

def get_users(db_path: Path) -> Dict[str, Dict[str, Any]]:
    """Query the SQLite database and extract all users.

    Args:
        db_path: Path to the SQLite database file

    Returns:
        Dictionary with UserID as keys and user data as values

    Raises:
        sqlite3.Error: If there's a database error
        Exception: For other unexpected errors
    """
    try:
        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM results')

            users = {}
            for row in cursor.fetchall():
                row_dict = dict(row)
                user_id = row_dict['UserID']
                users[user_id] = row_dict

            return users

    except sqlite3.Error as e:
        print(f"SQLite error: {e}")
        raise
    except Exception as e:
        print(f"Error querying database: {e}")
        raise

def get_data(db_path: Path) -> Dict[str, Dict[str, Any]]:
    """Fetch all rows from the 'results' table in the SQLite database.

    Each row is returned as a dictionary with column names as keys.

    Args:
        db_path: Path to the SQLite database file.

    Returns:
        A list of dictionaries, each representing a row from the 'results' table.

    Raises:
        sqlite3.Error: If a database error occurs.
        Exception: For other unexpected errors.
    """
    try:
        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM results')
            return [dict(row) for row in cursor.fetchall()]

    except sqlite3.Error as e:
        print(f"SQLite error: {e}")
        raise
    except Exception as e:
        print(f"Error querying database: {e}")
        raise


def save_stats_to_json(data: Dict[str, Any], output_path: Path) -> None:
    """Save data to a JSON file.

    Args:
        data: Data to save (dictionary that can be JSON serialized)
        output_path: Path where the JSON file will be saved

    Raises:
        Exception: If there's an error creating the output directory or saving the file
    """
    try:
        # Ensure the output directory exists
        output_dir = output_path.parent
        output_dir.mkdir(parents=True, exist_ok=True)

        # Save to JSON
        with open(output_path, "w") as f:
            json.dump(data, f, indent=4)

        print(f"Stats saved to {output_path}")
    except Exception as e:
        print(f"Error saving JSON: {e}")
        raise

def execute_query(params: ContainerParams) -> bool:
    """Execute the query using the query engine client.

    Args:
        params: Container parameters with query details

    Returns:
        True if query execution was successful, False otherwise
    """
    if not params.validate_production_mode():
        return False

    # Initialize query engine client
    query_engine_client = QueryEngineClient(
        params.query,
        params.query_signature,
        str(params.db_path)
    )

    # Execute query
    print(f"Executing query: {params.query}")
    query_result = query_engine_client.execute_query(
        params.compute_job_id,
        params.data_refiner_id,
        params.query_params
    )

    if not query_result.success:
        error_msg = f"Error executing query: {query_result.error}"
        if query_result.status_code:
            error_msg += f" (Status code: {query_result.status_code})"
        if query_result.data:
            error_msg += f"\nResponse data: {json.dumps(query_result.data, indent=2)}"
        print(error_msg)
        return False

    print(f"Query executed successfully, processing results from {params.db_path}")
    return True

# def process_results(params: ContainerParams) -> None:
#     """Process query results and generate stats file.

#     Args:
#         params: Container parameters
#     """
#     user_data = get_user_data(params.db_path)

#     if user_data:
#         print(f"Found {len(user_data)} users in the database")
#         save_stats_to_json(user_data, params.stats_path)
#     else:
#         print("No user stats found in the database")
#         # Create an empty stats file to indicate processing completed
#         save_stats_to_json({}, params.stats_path)

# def process_results(params: ContainerParams) -> None:
#     """Process query results and generate stats file."""
#     messages = get_chat_messages(params.db_path)

#     if messages:
#         print(f"Found {len(messages)} chat messages in the database!")
#         save_stats_to_json(messages, params.stats_path)
#     else:
#         print("No chat messages found in the database!")
#         save_stats_to_json({}, params.stats_path)

def process_results(params: ContainerParams) -> None:
    """Process query results and generate stats file."""
    chats = get_submission_chats(params.db_path)

    if chats:
        print(f"Found {len(chats)} submission chats in the database!")
        save_stats_to_json(chats, params.stats_path)
    else:
        print("No submission chats found in the database!")
        save_stats_to_json({}, params.stats_path)

# def process_results(params: ContainerParams) -> None:
#     """Process query results and generate stats file."""
#     chats = get_submissions(params.db_path)

#     if chats:
#         print(f"Found {len(chats)} submissions in the database!")
#         save_stats_to_json(chats, params.stats_path)
#     else:
#         print("No submissions found in the database!")
#         save_stats_to_json({}, params.stats_path)

# def process_results(params: ContainerParams) -> None:
#     """Process query results and generate stats file."""
#     chats = get_users(params.db_path)

#     if chats:
#         print(f"Found {len(chats)} users in the database!")
#         save_stats_to_json(chats, params.stats_path)
#     else:
#         print("No users found in the database!")
#         save_stats_to_json({}, params.stats_path)

# def process_results(params: ContainerParams) -> None:
#     """Process query results and generate stats file."""
#     data = get_data(params.db_path)

#     if data:
#         print(f"Found {len(data)} records in the database!")
#         save_stats_to_json(data, params.stats_path)
#     else:
#         print("No records found in the database!")
#         save_stats_to_json({}, params.stats_path)

def main() -> None:
    """Main entry point for the worker. TEST"""
    try:
        # Load parameters from environment variables
        try:
            params = ContainerParams.from_env()
            print(f"params: ${params}")
        except ContainerParamError as e:
            print(f"Error in container parameters: {e}")
            sys.exit(1)

        # Handle development vs production mode
        if params.dev_mode:
            print("Running in DEVELOPMENT MODE - using local database file")
            print(f"Processing query results from {params.db_path}")
        else:
            # In production mode, execute the query first
            if not execute_query(params):
                sys.exit(2)

        # Process results (whether from dev mode or query execution)
        process_results(params)

    except Exception as e:
        print(f"Error in worker execution: {e}")
        sys.exit(3)

if __name__ == "__main__":
    main()