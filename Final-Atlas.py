import requests
import os 
import psycopg2
import time
import json
from datetime import datetime, timezone

# PostgreSQL Configuration

ATLAS_API_TOKEN = os.environ.get('ATLAS_TOKEN') 

# PostgreSQL Configuration
DB_CONFIG = {
    "dbname": os.environ.get('DB_NAME'),
    "user": os.environ.get('DB_USER'),
    "password": os.environ.get('DB_PASS'),
    "host": os.environ.get('DB_HOST'),
    "port": "5432",
    "sslmode": "require"
}
# API Configuration
API_URL = "https://api.atlas.so/v1/conversations"

HEADERS = {
    "Authorization": f"Bearer {ATLAS_API_TOKEN}",
    "Accept": "application/json"
}
# Query Parameters
LIMIT = 3000  # Fetch records in batches

# Ensure schema is specified correctly
TABLE_CREATION_QUERY = """
CREATE TABLE IF NOT EXISTS atlas.conversations (
    conversation_id UUID PRIMARY KEY,
    customer_id UUID,
    customer_first_name VARCHAR(255),
    customer_last_name VARCHAR(255),
    customer_email VARCHAR(255),
    customer_phone VARCHAR(50),
    customer_external_user_id VARCHAR(255),
    customer_created_at TIMESTAMP,
    company_id UUID,
    company_name VARCHAR(255),
    company_email VARCHAR(255),
    company_website VARCHAR(255),
    company_external_id VARCHAR(255),
    started_at TIMESTAMP,
    closed_at TIMESTAMP,
    created_at TIMESTAMP,
    assigned_at TIMESTAMP,
    assigned_by UUID,
    closed_by UUID,
    assigned_agent_id UUID,
    assigned_agent_name VARCHAR(255),
    assigned_agent_email VARCHAR(255),
    assigned_agent_created_at TIMESTAMP,
    browser VARCHAR(255),
    operating_system VARCHAR(255),
    last_message_id INTEGER,
    last_message_text TEXT,
    last_message_channel VARCHAR(255),
    csat_score VARCHAR(10),
    csat_comment TEXT,
    stats_first_response_time FLOAT,
    stats_avg_response_time FLOAT,
    stats_total_resolution_time FLOAT,
    conversation_status VARCHAR(50),
    conversation_priority VARCHAR(50),
    conversation_subject TEXT,
    assigned_team_id UUID,
    updated_by UUID,
    tags TEXT[],
    snoozed_until TIMESTAMP,
    started_channel VARCHAR(255),
    started_sub_channel VARCHAR(255),
    number INTEGER,
    customer_custom_fields JSONB, 
    account_custom_fields JSONB,  
    conversation_custom_fields JSONB ,
    escalated_at TIMESTAMP
);
"""

def connect_db():
    """Establish connection to PostgreSQL and set schema."""
    conn = psycopg2.connect(**DB_CONFIG)
    with conn.cursor() as cur:
        cur.execute("SET search_path TO atlas;")
        conn.commit()
    return conn

def create_table():
    """Ensure the database table exists before inserting data."""
    with connect_db() as conn:
        with conn.cursor() as cur:
            cur.execute(TABLE_CREATION_QUERY)
            conn.commit()

def fetch_conversations(cursor):
    """Fetch conversations from API using cursor for pagination."""
    today_date = datetime.today().strftime("%Y-%m-%d")
    
    params = {
        "cursor": cursor,
        "limit": LIMIT,
        "startDate": "2021-01-01",
        "endDate": today_date
    }
    
    response = requests.get(API_URL, headers=HEADERS, params=params)
    if response.status_code != 200:
        print(f"[ERROR] API request failed, Status: {response.status_code} - {response.text}")
        return None
    return response.json()

def convert_to_timestamp(value):
    """Convert UNIX timestamps & ISO timestamps safely."""
    if isinstance(value, datetime):
        return value
    if isinstance(value, int):
        return datetime.fromtimestamp(value, timezone.utc)
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%fZ")
    except ValueError:
        return None

def insert_into_db(data):
    """Insert extracted data into PostgreSQL, updating only the escalated_at field on conflict."""
    insert_query = """
    INSERT INTO atlas.conversations (
        conversation_id, customer_id, customer_first_name, customer_last_name,
        customer_email, customer_phone, customer_external_user_id, customer_created_at,
        company_id, company_name, company_email, company_website, company_external_id,
        started_at, closed_at, created_at, assigned_at, assigned_by,
        closed_by, assigned_agent_id, assigned_agent_name, assigned_agent_email,
        assigned_agent_created_at, browser, operating_system,
        last_message_id, last_message_text, last_message_channel,
        csat_score, csat_comment,
        stats_first_response_time, stats_avg_response_time, stats_total_resolution_time,
        conversation_status, conversation_priority, conversation_subject,
        assigned_team_id, updated_by, tags,
        snoozed_until, started_channel, started_sub_channel, number,
        customer_custom_fields, account_custom_fields, conversation_custom_fields,
        escalated_at
    ) VALUES (
        %(conversation_id)s, %(customer_id)s, %(customer_first_name)s, %(customer_last_name)s,
        %(customer_email)s, %(customer_phone)s, %(customer_external_user_id)s, %(customer_created_at)s,
        %(company_id)s, %(company_name)s, %(company_email)s, %(company_website)s, %(company_external_id)s,
        %(started_at)s, %(closed_at)s, %(created_at)s, %(assigned_at)s, %(assigned_by)s,
        %(closed_by)s, %(assigned_agent_id)s, %(assigned_agent_name)s, %(assigned_agent_email)s,
        %(assigned_agent_created_at)s, %(browser)s, %(operating_system)s,
        %(last_message_id)s, %(last_message_text)s, %(last_message_channel)s,
        %(csat_score)s, %(csat_comment)s,
        %(stats_first_response_time)s, %(stats_avg_response_time)s, %(stats_total_resolution_time)s,
        %(conversation_status)s, %(conversation_priority)s, %(conversation_subject)s,
        %(assigned_team_id)s, %(updated_by)s, %(tags)s,
        %(snoozed_until)s, %(started_channel)s, %(started_sub_channel)s, %(number)s,
        %(customer_custom_fields)s, %(account_custom_fields)s, %(conversation_custom_fields)s,
        %(escalated_at)s
    ) ON CONFLICT (conversation_id) 
    DO UPDATE SET 
        escalated_at = CASE
                           WHEN EXCLUDED.escalated_at IS NOT NULL THEN EXCLUDED.escalated_at
                           ELSE atlas.conversations.escalated_at
                       END;
    """
    with connect_db() as conn:
        with conn.cursor() as cur:
            for conversation in data:
                if "id" not in conversation:
                    print(f"Skipping record: Missing 'conversation_id': {conversation}")
                    continue
                
                # Extract Nested Data Safely
                customer = conversation.get("customer", {}) or {}
                account = customer.get("account", {}) or {}
                assigned_agent = conversation.get("assignedAgent", {}) or {}
                last_message = conversation.get("lastMessage", {}) or {}
                csat = conversation.get("csat", {}) or {} 
                stats = conversation.get("statistics", {}) or {} 

                # Extract JSON fields safely
                customer_custom_fields = json.dumps(customer.get("customFields", {}))
                account_custom_fields = json.dumps(account.get("customFields", {}))
                conversation_custom_fields = json.dumps(conversation.get("customFields", {}))

                # Ensure 'tags' is always a list
                tags = conversation.get("tags", []) or []

                cur.execute(insert_query, {
                    "conversation_id": conversation.get("id"),
                    "customer_id": customer.get("id"),
                    "customer_first_name": customer.get("firstName"),
                    "customer_last_name": customer.get("lastName"),
                    "customer_email": customer.get("email"),
                    "customer_phone": customer.get("phoneNumber"),
                    "customer_external_user_id": customer.get("externalUserId"),
                    "customer_created_at": convert_to_timestamp(customer.get("createdAt")),
                    "company_id": customer.get("companyId"),
                    "company_name": account.get("name"),
                    "company_email": account.get("email"),
                    "company_website": account.get("website"),
                    "company_external_id": account.get("externalId"),
                    "started_at": convert_to_timestamp(conversation.get("startedAt")),
                    "closed_at": convert_to_timestamp(conversation.get("closedAt")),
                    "created_at": convert_to_timestamp(conversation.get("createdAt")),
                    "assigned_at": convert_to_timestamp(conversation.get("assignedAt")),
                    "assigned_by": conversation.get("assignedBy"),
                    "closed_by": conversation.get("closedBy"),
                    "assigned_agent_id": assigned_agent.get("id"),
                    "assigned_agent_name": assigned_agent.get("firstName"),
                    "assigned_agent_email": assigned_agent.get("email"),
                    "assigned_agent_created_at": convert_to_timestamp(assigned_agent.get("createdAt")),
                    "browser": conversation.get("browser"),
                    "operating_system": conversation.get("operatingSystem"),
                    "last_message_id": last_message.get("id"),
                    "last_message_text": last_message.get("text"),
                    "last_message_channel": last_message.get("channel"),
                    "csat_score": csat.get("score", None),
                    "csat_comment": csat.get("comment", None),
                    "stats_first_response_time": stats.get("firstResponseTime", None),
                    "stats_avg_response_time": stats.get("avgResponseTime", None),
                    "stats_total_resolution_time": stats.get("totalResolutionTime", None),
                    "conversation_status": conversation.get("status"),
                    "conversation_priority": conversation.get("priority"),
                    "conversation_subject": conversation.get("subject"),
                    "assigned_team_id": conversation.get("assignedTeamId"),
                    "updated_by": conversation.get("updatedBy"),
                    "tags": conversation.get("tags", []) or [],
                    "snoozed_until": convert_to_timestamp(conversation.get("snoozedUntil")),
                    "started_channel": conversation.get("startedChannel"),
                    "started_sub_channel": conversation.get("startedSubChannel"),
                    "number": conversation.get("number"),
                    "customer_custom_fields": json.dumps(customer.get("customFields", {})), 
                    "account_custom_fields": json.dumps(account.get("customFields", {})), 
                    "conversation_custom_fields": json.dumps(conversation.get("customFields", {})),
                    "escalated_at": convert_to_timestamp(conversation.get("escalatedAt"))
                })
            conn.commit()

# ---
# The rest of the script is unchanged.
def get_existing_record_ids():
    """Fetch all existing conversation IDs from the database."""
    with connect_db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT conversation_id FROM atlas.conversations;")
            records = cur.fetchall()
            return {record[0] for record in records}

def main():
    """Fetch and upsert records from API into PostgreSQL."""
    create_table()
    cursor = 0
    limit = 3000

    # Get total available records from API
    initial_data = fetch_conversations(cursor)
    if not initial_data or "total" not in initial_data:
        print("Failed to retrieve total records. Exiting.")
        return

    total_records = initial_data["total"]
    print(f"Total records available in API: {total_records}")

    # Process records in batch
    while cursor < total_records:
        print(f"Fetching batch with cursor: {cursor}")
        data = fetch_conversations(cursor)
        if not data or "data" not in data or not data["data"]:
            print(f"No more data to process after cursor {cursor}. Exiting.")
            break

        records_to_process = data["data"]
        insert_into_db(records_to_process)
        print(f"Processed and synchronized {len(records_to_process)} records.")

        cursor += limit
        time.sleep(1)

    print("Data Sync Complete! Database is now fully updated.")

if __name__ == "__main__":
    main()