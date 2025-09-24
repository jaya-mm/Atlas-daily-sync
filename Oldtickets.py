import os 
import requests
import psycopg2
import time
import json
from datetime import datetime, timezone

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
    conversation_custom_fields JSONB,
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

from datetime import datetime, timezone

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
    """Insert extracted data into PostgreSQL safely handling None values."""
    insert_query = """
INSERT INTO atlas.conversations VALUES (
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
    %(customer_custom_fields)s, %(account_custom_fields)s, %(conversation_custom_fields)s,  %(escalated_at)s
) ON CONFLICT (conversation_id)
DO UPDATE SET
    customer_first_name = COALESCE(EXCLUDED.customer_first_name, atlas.conversations.customer_first_name),
    customer_last_name = COALESCE(EXCLUDED.customer_last_name, atlas.conversations.customer_last_name),
    customer_email = COALESCE(EXCLUDED.customer_email, atlas.conversations.customer_email),
    customer_phone = COALESCE(EXCLUDED.customer_phone, atlas.conversations.customer_phone),
    customer_external_user_id = COALESCE(EXCLUDED.customer_external_user_id, atlas.conversations.customer_external_user_id),
    customer_created_at = COALESCE(EXCLUDED.customer_created_at, atlas.conversations.customer_created_at),
    company_name = COALESCE(EXCLUDED.company_name, atlas.conversations.company_name),
    company_email = COALESCE(EXCLUDED.company_email, atlas.conversations.company_email),
    company_website = COALESCE(EXCLUDED.company_website, atlas.conversations.company_website),
    company_external_id = COALESCE(EXCLUDED.company_external_id, atlas.conversations.company_external_id),
    last_message_text = COALESCE(EXCLUDED.last_message_text, atlas.conversations.last_message_text),
    last_message_channel = COALESCE(EXCLUDED.last_message_channel, atlas.conversations.last_message_channel),
    csat_score = COALESCE(EXCLUDED.csat_score, atlas.conversations.csat_score),
    csat_comment = COALESCE(EXCLUDED.csat_comment, atlas.conversations.csat_comment),
    stats_first_response_time = COALESCE(EXCLUDED.stats_first_response_time, atlas.conversations.stats_first_response_time),
    stats_avg_response_time = COALESCE(EXCLUDED.stats_avg_response_time, atlas.conversations.stats_avg_response_time),
    stats_total_resolution_time = COALESCE(EXCLUDED.stats_total_resolution_time, atlas.conversations.stats_total_resolution_time),
    conversation_status = COALESCE(EXCLUDED.conversation_status, atlas.conversations.conversation_status),
    conversation_priority = COALESCE(EXCLUDED.conversation_priority, atlas.conversations.conversation_priority),
    tags = COALESCE(EXCLUDED.tags, atlas.conversations.tags),
    customer_custom_fields = COALESCE(EXCLUDED.customer_custom_fields, atlas.conversations.customer_custom_fields),
    account_custom_fields = COALESCE(EXCLUDED.account_custom_fields, atlas.conversations.account_custom_fields),
    conversation_custom_fields = COALESCE(EXCLUDED.conversation_custom_fields, atlas.conversations.conversation_custom_fields),
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
                last_message_agent = last_message.get("agent", {}) or {}
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

def main():
    """Fetch and insert/update records from API into PostgreSQL."""
    create_table()
    cursor = 0  # Start from the beginning
    limit = 3000  # Batch size

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

        # Insert/update all fetched records
        insert_into_db(data["data"])
        print(f"Processed {len(data['data'])} records in this batch.")

        cursor += limit  # Move to next batch
        time.sleep(1)  # Pause to avoid rate limiting

    print("Data Sync Complete! Database is now updated with the latest information.")

if __name__ == "__main__":
    main()