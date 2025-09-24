import os 
import psycopg2
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

# === CONFIG ===
DB_HOST = os.environ.get('DB_HOST')
DB_NAME = os.environ.get('DB_NAME')
DB_USER = os.environ.get('DB_USER')
DB_PASSWORD = os.environ.get('DB_PASS') 
ATLAS_API_BASE = 'https://api.atlas.so/v1/conversations/'
ATLAS_API_TOKEN = os.environ.get('ATLAS_TOKEN') 

# === STEP 1: Connect to DB ===
conn = psycopg2.connect(
    host=DB_HOST,
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD,
    port='5432',
    sslmode='require'
)
conn.autocommit = True  # Enable autocommit
cursor = conn.cursor()

# === STEP 2: Add the 'ticket_number' column if not exists ===
cursor.execute("""
    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT FROM information_schema.columns 
            WHERE table_schema = 'atlas' AND table_name = 'conversations' AND column_name = 'ticket_number'
        ) THEN
            ALTER TABLE atlas.conversations ADD COLUMN ticket_number TEXT;
        END IF;
    END
    $$;
""")

# === STEP 3: Fetch only new records where 'ticket_number' is NULL ===
cursor.execute("SELECT conversation_id FROM atlas.conversations WHERE ticket_number IS NULL;")
conversation_ids = cursor.fetchall()

# === STEP 4: Parallel processing ===
headers = {
    "Authorization": f"Bearer {ATLAS_API_TOKEN}"
}

def process_conversation(conv_id):
    try:
        api_url = f"{ATLAS_API_BASE}{conv_id}"
        response = requests.get(api_url, headers=headers, timeout=10)
        if response.status_code == 200:
            data = response.json()
            number = data.get("number")

            # Reconnect to DB in thread (each thread needs its own cursor)
            local_conn = psycopg2.connect(
                host=DB_HOST,
                dbname=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD,
                port='5432',
                sslmode='require'
            )
            local_conn.autocommit = True
            local_cursor = local_conn.cursor()

            local_cursor.execute(
                "UPDATE atlas.conversations SET ticket_number = %s WHERE conversation_id = %s",
                (str(number) if number is not None else None, conv_id)
            )

            local_cursor.close()
            local_conn.close()

            return f"✅ Updated conversation {conv_id} with ticket number: {number}"
        else:
            return f"❌ Failed for {conv_id} - Status code: {response.status_code}"
    except Exception as e:
        return f"❌ Error for {conv_id}: {str(e)}"

# Run in parallel (10 threads)
results = []
with ThreadPoolExecutor(max_workers=10) as executor:
    futures = {executor.submit(process_conversation, conv_id[0]): conv_id[0] for conv_id in conversation_ids}
    for future in as_completed(futures):
        result = future.result()
        results.append(result)

# Close original cursor and connection
cursor.close()
conn.close()


