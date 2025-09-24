import psycopg2 
import requests
import os

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
cursor = conn.cursor()

# === STEP 2: Add the 'first_message' column if not exists ===
cursor.execute("""
    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT FROM information_schema.columns 
            WHERE table_schema = 'atlas' AND table_name = 'conversations' AND column_name = 'first_message'
        ) THEN
            ALTER TABLE atlas.conversations ADD COLUMN first_message TEXT;
        END IF;
    END
    $$;
""")
conn.commit()

# === STEP 3: Fetch only new records where 'first_message' is NULL ===
cursor.execute("SELECT conversation_id FROM atlas.conversations WHERE first_message IS NULL;")
conversation_ids = cursor.fetchall()

# === STEP 4: Process each conversation ID ===
headers = {
    "Authorization": f"Bearer {ATLAS_API_TOKEN}"
}

for (conv_id,) in conversation_ids:
    try:
        api_url = f"{ATLAS_API_BASE}{conv_id}/messages"
        response = requests.get(api_url, headers=headers)
        if response.status_code == 200:
            data = response.json()
            first_message_text = None
            if data.get("data"):
                first_message_text = data["data"][0].get("text", "")
            # Update in DB
            cursor.execute(
                "UPDATE atlas.conversations SET first_message = %s WHERE conversation_id = %s",
                (first_message_text, conv_id)
            )
            print(f"Updated conversation {conv_id}")
        else:
            print(f"Failed for {conv_id} - Status {response.status_code}")
    except Exception as e:
        print(f"Error for {conv_id}: {str(e)}")

# === STEP 5: Commit changes and close DB ===a
conn.commit()
cursor.close()
conn.close()
print("Done!")