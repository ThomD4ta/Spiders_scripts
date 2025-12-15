# python3 pokeapi.py
# Pulls data from the https://pokeapi.co/ API and puts it into our pokeapi table
# Requirements: psycopg2 (or psycopg2-binary), requests
# Make sure your hidden.py file contains DBNAME, DBUSER, DBPASSWORD, DBHOST, DBPORT

import time
import requests
import psycopg2
from psycopg2.extras import Json
import hidden

# ---- Config ----
START_ID = 1
END_ID = 100
BASE_URL_TEMPLATE = "https://pokeapi.co/api/v2/pokemon/{id}/"
SLEEP_BETWEEN_REQUESTS = 0.3  # be polite to the API
MAX_RETRIES = 3
RETRY_BACKOFF = 1.0  # seconds * attempt

# ---- Connect to DB ----
conn = psycopg2.connect(
    dbname=hidden.DBNAME,
    user=hidden.DBUSER,
    password=hidden.DBPASSWORD,
    host=hidden.DBHOST,
    port=hidden.DBPORT,
    connect_timeout=3
)
cur = conn.cursor()

# ---- Create table if it doesn't exist ----
create_table_sql = '''
CREATE TABLE IF NOT EXISTS pokeapi (
    id serial PRIMARY KEY,
    name VARCHAR(255),
    url VARCHAR(2048) UNIQUE, 
    status INTEGER, 
    body JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), 
    updated_at TIMESTAMPTZ
);
'''
cur.execute(create_table_sql)
conn.commit()

# ---- Helper: upsert row ----
def upsert_pokemon(cur, name, url, status, body_json):
    """Insert or update a row by url (upsert)."""
    sql = """
    INSERT INTO pokeapi (name, url, status, body, updated_at)
    VALUES (%s, %s, %s, %s, now())
    ON CONFLICT (url) DO UPDATE
      SET name = EXCLUDED.name,
          status = EXCLUDED.status,
          body = EXCLUDED.body,
          updated_at = now();
    """
    cur.execute(sql, (name, url, status, Json(body_json) if body_json is not None else None))

# ---- Spider loop ----
session = requests.Session()
session.headers.update({"User-Agent": "pokeapi-spider/1.0 (+https://example.com)"})

for i in range(START_ID, END_ID + 1):
    url = BASE_URL_TEMPLATE.format(id=i)
    attempt = 0
    success = False

    while attempt < MAX_RETRIES and not success:
        attempt += 1
        try:
            resp = session.get(url, timeout=10)
            status = resp.status_code

            if status == 200:
                # parse JSON safely
                try:
                    response_json = resp.json()
                except ValueError:
                    response_json = None

                if response_json:
                    # Extraer campos relevantes del JSON
                    abilities = [
                        {"name": ab["ability"]["name"]}
                        for ab in response_json.get("abilities", [])
                    ]
                    types = [
                        {"name": t["type"]["name"]}
                        for t in response_json.get("types", [])
                    ]
                    species = None
                    if response_json.get("species"):
                        species = {
                            "name": response_json["species"].get("name")
                        }

                    # Extraer otros campos de interés opcionales
                    extracted = {
                        "pokemon_id": response_json.get("id"),
                        "pokemon_name": response_json.get("name"),
                        "height": response_json.get("height"),
                        "weight": response_json.get("weight"),
                        "abilities": abilities,
                        "types": types,
                        "species": species,
                        # puedes agregar más campos si los necesitas
                    }

                    # Inserta/actualiza la fila con el body reducido (extracted)
                    upsert_pokemon(cur, response_json.get("name"), url, status, extracted)
                    conn.commit()
                    print(f"[{i}] OK {status} -> {url}")
                    success = True
                else:
                    # respuesta 200 pero JSON inválido
                    upsert_pokemon(cur, None, url, 200, None)
                    conn.commit()
                    print(f"[{i}] 200 but invalid JSON -> {url}")
                    success = True

            else:
                # Para respuestas no-200, registrar status y body vacío
                upsert_pokemon(cur, None, url, status, None)
                conn.commit()
                print(f"[{i}] NON-200 {status} -> {url}")
                success = True  # no reintentamos para 4xx/5xx por defecto

        except requests.RequestException as e:
            # Error de red o timeout — reintentar con backoff
            print(f"[{i}] Request error on attempt {attempt}: {e}. Retrying...")
            time.sleep(RETRY_BACKOFF * attempt)

    # Si al final no se logró, insertar marcador de fallo
    if not success:
        print(f"[{i}] FAILED after {MAX_RETRIES} attempts. Inserting failure marker.")
        upsert_pokemon(cur, None, url, -1, None)
        conn.commit()

    # Espera para no saturar la API
    time.sleep(SLEEP_BETWEEN_REQUESTS)

# ---- Summary (simple counts) ----
cur.execute("SELECT COUNT(*) FROM pokeapi;")
total = cur.fetchone()[0]
cur.execute("SELECT COUNT(*) FROM pokeapi WHERE status IS NULL;")
todo = cur.fetchone()[0]
cur.execute("SELECT COUNT(*) FROM pokeapi WHERE status = 200;")
good = cur.fetchone()[0]
cur.execute("SELECT COUNT(*) FROM pokeapi WHERE status != 200;")
error = cur.fetchone()[0]

print("=== Summary ===")
print(f"Total rows in pokeapi table: {total}")
print(f"Rows with status NULL: {todo}")
print(f"Rows with status=200: {good}")
print(f"Rows with status!=200: {error}")

# ---- Cleanup ----
cur.close()
conn.close()
