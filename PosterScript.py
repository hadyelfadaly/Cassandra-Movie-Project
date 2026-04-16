from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import os


def strip_wrapping_quotes(value):
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
        return value[1:-1]
    return value


def load_env_file(env_path=".env"):
    if not os.path.exists(env_path):
        return

    with open(env_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = strip_wrapping_quotes(value.strip())
            if key and key not in os.environ:
                os.environ[key] = value


load_env_file()

astra_application_token = os.getenv("ASTRA_APPLICATION_TOKEN") or os.getenv("ASTRA_DB_APPLICATION_TOKEN")
astra_client_id = os.getenv("ASTRA_CLIENT_ID")
astra_client_secret = os.getenv("ASTRA_CLIENT_SECRET")
secure_connect_bundle = os.getenv("ASTRA_SECURE_CONNECT_BUNDLE", "secure-connect-assi.zip")
astra_keyspace = os.getenv("ASTRA_KEYSPACE", "Movies")

if not astra_application_token and not (astra_client_id and astra_client_secret):
    raise ValueError(
        "Missing auth config. Set ASTRA_APPLICATION_TOKEN (recommended), "
        "or ASTRA_CLIENT_ID and ASTRA_CLIENT_SECRET (legacy)."
    )

cloud_config = {
    "secure_connect_bundle": secure_connect_bundle
}

if astra_application_token:
    # Modern Astra auth for Cassandra drivers: username must be literal "token".
    auth_provider = PlainTextAuthProvider(username="token", password=astra_application_token)
else:
    auth_provider = PlainTextAuthProvider(
        username=astra_client_id,
        password=astra_client_secret
    )

cluster = Cluster(
    cloud=cloud_config,
    auth_provider=auth_provider
)

session = cluster.connect()
session.execute(f'USE "{astra_keyspace}"')

print("Connected successfully to astra")

# ---------- POSTER FOLDER ----------
POSTER_FOLDER = "Posters"

# filename -> movie Id mapping
movie_id_map = {

    "Fight Club Poster.jfif": 3,
    "Good Will Hunting Poster.jfif": 2,
    "Interstellar Poster.jpg": 4,
    "The Godfather Poster.avif": 1

}

# ---------- UPDATE POSTERS ----------
for filename, movie_id in movie_id_map.items():

    poster_path = os.path.join(POSTER_FOLDER, filename)

    if not os.path.exists(poster_path):

        print(f" File not found: {filename}")

        continue

    # read image as bytes (BLOB)
    with open(poster_path, "rb") as f:

        poster_blob = f.read()

    session.execute(
        'UPDATE "Movie" SET "movie-poster" = %s WHERE "Id" = %s',
        (poster_blob, movie_id)
    )

    print(f" Updated poster for movie Id {movie_id}")

print("All posters uploaded successfully")

