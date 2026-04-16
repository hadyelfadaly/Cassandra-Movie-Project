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

cloud_config = {"secure_connect_bundle": secure_connect_bundle}
if astra_application_token:
    # Modern Astra auth for Cassandra drivers: username must be literal "token".
    auth_provider = PlainTextAuthProvider(username="token", password=astra_application_token)
else:
    auth_provider = PlainTextAuthProvider(
        username=astra_client_id,
        password=astra_client_secret
    )
cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
session = cluster.connect()

session.execute(f'USE "{astra_keyspace}"')

print("Connected successfully to astra")

def query_movies_by_director_or_actor():

    person = input("Enter director or actor name: ").strip().lower()

    query = '''
        SELECT "Id", name, "movie-cast", "movie-poster"
        FROM "Movie";
    '''

    rows = session.execute(query)
    output_dir = 'output'

    os.makedirs(output_dir, exist_ok=True)

    found = False

    for row in rows:
        # attribute access (not dictionary)
        movie_cast = row.movie_cast or {}

        # normalize map<text, list<text>>
        normalized_cast = {
            k.lower(): [name.lower() for name in v]
            for k, v in movie_cast.items()
        }

        directors = normalized_cast.get('director', [])
        actors = normalized_cast.get('actors', [])

        if person in directors or person in actors:

            found = True
            print("ID:", row.Id)
            print("Name:", row.name)
            print("Cast:", movie_cast)

            if row.movie_poster:

                poster_path = os.path.join(
                    output_dir,
                    f"{row.name.replace(' ', '_')}_poster.jpg"
                )
                with open(poster_path, 'wb') as f:
                    f.write(row.movie_poster)

                print("Poster saved:", poster_path)

            print("-" * 30)

    if not found:

        print("No movies found for:", person)


if __name__ == "__main__":
    query_movies_by_director_or_actor()


