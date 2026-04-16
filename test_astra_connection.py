import os
import socket
from cassandra import DriverException
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster


def strip_wrapping_quotes(value):
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
        return value[1:-1]
    return value


def load_env_file(env_path=".env"):
    if not os.path.exists(env_path):
        return

    with open(env_path, "r", encoding="utf-8") as f:
        for raw_line in f:
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = strip_wrapping_quotes(value.strip())
            if key and key not in os.environ:
                os.environ[key] = value


load_env_file()

application_token = os.getenv("ASTRA_APPLICATION_TOKEN") or os.getenv("ASTRA_DB_APPLICATION_TOKEN")
client_id = os.getenv("ASTRA_CLIENT_ID")
client_secret = os.getenv("ASTRA_CLIENT_SECRET")
bundle_path = os.getenv("ASTRA_SECURE_CONNECT_BUNDLE", "secure-connect-assi.zip")
keyspace = os.getenv("ASTRA_KEYSPACE", "Movies")

if not application_token and not (client_id and client_secret):
    raise SystemExit(
        "Missing auth config. Set ASTRA_APPLICATION_TOKEN (recommended), "
        "or ASTRA_CLIENT_ID and ASTRA_CLIENT_SECRET (legacy)."
    )

if not os.path.exists(bundle_path):
    raise SystemExit(f"Secure connect bundle not found: {bundle_path}")

cluster = None

try:
    if application_token:
        # Modern Astra auth for Cassandra drivers: username must be literal "token".
        auth_provider = PlainTextAuthProvider(username="token", password=application_token)
    else:
        auth_provider = PlainTextAuthProvider(username=client_id, password=client_secret)

    cluster = Cluster(cloud={"secure_connect_bundle": bundle_path}, auth_provider=auth_provider)
    session = cluster.connect()
    session.execute(f'USE "{keyspace}"')
    row = session.execute("SELECT release_version FROM system.local").one()

    print("Connection test: SUCCESS")
    print(f"Keyspace selected: {keyspace}")
    print(f"Cassandra release version: {row.release_version}")

except DriverException as ex:
    message = str(ex)
    print("Connection test: FAILED")
    print(message)

    if "metadata service" in message or "Name or service not known" in message:
        print("Hint: your secure connect bundle endpoint cannot be resolved.")
        print("Download a fresh bundle from the Astra console for a RUNNING database,")
        print("then update ASTRA_SECURE_CONNECT_BUNDLE in .env if the filename changed.")
    elif "deprecated authentication method" in message.lower() or "bad credentials" in message.lower():
        print("Hint: this endpoint requires modern application token authentication.")
        print("Set ASTRA_APPLICATION_TOKEN to AstraCS:... in .env")
        print("and use a fresh token from Astra Portal -> Database -> Generate Token.")

except socket.gaierror as ex:
    print("Connection test: FAILED")
    print(f"DNS resolution error: {ex}")

finally:
    if cluster is not None:
        cluster.shutdown()
