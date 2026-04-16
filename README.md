# Astra DB (Cassandra) Movie Project

<p align="left">
  <img src="https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white" alt="Python" />
  <img src="https://img.shields.io/badge/Apache_Cassandra-1287B1?style=for-the-badge&logo=apachecassandra&logoColor=white" alt="Apache Cassandra" />
  <img src="https://img.shields.io/badge/Astra_DB-0A66C2?style=for-the-badge&logo=datastax&logoColor=white" alt="Astra DB" />
  <img src="https://img.shields.io/badge/CQL-Query_Language-4B5563?style=for-the-badge" alt="CQL" />
  <img src="https://img.shields.io/badge/BLOB-Image_Storage-2E8B57?style=for-the-badge" alt="BLOB" />
  <img src="https://img.shields.io/badge/TTL-Column_Expiry-F59E0B?style=for-the-badge" alt="TTL" />
</p>

This project implements a movie database workflow on Astra DB using Cassandra CQL and Python.
It covers schema design, data insertion, poster upload as BLOB, actor/director search, and TTL behavior.

## What This Project Demonstrates

- Creating a Cassandra keyspace and table for movie data.
- Using a nested collection: `map<text, frozen<list<text>>>` for cast metadata.
- Storing and retrieving poster images as `blob` values.
- Applying TTL to inserted/updated column values.
- Implementing application-level filtering for actor/director search.

## Database Setup (CQL)

### 1) Keyspace

```sql
CREATE KEYSPACE "Movies"
WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': 3 };
```

### 2) Table

```sql
CREATE TABLE "Movie" (
  "Id" int PRIMARY KEY,
  name text,
  "movie-cast" map<text, frozen<list<text>>>,
  "movie-poster" blob
);
```

### 3) Initial Row with 7-Day TTL

```sql
INSERT INTO "Movie"("Id", "name", "movie-cast")
VALUES (
  1,
  'The Godfather',
  {
    'Director': ['Francis Ford Coppola'],
    'Actors': ['Marlon Brando', 'Al Pacino'],
    'Music': ['Nino Rota']
  }
)
USING TTL 604800;
```

### 4) Additional Inserts

```sql
INSERT INTO "Movie"("Id", "name", "movie-cast")
VALUES (
  4,
  'Interstellar',
  {
    'Director': ['Christopher Nolan'],
    'Actors': ['Matthew McConaughey', 'Anne Hathaway'],
    'Music': ['Hans Zimmer']
  }
);

INSERT INTO "Movie"("Id", "name", "movie-cast")
VALUES (
  2,
  'Good Will Hunting',
  {
    'Director': ['Gus Van Sant'],
    'Actors': ['Matt Damon', 'Robin Williams'],
    'Music': ['Danny Elfman']
  }
);

INSERT INTO "Movie"("Id", "name", "movie-cast")
VALUES (
  3,
  'Fight Club',
  {
    'Director': ['David Fincher'],
    'Actors': ['Brad Pitt', 'Edward Norton'],
    'Music': ['Dust Brothers']
  }
);
```

### 5) Update Frozen Cast Field

Because `movie-cast` contains frozen lists, the full field is rewritten during updates.

```sql
UPDATE "Movie"
SET "movie-cast" = {
  'Director': ['David Fincher'],
  'Actors': ['Brad Pitt', 'Edward Norton', 'Helena Bonham Carter'],
  'Music': ['Dust Brothers']
}
WHERE "Id" = 3;
```

### 6) TTL Update Example

```sql
UPDATE "Movie"
USING TTL 3
SET name = 'The Godfather',
    "movie-cast" = {
      'Director': ['Francis Ford Coppola'],
      'Actors': ['Marlon Brando', 'Al Pacino'],
      'Music': ['Nino Rota']
    },
    "movie-poster" = null
WHERE "Id" = 1;
```

After TTL expires, non-primary-key columns expire while the row key remains.

## Python Scripts

- `PosterScript.py`
  - Connects to Astra DB.
  - Reads poster files from `Posters/`.
  - Uploads posters into `"movie-poster"` as BLOB values.

- `SearchFunction.py`
  - Connects to Astra DB.
  - Prompts for a director or actor name.
  - Retrieves matching movies using application-level filtering.
  - Writes returned poster bytes to `output/`.

## Environment Configuration

Create `.env` in the project root (already supported by both scripts):

```env
ASTRA_APPLICATION_TOKEN=AstraCS:...
ASTRA_SECURE_CONNECT_BUNDLE=secure-connect-assi.zip
ASTRA_KEYSPACE=Movies
```

Legacy fallback (older tokens only):

```env
ASTRA_CLIENT_ID=your_astra_client_id
ASTRA_CLIENT_SECRET=your_astra_client_secret
```

## How to Run

1. Create and activate a Python environment.
2. Install dependency:

```bash
pip install cassandra-driver
```

3. Put your Astra secure connect bundle zip in the project root.
4. Fill `.env` with valid Astra credentials.
5. Run poster upload:

```bash
python "PosterScript.py"
```

6. Run search:

```bash
python "SearchFunction.py"
```

7. Run a safe connection-only test:

```bash
python "test_astra_connection.py"
```

## Project Structure

```text
Project Instructions.pdf
PosterScript.py
SearchFunction.py
test_astra_connection.py
Posters/
output/
.env
.env.example
.gitignore
README.md
```

## Cassandra Design Notes

- Search is implemented at the application level because filtering inside nested collection data is limited in Cassandra without patterns that are outside this assignment scope.
- Binary poster storage uses `blob` without format interpretation.
- TTL in Cassandra is column-level for non-key columns.

## Team

- Hady El Fadaly - [Github Profile](https://github.com/hadyelfadaly)
- Hozayfa Ashraf - [Github Profile](https://github.com/HozayfaAshraf)
- Omar Waleed El Sobky - [Github Profile](https://github.com/Omarsobky)
- Yassin Mohy Eldin - [Github Profile](https://github.com/Yassin-Mohy)
- Ibrahim Wael El Noty - [Github Profile]()
