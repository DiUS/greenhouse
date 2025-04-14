# greenhouse
Greenhouse Data Extractor

With thanks to https://github.com/Siddhant-K-code/greenhouse-data-exporter

## Running GHDE

### Commands

Records representing the major entities in the Harvest API can be retrieved in a single operation for each entity type. These commands can be modified by the _before_ and _after_ creation date parameters.

The following optional command parameters filter the retrieved records by their creation date. Each requires an additional value specifying the date in ISO-8601 format.

```
--created_after (-a)
--created_before (-b)
```

Entity retrieval commands:
- applications
- candidates
- jobs
- offers
- prospect_pools
- scorecards
- sources

e.g. To extract all candidate records created after 1st November 2022:

```
  python extractor.py candidates -a 2022-11-01
```

Some commands use the candidate records already extracted into the cache.
These should be run after all required candidates have been extracted.

Candidate-dependent commands:
- activity_feeds
- attachments

Cache administration commands:
- check

## Cache organisation

The cache is a local folder hierarchy that uses the file system to store one file per entity instance. The top-level folders are named according to the entity type they hold. Within that folder are individual JSON files, each holding one instance whose _id_ is used as the filename.

Each entity folder also has an `index.csv` file which has one row per entity instance file in that folder. The columns are:
- `id` : the record _id_
- `moniker` : the record name or some useful identifier
- `timestamp` : the time when the record was extracted from Greenhouse.

The hierarchy is:

    ├── applications       <- All the applications.
    ├── candidates         <- All the candidates and their attachments.
    ├── jobs               <- All the jobs.
    ├── offers             <- All the offers.
    ├── prospect_pools     <- All the prospect pools.
    ├── scorecards         <- All the scorecards.
    ├── sources            <- All the sources.

### Attachments

Within the _candidates_ folder, any candidate having a file attachment has a subfolder named like `XXXX-attachments`, where `XXXX` is the _candidate id_. Within that subfolder are one or more subfolders - one per attachment for that candidate. Each of these subfolders is named according to the attachment type (`resume`, `cover_letter`, `other`) and the date when it was uploaded to Greenhouse. Within that lower subfolder should be two files: the attachment file itself and a completion marker file named `complete` that indicates the file was completely downloaded from Greenhouse.

