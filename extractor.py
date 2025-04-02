"""
  Greenhouse Data Extractor (GHDE)

  Extract all Hiring data from Greenhouse (www.greenhouse.com)
  via their Harvest API (https://developer.greenhouse.io/harvest.html).
  Extracted data is stored in the local filesystem in a format that is
  readily convertible into whatever format you need.

  Script presumes two environment variables are defined:

    API_TOKEN  - a Harvest API key obtained from Greenhouse by a user
                 with appropriate permissions, as explained in the
                 Harvest API documentation.

    CACHE_DIR  - the path in the local filesystem to a directory
                 where the extracted data is to be written.
"""
import base64
import csv
import json
import logging
import os
from datetime import datetime
from pathlib import Path
import fire
import requests
import structlog


class ConfigurationError(RuntimeError):
    pass

class InputDataError(RuntimeError):
    pass


API_TOKEN = os.getenv('API_TOKEN')
if API_TOKEN is None:
    raise ConfigurationError('API_TOKEN is not defined in the environment')
CACHE_DIR = os.getenv('CACHE_DIR')
if CACHE_DIR is None:
    raise ConfigurationError('CACHE_DIR is not defined in the environment')
cache_dir = Path(CACHE_DIR)
if not cache_dir.is_dir():
    raise ConfigurationError(f'CACHE_DIR ({CACHE_DIR}) is not a directory')

BASE_URL = 'https://harvest.greenhouse.io/v1/'

structlog.configure(
    wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
)
log = structlog.get_logger()


def mk_header(api_token: str) -> dict[str, str]:
    "Create HTTP header with authorisation that Greenhouse requires."
    credential = base64.b64encode(
            f"{api_token}:".encode('utf-8')
        ).decode('utf-8')
    return {'Authorization': f'Basic {credential}'}


def parse_link(link: str) -> list[tuple[str, str]]:
    "Parse link item from RFC-5988 response header, returning URIs and their relations"
    contents = []
    duplicates = link.split(',')
    for d in duplicates:
        parts = d.split('; ')
        if len(parts) != 2:
            raise ValueError(f'Unexpected link format: {d}')
        uri = parts[0]
        if uri.startswith('<') and uri.endswith('>'):
            uri = uri[1:-1]
        else:
            raise ValueError(f'Unexpected link URL format: {uri}')
        rel_type = parts[1]
        if rel_type.startswith('rel='):
            rel_type = rel_type[4:][1:-1]
        else:
            raise ValueError(f'Unexpected link relation format: {rel_type}')
        contents.append((uri, rel_type))
    return contents


def select_link(
        parsed_link_contents: list[tuple[str, str]], rel_type: str
    ) -> str|None:
    "Using values returned by parse_link(), select URI according to relation."
    for uri, rel in parsed_link_contents:
        if rel == rel_type:
            return uri
    else:
        return None


def get_paginated(
        query: str,
        headers: dict[str, str],
        params: dict[str,str]
    ) -> list:
    """Make HTTP GET requests to retrieve a resource.

    Multiple linked requests are issued if the endpoint requires this.

    Args:
      query: RESTful suffix to retrieve (e.g. "jobs")
      headers: request headers
      params: request parameters
    """
    log.info('get_paginated', query=query, headers=headers, params=params)
    items = []
    page = 1
    next_uri = BASE_URL + query
    while next_uri is not None:
        response = requests.get(next_uri, headers=headers, params=params)
        log.info(
            'get_paginated: response',
            status=response.status_code,
            ratelimit_remaining=response.headers.get('X-Ratelimit-Remaining')
        )
        if response.status_code != 200:
            if response.status_code == 429:
                # Rate limit exceeded
                wait_secs = response.headers['Retry-After']
                log.info('get_paginated: rate limit exceeded', wait_secs=wait_secs)
                # TODO: implement a sleep here
            raise ValueError(f"Fetch error for {query}: {response.status_code}")
        data = response.json()
        if not data:
            log.info(f'get_paginated: request {page} produced no data')
            break
        items.extend(data)
        link = response.headers.get('link')
        if link is None:
            next_uri = None
        else:
            link_contents = parse_link(link)
            next_uri = select_link(link_contents, 'next')
            # prev_uri = select_link(link_contents, 'prev')
            # last_uri = select_link(link_contents, 'last')
        log.info(f'get_paginated: fetched {len(data)} items on page {page}', next_uri=next_uri)
        page += 1
        params = None  # Subsequent calls will have params in URI
    log.info(f'get_paginated: retrieved {len(items)} items')
    return items


class Entity:
    def __init__(self, retrieval_timestamp: str):
        """Abstract base class for entities (e.g. Job, Candidate, etc).

        Args:
          retrieval_timestamp: ISO-8601 date/time when retrieved from Greenhouse
        """
        self.retrieval_timestamp = retrieval_timestamp

    def moniker(self):
        raise RuntimeError('Entity.moniker() invoked')


class Application(Entity):
    # URL suffix identifying this entity in HTTP GET request
    rest_name = 'applications'

    def __init__(self, job_data: dict, retrieval_timestamp: str):
        self.data = job_data
        super().__init__(retrieval_timestamp)

    def moniker(self) -> str:
        candidate_id = self.data['candidate_id']
        jdata = self.data['jobs']
        job_id = '' if len(jdata) == 0 else jdata[0]['id']
        return f'{candidate_id}/{job_id}'


class Candidate(Entity):
    # URL suffix identifying this entity in HTTP GET request
    rest_name = 'candidates'

    def __init__(self, job_data: dict, retrieval_timestamp: str):
        self.data = job_data
        super().__init__(retrieval_timestamp)

    def moniker(self) -> str:
        first = self.data['first_name']
        last = self.data['last_name']
        if first is None:
            moniker = '' if last is None else last
        elif last is None:
            moniker = first
        else:
            moniker = first + ' ' + last
        return moniker


class Job(Entity):
    # URL suffix identifying this entity in HTTP GET request
    rest_name = 'jobs'

    def __init__(self, job_data: dict, retrieval_timestamp: str):
        self.data = job_data
        super().__init__(retrieval_timestamp)

    def moniker(self) -> str:
        return self.data['name']


class Scorecard(Entity):
    # URL suffix identifying this entity in HTTP GET request
    rest_name = 'scorecards'

    def __init__(self, job_data: dict, retrieval_timestamp: str):
        self.data = job_data
        super().__init__(retrieval_timestamp)

    def moniker(self) -> str:
        return str(self.data['candidate_id'])


def get_entities_from_greenhouse(
        rest_name: str, headers: dict[str, str], params: dict[str,str]
    ) -> tuple[list[Entity], str]:
    "Retrieve all entites of given type from Greenhouse"
    log.info('get_entities_from_greenhouse')
    entities = get_paginated(rest_name, headers, params)
    timestamp = datetime.now().isoformat(timespec='seconds')
    return entities, timestamp


def get_applications_from_greenhouse(
        headers: dict[str, str], params: dict[str,str]
    ) -> list[Job]:
    log.info('get_applications_from_greenhouse')
    applications, timestamp = get_entities_from_greenhouse(Application.rest_name, headers, params)
    return [Application(application_data, timestamp) for application_data in applications]


def get_candidates_from_greenhouse(
        headers: dict[str, str], params: dict[str,str]
    ) -> list[Job]:
    log.info('get_candidates_from_greenhouse')
    candidates, timestamp = get_entities_from_greenhouse(Candidate.rest_name, headers, params)
    return [Candidate(candidate_data, timestamp) for candidate_data in candidates]


def get_jobs_from_greenhouse(
        headers: dict[str, str], params: dict[str,str]
    ) -> list[Job]:
    log.info('get_jobs_from_greenhouse')
    jobs, timestamp = get_entities_from_greenhouse(Job.rest_name, headers, params)
    return [Job(job_data, timestamp) for job_data in jobs]


def get_scorecards_from_greenhouse(
        headers: dict[str, str], params: dict[str,str]
    ) -> list[Job]:
    log.info('get_scorecards_from_greenhouse')
    scorecards, timestamp = get_entities_from_greenhouse(Scorecard.rest_name, headers, params)
    return [Scorecard(scorecard_data, timestamp) for scorecard_data in scorecards]


def read_index(cache_dir: Path, rest_name: str) -> dict[str, tuple]:
    index_file_name = cache_dir / rest_name / 'index.csv'
    entity_summary = {}
    if index_file_name.exists():
        with index_file_name.open('r') as index_file:
            reader = csv.DictReader(index_file)
            for row in reader:
                entity_summary[row['id']] = (row['id'], row['moniker'], row['timestamp'])
    return entity_summary


def write_index(cache_dir: Path, rest_name: str, index: dict[str, tuple]) -> None:
    entity_dir = cache_dir / rest_name
    if not entity_dir.is_dir():
        raise ValueError(f'Missing entity directory: {str(entity_dir)}')
    index_items = sorted(
        [(str(k), v) for k, v in index.items()],
        key=lambda t: t[0]
    )
    index_file_name = entity_dir / 'index.csv'
    log.info('write_index: writing file', file_name=str(index_file_name))
    with index_file_name.open('w', newline='') as index_file:
        writer = csv.writer(index_file, delimiter=',')
        writer.writerow(['id', 'moniker', 'timestamp'])
        for k, v in index_items:
            v = [str(v[0]), v[1], v[2]]
            if ',' in v[1]:
                v[1] = f'"{v[1]}"'
            writer.writerow(v)


def save_entities(cache_dir: Path, entities: list[Entity]) -> None:
    """Write entities to cache as JSON objects, updating the cache index.
    """
    rest_name = entities[0].rest_name
    entity_dir = cache_dir / rest_name
    if not entity_dir.is_dir():
        log.info('save_entities: creating directory', entity_dir=str(entity_dir))
        entity_dir.mkdir()
    new_entity_summary = {}
    for e in entities:
        if e.rest_name != rest_name:
            raise RuntimeError(f'REST name mismatch: {e.rest_name}, {rest_name}')
        entity_id = str(e.data['id'])
        file_name = cache_dir / rest_name / (entity_id + '.json')
        log.info('save_entities: writing file', file_name=str(file_name))
        with file_name.open('w') as ef:
            json.dump(e.data, ef)
        new_entity_summary[entity_id] = (entity_id, e.moniker(), e.retrieval_timestamp)
    entity_summary = read_index(cache_dir, rest_name)
    # Assume new_entity_summary has more recent content
    entity_summary.update(new_entity_summary)
    write_index(cache_dir, rest_name, entity_summary)


def download_candidate_attachment(
        cache_dir: Path, candidate_id: str, attachment: dict[str,str]
    ) -> None:
    "Download and save attachment file to cache."
    entity_dir = cache_dir / Candidate.rest_name
    attach_type = attachment['type']
    attach_filename = attachment['filename']
    attach_url = attachment['url']
    attach_timestamp = attachment['created_at']
    # Replace ':' character with 'c' in timestamp to ensure dir name is portable
    attachment_dir = (entity_dir / (candidate_id + '-attachments')
                    / (attach_type + '-' + attach_timestamp.replace(':', 'c')))
    attachment_dir.mkdir(parents=True, exist_ok=True)
    attachment_filename = attachment_dir / attach_filename
    complete_filename = attachment_dir / 'complete'
    skip = attachment_filename.exists() and complete_filename.exists()
    log.info(
        'download_candidate_attachment', candidate_id=candidate_id,
        filename=attach_filename, type=attach_type, skip=skip
    )
    if not skip:
        complete_filename.unlink(True)
        attachment_filename.unlink(True)
        response = requests.get(attach_url)
        with attachment_filename.open(mode='wb') as af:
            af.write(response.content)
    with complete_filename.open(mode='w') as cf:
        cf.write('')


def get_candidate_attachments(cache_dir: Path) -> None:
    "Get all attachments for cached candidates."
    entity_summary = read_index(cache_dir, Candidate.rest_name)
    entity_dir = cache_dir / Candidate.rest_name
    i = 0
    for v in entity_summary.values():
        entity_file_name = entity_dir / f'{v[0]}.json'
        with entity_file_name.open('r') as fp:
            entity = json.load(fp)
            for attach in entity['attachments']:
                download_candidate_attachment(cache_dir, v[0], attach)
                i += 1
    log.info('get_candidate_attachments', number=i)


def mk_params(
        default_params: dict, after_date: str|None, before_date: str|None
    ) -> dict[str,str]:
    """Assemple HTTP request parameters.

      Args:
        default_params: Values always used
        after_date: Only get records created after this ISO-8601 date
        before_date: Only get records created before this ISO-8601 date
    """
    params = default_params
    if after_date is not None:
        params.update({'created_after': after_date})
    if before_date is not None:
        params.update({'created_before': before_date})
    return params


def main():
    headers = mk_header(API_TOKEN)
    standard_params = {'per_page': 100}
    # Set object creation date window to filter retrieved records:
    #   after_date:   Only retrieve objects created after this date
    #   before_date:  Only retrieve objects created before this date
    after_date = '2023-10-01T00:00:00Z'
    before_date = None
    params = mk_params(standard_params, after_date, before_date)
    log.info('Starting', cache_dir=str(cache_dir), params=params)

    # entrypoints = ['Jobs', 'Candidates', 'Candidate-attachments', 'Applications', 'Scorecards']
    # entrypoints = ['Jobs']
    # entrypoints = ['Candidates']
    # entrypoints = ['Candidate-attachments']
    # entrypoints = ['Applications']
    entrypoints = ['Scorecards']

    if 'Applications' in entrypoints:
        print("Fetching applications...")
        entities = get_applications_from_greenhouse(headers, params)
        print(f"Total entities fetched: {len(entities)}")
        for i, entity in enumerate(entities):
            print(i, entity.data, entity.retrieval_timestamp)
            print('----------------------')
        save_entities(cache_dir, entities)

    if 'Candidates' in entrypoints:
        print("Fetching candidates...")
        entities = get_candidates_from_greenhouse(headers, params)
        print(f"Total entities fetched: {len(entities)}")
        for i, entity in enumerate(entities):
            print(i, entity.data, entity.retrieval_timestamp)
            print('----------------------')
        save_entities(cache_dir, entities)

    if 'Candidate-attachments' in entrypoints:
        get_candidate_attachments(cache_dir)

    if 'Jobs' in entrypoints:
        print("Fetching jobs...")
        entities = get_jobs_from_greenhouse(headers, params)
        print(f"Total entities fetched: {len(entities)}")
        for i, entity in enumerate(entities):
            print(i, entity.data, entity.retrieval_timestamp)
            print('----------------------')
        save_entities(cache_dir, entities)

    if 'Scorecards' in entrypoints:
        print("Fetching scorecards...")
        entities = get_scorecards_from_greenhouse(headers, params)
        print(f"Total entities fetched: {len(entities)}")
        for i, entity in enumerate(entities):
            print(i, entity.data, entity.retrieval_timestamp)
            print('----------------------')
        save_entities(cache_dir, entities)


if __name__ == "__main__":
    fire.Fire(main)
