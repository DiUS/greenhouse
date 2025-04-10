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
from collections import defaultdict
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

    def __init__(self, application_data: dict, retrieval_timestamp: str):
        self.data = application_data
        super().__init__(retrieval_timestamp)

    def moniker(self) -> str:
        candidate_id = self.data['candidate_id']
        jdata = self.data['jobs']
        job_id = '' if len(jdata) == 0 else jdata[0]['id']
        return f'{candidate_id}/{job_id}'


class Candidate(Entity):
    # URL suffix identifying this entity in HTTP GET request
    rest_name = 'candidates'

    def __init__(self, candidate_data: dict, retrieval_timestamp: str):
        self.data = candidate_data
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


class Offer(Entity):
    # URL suffix identifying this entity in HTTP GET request
    rest_name = 'offers'

    def __init__(self, offer_data: dict, retrieval_timestamp: str):
        self.data = offer_data
        super().__init__(retrieval_timestamp)

    def moniker(self) -> str:
        candidate_id = self.data['candidate_id']
        application_id = self.data['application_id']
        return f'{candidate_id}/{application_id}'


class Scorecard(Entity):
    # URL suffix identifying this entity in HTTP GET request
    rest_name = 'scorecards'

    def __init__(self, scorecard_data: dict, retrieval_timestamp: str):
        self.data = scorecard_data
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


def get_offers_from_greenhouse(
        headers: dict[str, str], params: dict[str,str]
    ) -> list[Job]:
    log.info('get_offers_from_greenhouse')
    offers, timestamp = get_entities_from_greenhouse(Offer.rest_name, headers, params)
    return [Offer(offer_data, timestamp) for offer_data in offers]


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


def mk_attachment_path(prefix_dir: Path, attachment: dict[str,str]) -> Path:
    attach_type = attachment['type']
    attach_timestamp = attachment['created_at']
    # Replace ':' character with 'c' in timestamp to ensure dir name is portable
    return (prefix_dir /
        (attach_type + '-' + attach_timestamp.replace(':', 'c')))


def mk_attachment_index(cache_dir: Path) -> dict[str,list[tuple]]:
    """Create index of all candidate attachments.

      Returns a mapping candidate_id -> list of tuples, each with attributes of
      one attachment associated with that candidate.
    """
    entity_dir = cache_dir / Candidate.rest_name
    attachment_index = defaultdict(list)
    for attachment_dir in entity_dir.glob('*-attachments'):
        candidate_id = attachment_dir.name.split('-')[0]
        for at_subdir in attachment_dir.iterdir():
            parts = at_subdir.name.split('-', 1)
            attachment_type = parts[0]
            attachment_date = parts[1].replace('c', ':')
            attachment_filename = None
            complete = False
            for f in at_subdir.iterdir():
                if f.name == 'complete':
                    complete = True
                else:
                    attachment_filename = str(f)
            attachment_index[candidate_id].append(
                (attachment_type, attachment_date, attachment_filename, complete)
            )
    return attachment_index


def candidate_attachment_exists(
        cache_dir: Path, candidate_id: str, attachment: dict[str,str]
    ) -> bool:
    # Duplication of code in download_candidate_attachment()
    entity_dir = cache_dir / Candidate.rest_name
    attachment_dir = (entity_dir 
        / mk_attachment_path(candidate_id + '-attachments', attachment))
    attachment_filename = attachment_dir / attachment['filename']
    complete_filename = attachment_dir / 'complete'
    return attachment_filename.exists() and complete_filename.exists()


def download_candidate_attachment(
        cache_dir: Path, candidate_id: str, attachment: dict[str,str]
    ) -> None:
    "Download and save attachment file to cache."
    entity_dir = cache_dir / Candidate.rest_name
    attachment_dir = (entity_dir 
        / mk_attachment_path(candidate_id + '-attachments', attachment))
    attachment_dir.mkdir(parents=True, exist_ok=True)
    attachment_filename = attachment_dir / attachment['filename']
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


def print_category(category_title: str, members: list[str]) -> None:
    member_limit = 25
    print(f'{category_title}: {len(members)}')
    if 0 < len(members) <= member_limit:
        for member_id in members:
            print('   ', member_id)


def check_references(cache_dir: Path) -> None:
    application_index = read_index(cache_dir, Application.rest_name)
    print(f'Applications: {len(application_index):5d}')
    candidate_index = read_index(cache_dir, Candidate.rest_name)
    print(f'Candidates:   {len(candidate_index):5d}')
    job_index = read_index(cache_dir, Job.rest_name)
    print(f'Jobs:         {len(job_index):5d}')
    offer_index = read_index(cache_dir, Offer.rest_name)
    print(f'Offers:       {len(offer_index):5d}')
    scorecard_index = read_index(cache_dir, Scorecard.rest_name)
    print(f'Scorecards:   {len(scorecard_index):5d}')
    attachment_index = mk_attachment_index(cache_dir)
    n_attachments = sum([
        len(candidate_attachments)
        for candidate_attachments in attachment_index.values()
    ])
    print(f'Attachments:  {n_attachments:5d}')

    application_candidates = set()
    application_jobs = set()
    all_applications = set(application_index.keys())
    for app_id, application_summary in application_index.items():
        # Application moniker is a portmanteau
        candidate_id, job_id = application_summary[1].split('/')
        if len(candidate_id) > 0:
            application_candidates.add(candidate_id)
        if len(job_id) > 0:
            application_jobs.add(job_id)
    all_candidates = set(candidate_index.keys())
    candidates_without_applications = sorted(
        all_candidates - application_candidates
    )
    missing_candidates_from_applications = sorted(
        application_candidates - all_candidates
    )
    scorecard_candidates = set()
    for scorecard_id, scorecard_summary in scorecard_index.items():
        candidate_id = scorecard_summary[1]
        if len(candidate_id) > 0:
            scorecard_candidates.add(candidate_id)
    candidates_without_scorecards = sorted(all_candidates - scorecard_candidates)
    missing_candidates_from_scorecards = sorted(scorecard_candidates - all_candidates)
    all_jobs = set(job_index.keys())
    jobs_without_applications = sorted(all_jobs - application_jobs)
    missing_jobs_from_applications = sorted(application_jobs - all_jobs)
    offer_candidates = set()
    offer_applications = set()
    for offer_id, offer_summary in offer_index.items():
        # Offer moniker is a portmanteau
        candidate_id, application_id = offer_summary[1].split('/')
        if len(candidate_id) > 0:
            offer_candidates.add(candidate_id)
        if len(application_id) > 0:
            offer_applications.add(application_id)
    missing_candidates_from_offers = sorted(offer_candidates - all_candidates)
    missing_applications_from_offers = sorted(offer_applications - all_applications)
    print_category('Candidates without applications', candidates_without_applications)
    print_category('Missing candidates mentioned in applications', missing_candidates_from_applications)
    print_category('Jobs without applications', jobs_without_applications)
    print_category('Missing jobs mentioned in applications', missing_jobs_from_applications)
    print_category('Candidates without scorecards', candidates_without_scorecards)
    print_category('Missing candidates mentioned in scorecards', missing_candidates_from_scorecards)
    print_category('Missing candidates mentioned in offers', missing_candidates_from_offers)
    print_category('Missing applications mentioned in offers', missing_applications_from_offers)


def process_retrieved_entities(entities):
    print(f"Total entities fetched: {len(entities)}")
    for i, entity in enumerate(entities):
        print(i, entity.data, entity.retrieval_timestamp)
        print('----------------------')
    save_entities(cache_dir, entities)


class Commands:
    """Greenhouse Data Extractor."""

    def __init__(self, after_date=None, before_date=None):
        """Assemble HTTP headers and request parameters.

          Args:
            after_date: Only get records created after this ISO-8601 date
            before_date: Only get records created before this ISO-8601 date
        """
        self.headers = mk_header(API_TOKEN)
        standard_params = {'per_page': 100}
        self.params = mk_params(standard_params, after_date, before_date)
        log.info('Starting', cache_dir=str(cache_dir), params=self.params)

    def applications(self):
        print("Fetching applications...")
        entities = get_applications_from_greenhouse(self.headers, self.params)
        process_retrieved_entities(entities)

    def attachments(self):
        print("Fetching attachments for retrieved candidates...")
        get_candidate_attachments(cache_dir)

    def candidates(self):
        print("Fetching candidates...")
        entities = get_candidates_from_greenhouse(self.headers, self.params)
        process_retrieved_entities(entities)

    def check(self):
        print("Checking references...")
        check_references(cache_dir)

    def jobs(self):
        print("Fetching jobs...")
        entities = get_jobs_from_greenhouse(self.headers, self.params)
        process_retrieved_entities(entities)

    def offers(self):
        print("Fetching offers...")
        entities = get_offers_from_greenhouse(self.headers, self.params)
        process_retrieved_entities(entities)

    def scorecards(self):
        print("Fetching scorecards...")
        entities = get_scorecards_from_greenhouse(self.headers, self.params)
        process_retrieved_entities(entities)


if __name__ == "__main__":
    fire.Fire(Commands)
