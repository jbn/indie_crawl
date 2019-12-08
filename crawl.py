import logging
import requests
from urllib.parse import urlparse, urljoin
from urllib.robotparser import RobotFileParser
from lxml.html import fromstring as parse_doc
from datetime import datetime
import time
from hashlib import sha256
from elasticsearch import Elasticsearch, NotFoundError
from tempfile import NamedTemporaryFile
import shutil
from pathlib import Path


BLOB_DIR = Path("blobs")

LOGGER = logging.getLogger("crawl")

USER_AGENT = 'abreka-indie'

INDIE_CRAWL_INDEX = 'indie_crawl'

GUARDED_RESP_HEADERS = {'link'}

DEFAULT_REQ_HEADERS = {'User-Agent': USER_AGENT}

STALE_SECONDS = 60 * 24


INDIE_CRAWL_MAPPINGS = {
  "mappings": {
    "properties": {
        "id": {"type": "long"},
        
        "domain": {"type": "keyword"},
        "url": {"type": "keyword"},
        "status_code": {"type": "short"},
        
        "elapsed": {"type": "long"},
        "headers": {
            "type": "object",
            "properties": {
                "server": {"type": "keyword"},
                "connection": {"type": "keyword"},
                "content-type": {"type": "keyword"},
                "cache-control": {"type": "keyword"},
                "link": {"type": "text"},
                "access-control-allow-origin": {"type": "keyword"},
                "access-control-allow-headers": {"type": "keyword"},
                "date": {"type": "text"},
                "strict-transport-security": {"type": "keyword"},
                "x-no-cache": {"type": "keyword"},
                "x-cache": {"type": "keyword"},
            }
        },
        "content": { "type": "text"},
        "content_hash": {"type": "keyword"},
        "fetch_time": {"type": "date"},
    }
  }
}

def create_indie_crawl_index(force_fresh=False):
    if force_fresh:
        try:
            es.indices.delete(INDIE_CRAWL_INDEX)
        except NotFoundError:
            pass
        
    return es.indices.create(INDIE_CRAWL_INDEX, body=INDIE_CRAWL_MAPPINGS)



def normalize_resp_headers(d):
    """
    Normalize http response headers for elastic search keyword storage
    """
    d = {k.lower(): v if v in GUARDED_RESP_HEADERS else v.lower() 
         for k, v in d.items()}
    
    return d

            
            
def stream_blob_and_hash(resp):
    h = sha256()
    
    with NamedTemporaryFile('wb', delete=False) as fp:
        for chunk in resp.iter_content(chunk_size=8192): 
            if chunk: # filter out keep-alive new chunks
                fp.write(chunk)
                h.update(chunk)
    
    hexdigest = h.hexdigest()
    out_path = (BLOB_DIR / hexdigest[0] / hexdigest[1] / hexdigest[2])
    out_path.mkdir(parents=True, exist_ok=True)
    shutil.move(fp.name, out_path / hexdigest)
    return hexdigest


def fetch_page(url):
    fetch_time = int(time.time() * 1000)
    
    resp = requests.get(url, DEFAULT_REQ_HEADERS, stream=True)
    
    content, content_hash = '', ''
    
    if 'text' in resp.headers.get('content-type','').lower():
        content = resp.content.decode()
        content_hash = sha256(resp.content).hexdigest()
    else:
        content_hash = stream_blob_and_hash(resp)
        
    return {'status_code': resp.status_code,
            'headers': normalize_resp_headers(resp.headers),
            'elapsed': resp.elapsed.microseconds,
            'url': url,
            'content': content,
            'content_hash': content_hash,
            'domain': urlparse(url).netloc.lower(),
            'fetch_time': fetch_time}
    
    return resp


def put_page_for_archive(es, doc):
    return es.index(INDIE_CRAWL_INDEX, id=doc['url'], body=doc)


def get_archived_page(es, url):
    try:
        return es.get(index=INDIE_CRAWL_INDEX, id=url)
    except NotFoundError:
        return None


def _does_need_fetch(es, url, stale_seconds=STALE_SECONDS):
    try:
        doc = es.get(index=INDIE_CRAWL_INDEX, id=url)
        fetch_time = datetime.fromtimestamp(doc['_source']['fetch_time'] / 1000.)
        return doc, (datetime.now() - fetch_time).total_seconds() > STALE_SECONDS
    except NotFoundError:
        return None, True
    
    
def refresh_archive(es, target_url):
    doc, is_stale = _does_need_fetch(es, target_url)
    if is_stale:
        page = fetch_page(target_url)
        put_page_for_archive(es, page)
        return page, True
    else:
        return doc['_source'], False


def make_robots_allow_all():
    robots = RobotFileParser()
    robots.parse(["crawl-delay: 1"])
    return robots


def get_robots(es, domain):
    # Someone mentioned at Camp SF that they supported both http and https
    # for better coverage of older clients. This keeps that in mind.
    for protocol in ['https', 'http']:
        try:
            url = f"{protocol}://{domain}/robots.txt"
            doc = refresh_archive(es, url)[0]
            
            if doc['status_code'] == 200:
                lines = doc['content'].splitlines()
                robots = RobotFileParser(url=url)
                robots.parse(lines)
                return robots
            else:
                raise ValueError(f"Status code {doc['status_code']}")
        except KeyboardInterrupt:
            raise
        except Exception as e:
            LOGGER.warning(f"Unable to fetch {url}: {e}")
            
    return make_robots_allow_all()
            
            
def link_iter(page_url, doc):
    seen, domain = set(), urlparse(page_url).netloc.lower()
    
    for a in doc.cssselect('a'):
        if not 'href' in a.attrib:
            continue
        link = {k: v for k, v in a.attrib.items()}
        link['text'] = a.text_content()
        url = urljoin(page_url, link['href'])
        link['url'] = url
        link['same_domain'] = urlparse(link['url']).netloc.lower() == domain
        
        if url not in seen:
            yield link
            seen.add(url)
        
    return domain


def is_text(doc):
    return 'html' in doc.get('headers', {}).get('content-type', "text/html").lower()


def is_html(doc):
    return 'html' in doc.get('headers', {}).get('content-type', "text/html").lower()


def crawl_domain(es, domain):
    robots = get_robots(es, domain)
    crawl_delay = robots.crawl_delay('*') or 1
    seed = f"https://{domain}/" # TODO: http/https check
    queue, seen = [seed], set()  # TODO: Real Queue
    
    while queue:
        url = queue.pop()
        
        try:
            if robots.can_fetch(USER_AGENT, url):
                LOGGER.info(f"Fetching {url} [in_queue={len(queue)}]")
                doc, did_hit = refresh_archive(es, url)
                seen.add(url)

                if is_html(doc):
                    for link in link_iter(url, parse_doc(doc['content'])):
                        if link['same_domain']:
                            if link['url'] not in seen:
                                queue.append(link['url'])

                if did_hit:
                    time.sleep(crawl_delay)
        except KeyboardInterrupt:
            raise
        except Exception as e:
            LOGGER.error(f"Error crawling {url}: {e}")
