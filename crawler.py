#!/usr/bin/env python 
import crawl
import logging
from elasticsearch import Elasticsearch


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    logging.getLogger("crawl").setLevel(logging.INFO)
    logging.getLogger("elasticsearch").setLevel(logging.ERROR)
    
    es = Elasticsearch()
    crawl.crawl_domain(es, "aaronparecki.com")