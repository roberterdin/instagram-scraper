import json
import random
from json import JSONDecodeError
import bs4
import requests
import re
import logging as log
from abc import ABCMeta, abstractmethod
import pymongo
from pymongo import MongoClient
import sys
import time
import yaml


class HashTagSearch(metaclass=ABCMeta):
    instagram_root = "https://www.instagram.com"

    def __init__(self, ):
        """
        This class performs a search on Instagrams hashtag search engine, and extracts posts for that given hashtag.
        """
        super().__init__()
        self.duplicate_posts = 0
        self.new_posts = 0
        self.client = MongoClient('mongodb://{}:{}/'.format(_config['db']['hostname'], _config['db']['port']))
        self.db = self.client[_config['db']['database']]
        self.posts = self.db[_config['db']['collection']]
        self.posts.ensure_index("postId", unique=True)

    def extract_recent_tag(self, tag):
        """
        Extracts Instagram posts for a given hashtag
        :param tag: Hashtag to extract
        """

        url_string = "https://www.instagram.com/explore/tags/%s/" % tag
        response = bs4.BeautifulSoup(requests.get(url_string).text, "html.parser")
        potential_query_ids = self.get_query_id(response)
        shared_data = self.extract_shared_data(response)

        media = shared_data['entry_data']['TagPage'][0]['tag']['media']
        posts = []
        for node in media['nodes']:
            post = self.extract_recent_instagram_post(node)
            posts.append(post)
        self.save_results(posts)

        end_cursor = media['page_info']['end_cursor']

        # figure out valid queryId
        for potential_id in potential_query_ids:
            url = "https://www.instagram.com/graphql/query/?query_id=%s&tag_name=%s&first=12&after=%s" % (
                potential_id, tag, end_cursor)
            try:
                data = requests.get(url).json()
                if 'hashtag' not in data['data']:
                    # empty response, skip
                    continue
                query_id = potential_id
                success = True
                break
            except JSONDecodeError as de:
                # no valid JSON retured, most likely wrong query_id resulting in 'Oops, an error occurred.'
                pass
        if not success:
            log.error("Error extracting Query Id, exiting")
            sys.exit(1)

        while end_cursor is not None:
            url = "https://www.instagram.com/graphql/query/?query_id=%s&tag_name=%s&first=12&after=%s" % (
                query_id, tag, end_cursor)
            data = requests.get(url).json()
            if 'hashtag' not in data['data']:
                # empty response, skip
                continue
            end_cursor = data['data']['hashtag']['edge_hashtag_to_media']['page_info']['end_cursor']
            posts = self.extract_instagram_posts(data['data']['hashtag']['edge_hashtag_to_media']['edges'])
            self.save_results(posts)

    @staticmethod
    def extract_shared_data(doc):
        for script_tag in doc.find_all("script"):
            if script_tag.text.startswith("window._sharedData ="):
                shared_data = re.sub("^window\._sharedData = ", "", script_tag.text)
                shared_data = re.sub(";$", "", shared_data)
                shared_data = json.loads(shared_data)
                return shared_data

    def extract_owner_details(self, owner):
        user = dict()
        user['userId'] = owner['id']
        user['username'] = owner["username"] if 'username' in owner else None
        user['isPrivate'] = True if 'is_private' in owner else False
        return user

    def extract_recent_instagram_post(self, node):
        post = dict()
        post['user'] = self.extract_owner_details(node["owner"])
        post['postId'] = node['id']
        post['code'] = node['code']
        post['caption'] = node['caption'] if 'caption' in node else None
        if post['caption'] is not None:
            post['hashTags'] = [re.sub(r'\W+', '', word) for word in post['caption'].split() if
                                word.startswith("#")]
        else:
            post['hashTags'] = []
        post['comments'] = node['comments']['count']
        post['likes'] = node['likes']['count']
        post['imgSmall'] = node["thumbnail_src"]
        post['imgLarge'] = node["display_src"]
        post['postedAt'] = node["date"]
        post['isVideo'] = node["is_video"]

        return post

    def extract_instagram_posts(self, nodes):
        """
        For a given set of nodes from Instagrams JSON response, parse the nodes into Instagram Post objects
        :param nodes: Instagram JSON nodes
        :return: A list of Instagram objects
        """
        posts = []
        for node in nodes:
            try:
                post = dict()
                post['dimensions'] = dict()
                post['dimensions']['width'] = node['node']['dimensions']['width']
                post['dimensions']['height'] = node['node']['dimensions']['height']
                post['user'] = self.extract_owner_details(node['node']["owner"])
                post['postId'] = node['node']['id']
                post['code'] = node['node']['shortcode']
                post['caption'] = node['node']['edge_media_to_caption']['edges'][0]['node']['text'] if len(
                    node['node']['edge_media_to_caption']['edges']) > 0 else None
                if post['caption'] is not None:
                    post['hashTags'] = [re.sub(r'\W+', '', word) for word in post['caption'].split() if
                                        word.startswith("#")]
                else:
                    post['hashTags'] = []
                post['comments'] = node['node']['edge_media_to_comment']
                post['likes'] = node['node']['edge_liked_by']
                post['imgSmall'] = node['node']["thumbnail_src"]
                post['imgLarge'] = node['node']["display_url"]
                post['postedAt'] = node['node']["taken_at_timestamp"]
                post['isVideo'] = node['node']["is_video"]

                if not set(post['hashTags']).isdisjoint(set(_config['instagram']['excluded'])):
                    # contains blocked hashtag, skip
                    continue

                posts.append(post)
            except KeyError as e:
                log.error("Problems parsing post {}".format(str(e)))
        return posts

    def get_query_id(self, doc):
        query_ids = []
        for script in doc.find_all("script"):
            if script.has_attr("src") and "en_US_Commons" in script['src']:
                text = requests.get("%s%s" % (self.instagram_root, script['src'])).text
                for query_id in re.findall("(?<=queryId:\")[0-9]{17,17}", text):
                    query_ids.append(query_id)
        return query_ids

    @abstractmethod
    def save_results(self, instagram_results):
        """
        Implement yourself to work out what to do with each extract batch of posts
        :param instagram_results: A list of Instagram Posts
        """


class HashTagSearchExample(HashTagSearch):
    def __init__(self):
        super().__init__()
        self.total_posts = 0

    def save_results(self, instagram_results):
        super().save_results(instagram_results)
        new_posts = 0
        duplicate_posts = 0
        for i, post in enumerate(instagram_results):
            try:
                self.posts.insert_one(post)
                new_posts += 1
            except pymongo.errors.DuplicateKeyError as ex:
                duplicate_posts += 1
        self.new_posts += new_posts
        self.duplicate_posts += duplicate_posts
        log.info("New posts: {}".format(new_posts))
        log.info("Duplicate posts: {}".format(duplicate_posts))


if __name__ == '__main__':
    # import configuration
    with open("config.yaml", 'r') as stream:
        try:
            global _config
            _config = yaml.load(stream)
        except yaml.YAMLError as exc:
            print(exc)


    start_time = time.time()
    log.basicConfig(level=log.INFO)
    crawler = HashTagSearchExample()

    try:
        crawler.extract_recent_tag(random.choice(_config['instagram']['tags']))
    except Exception as e:
        log.info(str(e))
    log.info("------------------------------")
    log.info("Stored posts: {}".format(crawler.new_posts))
    log.info("Duplicate posts: {}".format(crawler.duplicate_posts))
    log.info("Elapsed time: {}".format(time.time() - start_time))
    log.info("------------------------------")
