import requests
import re
import logging as log
from abc import ABCMeta, abstractmethod
import time
import pymongo
from pymongo import MongoClient


class HashTagSearch(metaclass=ABCMeta):
    def __init__(self, request_timeout=10, error_timeout=10, request_retries=3):
        """
        This class performs a search on Instagrams hashtag search, and extracts posts for that given hashtag.

        There are some limitations, as this does not extract all occurrences of the hash tag.

        Instead, it extracts the most recent uses of the tag.

        :param request_timeout: A timeout request
        :param error_timeout: A timeout we sleep for it we experience an error before our next retry
        :param request_retries: Number of retries on an error, before giving up
        """
        super().__init__()
        self.request_timeout = request_timeout
        self.error_timeout = error_timeout
        self.request_retries = request_retries
        self.instagram_root = "https://www.instagram.com"

        # We need a CSRF token, so we query Instagram first
        self.csrf_token, self.cookie_string = self.get_csrf_and_cookie_string()
        log.info("CSRF Token set to %s", self.csrf_token)
        log.info("Cookie String set to %s" % self.cookie_string)

    def extract_recent_tag(self, tag):
        """
        Extracts Instagram posts for a given hashtag
        :param tag: Hashtag to extract
        """

        result = requests.get("https://www.instagram.com/explore/tags/%s/?__a=1" % tag).json()
        nodes = result["tag"]["media"]["nodes"]
        cursor = result['tag']['media']['page_info']['end_cursor']
        last_cursor = None
        while len(nodes) != 0 and cursor != last_cursor:
            instagram_posts = self.extract_instagram_posts(nodes)
            self.save_results(instagram_posts)
            last_cursor = cursor
            nodes, cursor = self.get_next_results(tag, cursor)

    def get_csrf_and_cookie_string(self):
        """
        This method connects to Instagram, and returns a list of headers we need in order to process further
        requests, including a CSRF Token
        :return: A header parameter list
        """
        resp = requests.head(self.instagram_root)

        cookie_string = "mid=%s; csrftoken=%s;" % (resp.cookies["mid"], resp.cookies['csrftoken'])
        return resp.cookies['csrftoken'], cookie_string

        # return resp.cookies['csrftoken'], resp.headers['set-cookie']

    def get_next_results(self, tag, cursor):
        """
        Gets the next batch of results in the cursor.
        :param tag: Hashtag to search
        :param cursor: Cursor pagination object
        :return: The next set of nodes and cursor
        """
        log.info("Getting %s with cursor %s" % (tag, cursor))
        nodes = []
        next_cursor = cursor
        post_data = self.get_query_param(tag, cursor)
        headers = self.get_headers("https://www.instagram.com/explore/tags/%s/" % tag)
        try:
            response = requests.post("https://www.instagram.com/query/", data=post_data, headers=headers).json()
            if "media" in response and "nodes" in response["media"]:
                nodes = response["media"]["nodes"]
                if "page_info" in response["media"]:
                    next_cursor = response["media"]["page_info"]["end_cursor"]
        except Exception as ex:
            log.error(ex)

        return nodes, next_cursor

    def extract_instagram_posts(self, nodes):
        """
        For a given set of nodes from Instagrams JSON response, parse the nodes into Instagram Post objects
        :param nodes: Instagram JSON nodes
        :return: A list of Instagram objects
        """
        posts = []
        for node in nodes:
            post = dict()
            post['user'] = self.extract_owner_details(node["owner"])
            post['postId'] = node['id']
            post['code'] = node['code']
            post['caption'] = node['caption'] if 'caption' in node else None
            if post['caption'] is not None:
                post['hashTags'] = [re.sub(r'\W+', '', word) for word in post['caption'].split() if word.startswith("#")]
            else:
                post['hashTags'] = []
            post['comments'] = node['comments']['count']
            post['likes'] = node['likes']['count']
            post['imgSmall'] = node["thumbnail_src"]
            post['imgLarge'] = node["display_src"]
            post['postedAt'] = node["date"]
            post['isVideo'] = node["is_video"]
            posts.append(post)
        return posts

    @staticmethod
    def extract_owner_details(owner):
        user = dict()
        user['userId'] = owner['id']
        user['username'] = owner["username"] if 'username' in owner else None
        user['isPrivate'] = True if 'is_private' in owner else False
        return user

    def get_headers(self, referrer):
        """
        Returns a bunch of headers we need to use when querying Instagram
        :param referrer: The page referrer URL
        :return: A dict of headers
        """
        return {
            "referer": referrer,
            "accept": "application/json, text/javascript, */*; q=0.01",
            "Accept-Language": "en-GB,en;q=0.8,en-US;q=0.6",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "cookie": self.cookie_string,
            "origin": "https://www.instagram.com",
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/49.0.2623.87 Safari/537.36",
            "x-csrftoken": self.csrf_token,
            "x-instagram-ajax": "1",
            "X-Requested-With": "XMLHttpRequest"
        }

    @staticmethod
    def get_query_param(tag, end_cursor):
        """
        Returns the query params required to load next page on Instagram.
        This can be modified to return less information.
        :param tag: Tag we're querying
        :param end_cursor: The end cursor Instagram specifies
        :return: A dict of request parameters
        """
        return {
            'q':
                "ig_hashtag(%s) { media.after(%s, 100) {" % (tag, end_cursor) +
                "  count," +
                "  nodes {" +
                "    caption," +
                "    code," +
                "    date," +
                "    dimensions {" +
                "      height," +
                "      width" +
                "    }," +
                "    display_src," +
                "    id," +
                "    is_video," +
                "    likes {" +
                "      count," +
                "      nodes {" +
                "        user {" +
                "          id," +
                "          username," +
                "          is_private" +
                "        }" +
                "      }" +
                "    }," +
                "    comments {" +
                "      count" +
                "    }," +
                "    owner {" +
                "      id," +
                "      username," +
                "      is_private" +
                "    }," +
                "    thumbnail_src" +
                "  }," +
                "  page_info" +
                "}" +
                " }",
            "ref": "tags::show"}

    @abstractmethod
    def save_results(self, instagram_results):
        """
        Implement yourself to work out what to do with each extract batch of posts
        :param instagram_results: A list of Instagram Posts
        """


class HashTagSearchExample(HashTagSearch):
    def __init__(self):
        super().__init__()
        self.duplicate_posts = 0
        self.new_posts = 0
        self.client = MongoClient('mongodb://localhost:27017/')
        self.db = self.client['instagram']
        self.posts = self.db['posts']
        self.posts.ensure_index("postId", unique=True)

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
    start_time = time.time()
    log.basicConfig(level=log.INFO)
    crawler = HashTagSearchExample()

    try:
        crawler.extract_recent_tag("food")
    except Exception as e:
        log.info(str(e))
    log.info("------------------------------")
    log.info("Stored posts: {}".format(crawler.new_posts))
    log.info("Duplicate posts: {}".format(crawler.duplicate_posts))
    log.info("Elapsed time: {}".format(time.time() - start_time))
    log.info("------------------------------")
