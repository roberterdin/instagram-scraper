import json
import requests
import re
import logging as log
from abc import ABCMeta, abstractmethod
import time


class InstagramUser:

    def __init__(self, user_id, username=None, bio=None, followers_count=None, following_count=None, is_private=False):
        """
        A class to represent an Instagram User

        :param user_id: User ID of instagram user
        :param username: Username of Instagram user
        :param bio: Bio text for user
        :param followers_count: Number of followers
        :param following_count: Number of people following
        :param is_private: Boolean to indicate if account is private or not
        """
        self.id = user_id
        self.username = username
        self.bio = bio
        self.followers_count = followers_count
        self.following_count = following_count
        self.is_private = is_private


class InstagramPost:

    def __init__(self, post_id, code,
                 user=None,
                 caption="",
                 tags=[],
                 comments=None,
                 likes=None,
                 img_small=None,
                 img_large=None,
                 is_video=False,
                 created_at=None):
        """
        A class to represent a post on Instagram
        """
        self.post_id = post_id
        self.code = code
        self.caption = caption
        self.tags = tags
        self.comments = comments
        self.likes = likes
        self.user = user
        self.img_small = img_small
        self.img_large = img_large
        self.is_video = is_video
        self.created_at = created_at

    def processed_text(self):
        """
        Processes a caption to remove newlines in it.
        :return:
        """
        if self.caption is None:
            return ""
        else:
            text = re.sub('[\n\r]', ' ', self.caption)
            return text

    def hashtags(self):
        """
        Simple hashtag extractor to return the hastags in the post
        :return:
        """
        hashtags = []
        if self.caption is None:
            return hashtags
        else:
            for tag in re.findall("#[a-zA-Z0-9]+", self.caption):
                hashtags.append(tag)
            return hashtags


class HashTagSearch(metaclass=ABCMeta):

    def __init__(self, request_timeout=10, error_timeout=10, request_retries=3):
        """
        This class performs a search on Instagrams hashtag search engine, and extracts posts for that given hashtag.

        There are some limitations, as this does not extract all occurrences of the hash tag.

        Instead, it extracts the most recent uses of the tag.

        :param request_timeout: A timeout request
        :param error_timeout: A timeout we sleep for it we experience an error before our next retry
        :param request_retries: Number of retries on an error, before giving up
        """
        super().__init__()
        self.total_images = 0
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
            self.total_images += len(instagram_posts)
            log.info("Images crawled: {}".format(self.total_images))
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
        except Exception as e:
            log.error(e)

        return nodes, next_cursor

    def extract_instagram_posts(self, nodes):
        """
        For a given set of nodes from Instagrams JSON response, parse the nodes into Instagram Post objects
        :param nodes: Instagram JSON nodes
        :return: A list of Instagram objects
        """
        posts = []
        for node in nodes:
            user = self.extract_owner_details(node["owner"])

            # Extract post details
            text = None
            if "caption" in node:
                text = node["caption"]
            post = InstagramPost(node['id'], node['code'],
                                 user=user,
                                 caption=text,
                                 comments=node['comments']['count'],
                                 likes=node['likes']['count'],
                                 img_small=node["thumbnail_src"],
                                 img_large=node["display_src"],
                                 created_at=node["date"],
                                 is_video=node["is_video"])
            posts.append(post)
        return posts

    @staticmethod
    def extract_owner_details(owner):
        """
        Extracts the details of a user object.
        :param owner: Instagrams JSON user object
        :return: An Instagram User object
        """
        username = None
        if "username" in owner:
            username = owner["username"]
        is_private = False
        if "is_private" in owner:
            is_private = is_private
        user = InstagramUser(owner['id'], username=username, is_private=is_private)
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
        self.total_posts = 0

    def save_results(self, instagram_results):
        super().save_results(instagram_results)
        for i, post in enumerate(instagram_results):
            self.total_posts += 1
            # print("%i - %s" % (self.total_posts, post.processed_text()))


if __name__ == '__main__':
    start_time = time.time()
    log.basicConfig(level=log.INFO)
    crawler = HashTagSearchExample()
    try:
        crawler.extract_recent_tag("food")
    except Exception as e:
        log.info("Posts: {}".format(crawler.total_images))
        log.info("Elapsed time: {}".format(time.time() - start_time))
        log.info(str(e))
