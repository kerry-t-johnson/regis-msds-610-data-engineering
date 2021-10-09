import bs4
import logging
import requests
import re

SITE = 'https://stackoverflow.com'

#: StackOverflow stores a Post's tags as: <tag1><tag2>...<tagN>
#:
#: This regular expression captures the tag names and discards
#: the extraneous syntax
TAG_RE = re.compile('<(?P<tag>\S+?)>')


def extractTags(tag_string):
    ''' Returns a list of Stack Overflow Tags contained in the given string

        :param tag_string: a string containing <tag1><tag2>...<tagN>

        :return: A list of Stack Overflow Tags: [tag1, tag2, ..., tagN]
    '''
    try:
        return TAG_RE.findall(tag_string)
    except Exception as ex:
        # One of the input lines contains ",,,,,,," in the title
        # which is not correctly interpreted by the Spark CSV parser
        return []


class StopIterating(Exception):
    pass


class Scraper(object):

    PAGE_NUMBER_RE = re.compile(r'page=(?P<page_num>\d+)')

    def __init__(self):
        self.url = SITE + '/{endpoint}'
        self.logger = logging.getLogger('stackoverflow.Scraper')

    def search_tag(self, callback, tag):
        try:
            self.logger.info('Searching StackOverflow for questions with tag: {t}'.format(t=tag))

            self._iterate_search(lambda results: Scraper._with_results(callback, results),
                                 'questions/tagged/{t}'.format(t=tag),
                                 query={'tab': 'newest'})
        except StopIterating:
            pass

    @staticmethod
    def _with_results(callback, result):
        if callback(result) == False:
            raise StopIterating()

    def _iterate_search(self, callback, endpoint, query):
        if 'page' not in query:
            query['page'] = 1

        result = self._get_html(endpoint, params=query)

        callback(result)

        pager = result.find('div', {'class': 'pager'})
        next_page = pager.find('a', {'class': 'js-pagination-item', 'rel': 'next'})

        if next_page:
            last_page = next_page.find_previous_sibling('a', {'class': 'js-pagination-item'})
            m = Scraper.PAGE_NUMBER_RE.search(last_page.get('href'))
            last_page = m.group('page_num')

            m = Scraper.PAGE_NUMBER_RE.search(next_page.get('href'))
            next_page = m.group('page_num')
            self.logger.debug('Retrieving page {page} of {max_pages}'.format(page=next_page,
                                                                             max_pages=last_page))

            query['page'] = next_page
            self._iterate_search(callback, endpoint, query)

    def _get_html(self, endpoint, params={}):
        result = requests.get(self.url.format(endpoint=endpoint),
                              params=params)
        result.raise_for_status()
        return bs4.BeautifulSoup(result.content, 'html.parser')


class PageOfTaggedQuestions(object):

    QUESTION_ID_RE = re.compile(r'/questions/(?P<question_id>\d+)/')

    def __init__(self, content):
        self.content = bs4.BeautifulSoup(content, 'html.parser')
        self.logger = logging.getLogger('stackoverflow.PageOfTaggedQuestions')

    def iterate_questions(self, callback):
        for s in self.content.findAll('div', {'class': 'summary'}):
            question = {}
            question_anchor = s.find('a', {'class': 'question-hyperlink'})
            question['link'] = SITE + question_anchor.get('href').strip()
            question['title'] = question_anchor.text.strip()

            m = PageOfTaggedQuestions.QUESTION_ID_RE.search(question['link'])
            question['qid'] = m.group('question_id')

            tag_anchors = s.find('div', {'class': 'tags'}).findAll('a', {'rel': 'tag'})
            question['tags'] = [anchor.text.strip() for anchor in tag_anchors]

            if callback(question) == False:
                break
