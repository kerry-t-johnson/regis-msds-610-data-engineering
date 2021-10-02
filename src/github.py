import logging
import math
import requests


class StopIterating(Exception):
    pass


class GitHub(object):
    '''
    A wrapper for the GitHub API RESTful interface.

    The pygithub module was not used because it seemed to make excessive calls
    to the GitHub API which resulted in constantly exceeding the rate limit
    for the API token.
    '''

    CONTENT_TYPE = 'application/vnd.github.v3+json'
    PAGE_SIZE = 100
    LANGUAGES = [
        'JavaScript',   'Rust',         'Python',   'C++',      'Java',
        'Dart',         'TypeScript',   'C',        'Go',       'CSS',
        'PHP',          'C#',           'Clojure',  'Assembly', 'Nunjucks',
        'Ruby',         'Dockerfile',   'HTML',     'Shell',    'Vue',
        'Kotlin',       'Swift',        'Julia',    'Markdown', 'Jupyter Notebook',
        'Objective-C',  'SCSS',         'TeX',      'Scala',    'Lua',
        'Makefile',     'Haskell',      'Less',     'V',        'Batchfile',
        'OCaml',        'Standard ML',  'Elixir',   'Crystal',  'CoffeeScript'
    ]

    def __init__(self, username, token):
        self.auth = requests.auth.HTTPBasicAuth(username, token)
        self.headers = {'Accept': GitHub.CONTENT_TYPE}
        self.url = 'https://api.github.com/{endpoint}'
        self.logger = logging.getLogger('GitHub')
        pass

    def log_limits(self):
        result = requests.get(self.url.format(endpoint='rate_limit'),
                              auth=self.auth,
                              headers=self.headers)
        result.raise_for_status()
        rate_limit = result.json()

        self.logger.debug('Remaining core: {core}, remaining search: {search}'.format(core=rate_limit['resources']['core']['remaining'],
                                                                                      search=rate_limit['resources']['search']['remaining']))

    def search_repositories(self, callback, query, order='desc', sort=None):
        try:
            self.logger.info('Searching GitHub repositories: {q}'.format(q=query))

            self._iterate_search(lambda results: GitHub._for_each_result_item(callback, results),
                                 'search/repositories',
                                 query=query,
                                 order=order,
                                 sort=sort)
        except StopIterating:
            pass

    @staticmethod
    def _for_each_result_item(callback, result):
        for r in result['items']:
            if callback(r) == False:
                raise StopIterating()

    def _iterate_search(self, callback, endpoint, query, order='desc', sort=None):
        try:
            params = {'q': query, 'order': order, 'per_page': 100}

            if sort:
                params['sort'] = sort

            for page in range(1, 11):
                params['page'] = page

                result = self._get_json(endpoint, params)
                total_count = result['total_count']
                max_pages = math.ceil(result['total_count'] / GitHub.PAGE_SIZE)

                self.logger.debug('Total count: {total}.  Page {page} of {max_pages}'.format(total=total_count,
                                                                                             page=page,
                                                                                             max_pages=max_pages))

                callback(result)

                if page >= max_pages:
                    return

            self.logger.warning('Unable to retrieve all items due to GitHub limits')
        except Exception as ex:
            self.log_limits()
            raise ex

    def _get_json(self, endpoint, params={}):
        result = requests.get(self.url.format(endpoint=endpoint),
                              auth=self.auth,
                              headers=self.headers,
                              params=params)
        result.raise_for_status()
        return result.json()
