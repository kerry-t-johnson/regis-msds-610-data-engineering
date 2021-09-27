import hdfs
import hdfs.client
import logging

# Reference: https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/WebHDFS.html


class Hdfs(hdfs.Client):
    '''
    A thin wrapper around hdfs.Client which adds XAttrs
    '''

    _getXattrs = hdfs.client._Request('GET')
    _setXattr = hdfs.client._Request('PUT')
    _removeXattr = hdfs.client._Request('PUT')

    def __init__(self, url):
        super().__init__(url)
        self.logger = logging.getLogger('hadoop.Hdfs')

    def getXattrs(self, path, *attrs):
        # curl -i "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=GETXATTRS
        #              &xattr.name=<XATTRNAME1>&xattr.name=<XATTRNAME2>
        #              &encoding=<ENCODING>"
        params = {'encoding': 'text'}

        if attrs:
            params['xattr.name'] = attrs

        xattrs = self._getXattrs(path, **params).json().get('XAttrs', [])

        results = {}
        for x in xattrs:
            results[x['name'].removeprefix('user.')] = x['value'].strip('"')

        return results

    def setXattrs(self, path, **kwargs):
        # curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=SETXATTR
        #                      &xattr.name=<XATTRNAME>&xattr.value=<XATTRVALUE>
        #                      &flag=<FLAG>"
        for key, value in kwargs.items():
            if not key.startswith('user.'):
                key = 'user.{attr}'.format(attr=key)

            params = {
                'xattr.name': key,
                'xattr.value': value,
                'flag': 'CREATE'
            }

            try:
                self._setXattr(path, **params)
            except hdfs.HdfsError as ex:
                if 'The REPLACE flag must be specified' in ex.message:
                    params['flag'] = 'REPLACE'
                    self._setXattr(path, **params)
                else:
                    raise ex

    def removeXattr(self, path, *attrs):
        # curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=REMOVEXATTR
        #                     &xattr.name=<XATTRNAME>"
        for attr in attrs:
            if not attr.startswith('user.'):
                attr = 'user.{attr}'.format(attr=attr)

            params = {
                'xattr.name': attr
            }

            try:
                self._removeXattr(path, **params)
            except hdfs.HdfsError as ex:
                if 'No matching attributes found for remove operation' in ex.message:
                    pass
                else:
                    raise ex

    def removeAllXattrs(self, path):
        xattrs = self.getXattrs(path)
        self.removeXattr(path, *xattrs.keys())
