import datetime
import hadoop
import json
import os


class DataLake(object):

    HDFS_TYPE_PATH_FMT = '/{zone}/{source}/{type}'
    HDFS_STORAGE_PATH_FMT = '{year}/{month:02d}/{day:02d}'

    def __init__(self, zone, source, type):
        self.zone = zone
        self.source = source
        self.hdfs_client = hadoop.Hdfs('http://hadoop:9870')
        self.storage_date = datetime.datetime.utcnow().date()
        self.hdfs_type_path = DataLake.HDFS_TYPE_PATH_FMT.format(zone=zone,
                                                                 source=source,
                                                                 type=type)
        self.hdfs_storage_path = os.path.join(self.hdfs_type_path,
                                              DataLake.HDFS_STORAGE_PATH_FMT.format(year=self.storage_date.year,
                                                                                    month=self.storage_date.month,
                                                                                    day=self.storage_date.day))

    def store_json(self, id, contents, **attrs):
        full_path = os.path.join(self.hdfs_storage_path, '{id}.json'.format(id=id))
        with self.hdfs_client.write(full_path, overwrite=True, encoding='utf-8') as hdfs_writer:
            json.dump(contents, hdfs_writer)

        self.hdfs_client.setXattrs(full_path, **attrs)

    def get_json(self, stem):
        full_path = stem
        # Under some circumstances (e.g. walk), the user might call with the full path
        if not full_path.startswith(self.hdfs_type_path):
            full_path = os.path.join(self.hdfs_type_path, full_path)

        with self.hdfs_client.read(full_path, encoding='utf-8') as reader:
            return json.load(reader)

    def list(self, stem=None, showAttrs=False):
        root = os.path.join(self.hdfs_type_path, stem) if stem else self.hdfs_type_path
        for path, dirs, files in self.hdfs_client.walk(root):
            for f in files:
                fpath = os.path.join(path, f)

                if showAttrs:
                    attrs = self.hdfs_client.getXattrs(fpath)
                    print('{path:<75} {attrs}'.format(path=fpath, attrs=str(attrs)))
                else:
                    print(fpath)

    def walk(self, stem=None):
        root = os.path.join(self.hdfs_type_path, stem) if stem else self.hdfs_type_path
        return self.hdfs_client.walk(root)
