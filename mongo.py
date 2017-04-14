# coding=utf-8
__author__ = 'luojianbo'

import pymongo
import traceback
import time
import sys


class MongDb(object):
    ASCENDING = pymongo.ASCENDING
    DESCENDING = pymongo.DESCENDING

    def __init__(self, dbhost, dbport, dbname, dbuser, dbpass):
        # connect db
        try:
            self.conn = pymongo.MongoClient(dbhost, dbport)
            self.db = self.conn[dbname]  # connect db
            if dbuser and dbpass:
                self.connected = self.db.authenticate(dbuser, dbpass)
            else:
                self.connected = True
        except Exception:
            print dbhost, dbport, dbname, dbuser, dbpass
            print traceback.format_exc()
            print 'Connect Statics Database Fail.'
            sys.exit(1)

    def __del__(self):
        # disconnect db
        self.conn.close()
        pass

    def check_connected(self):
        if not self.connected:
            raise NameError, 'stat:connected Error'

    def save(self, table, value):
        try:
            self.check_connected()
            self.db[table].save(value)
            self.db[table].find_and_modify()
        except Exception:
            print traceback.format_exc()

    def insert(self, table, value):
        try:
            self.check_connected()
            self.db[table].insert(value)
        except Exception:
            print traceback.format_exc()

    def update(self, table, conditions, value, s_upsert=False, s_multi=False):
        try:
            self.check_connected()
            self.db[table].update(conditions, value, upsert=s_upsert, multi=s_multi)
        except Exception:
            print traceback.format_exc()

    def upsert(self, table, data):
        try:
            self.check_connected()
            query = {'_id': data['_id']}
            if not self.db[table].find_one(query):
                self.db[table].insert(data)
            else:
                data.pop('_id')
                self.db[table].update(query, {'$set': data})
        except Exception:
            print traceback.format_exc()

    def traverse(self, table):
        try:
            self.check_connected()
            for item in self.db[table].find():
                yield item
        except Exception:
            print traceback.format_exc()

    def selectField(self, table, where, filedArr):
        try:
            self.check_connected()
            return self.db[table].find(where, filedArr)
        except Exception:
            print traceback.format_exc()

    def select(self, table, value):
        try:
            self.check_connected()
            return self.db[table].find(value)
        except Exception:
            print traceback.format_exc()

    def select_colum(self, table, value, colum):
        try:
            self.check_connected()
            return self.db[table].find(value, {colum: 1})
        except Exception:
            print traceback.format_exc()

    def select_count(self, table, value):
        try:
            self.check_connected()
            return self.db[table].find(value).count()
        except Exception:
            print traceback.format_exc()

    def select_one(self, table, value):
        try:
            self.check_connected()
            result = self.db[table].find(value)  # fix-me to findOne function  find_one
            for item in result:
                return item
            return result
        except Exception:
            print traceback.format_exc()

    def find_one(self, table, query):
        try:
            self.check_connected()
            return self.db[table].find_one(query)
        except Exception:
            print traceback.format_exc()

    # sort => [(sort_colunm, pymongo.DESCENDING/pymongo.ASCENDING)]
    def select_sort(self, table, value, sort):
        try:
            self.check_connected()
            return self.db[table].find(value).sort(sort)
        except Exception:
            print traceback.format_exc()

    def delete(self, table, value):
        try:
            self.check_connected()
            return self.db[table].remove(value)
        except Exception:
            print traceback.format_exc()

    # index => [(index_colunm, pymongo.DESCENDING/pymongo.ASCENDING)]
    def create_index(self, table, index):
        try:
            self.check_connected()
            self.db[table].ensure_index(index)
        except Exception:
            print traceback.format_exc()

    def close_all_databases(self):
        try:
            self.check_connected()
            admin = self.conn['admin']
            auth = admin.authenticate('admin', 'liveadmin')
            if auth:
                return admin.command({'closeAllDatabases': 1})
        except Exception:
            print traceback.format_exc()

    def insert_info_batch(self, table, lst, is_order=True, insert=True):
        update_statistic = {'update_yes': 0, 'update_no': 0}
        if lst != None and len(lst) == 0:
            return update_statistic
        dbtemp = self.db[table]
        bulk = dbtemp.initialize_ordered_bulk_op() if is_order else dbtemp.initialize_unordered_bulk_op()
        update_num = 0
        for item in lst:
            item['_utime'] = time.strftime("%Y-%m-%d %X", time.localtime())
            try:
                if insert:
                    bulk.insert(item)
                    update_num += 1
                else:
                    _id = item.pop('_id')
                    bulk.find({'_id': _id}).update({'$set': item})
                    update_num += 1
            except Exception:
                update_statistic['update_no'] += 1
                print traceback.format_exc()
                pass
        try:
            bulk.execute({'w': 0})
            update_statistic['update_yes'] += update_num
            print 'insert_logs:' + str(len(lst))
        except BulkWriteError:
            update_statistic['update_no'] += update_num
            print traceback.format_exc()
            pass
        return update_statistic


if __name__ == '__main__':
    db = MongDb('localhost', 27017, 'crawl_data', None, None)
