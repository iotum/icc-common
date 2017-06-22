import ConfigParser
import glob
import redis

class DBWrapper(object):
    def __init__(self):
        config_files = \
            glob.glob("/etc/iccpmm/conf.d/*.conf") + \
            glob.glob("/etc/iccfhd/conf.d/*.conf")
        config_files.sort()

        config = ConfigParser.ConfigParser()
        config.read(config_files)

        server = config.get('redis', 'host')
        port = config.getint('redis', 'port')

        self._db = redis.StrictRedis(host=server, port=port, db=0)


    def get(self, name, key):
        return self._db.get('%s-%s' % (name, key))


    def set(self, name, key, value):
        return self._db.set('%s-%s' % (name, key), value)


    def delete(self, name, key):
        return self._db.delete('%s-%s' % (name, key))


    def expire(self, name, key, expiry):
        return self._db.expire('%s-%s' % (name, key), expiry)


    def rename(self, name, key, new_key):
        return self._db.rename('%s-%s' % (name, key), '%s-%s' % (name, new_key))


    def hget(self, name, key, hash_key):
        return self._db.hget('%s-%s' % (name, key), hash_key)


    def hdel(self, name, key, hash_key):
        return self._db.hdel('%s-%s' % (name, key), hash_key)


    def hset(self, name, key, hash_key, value):
        return self._db.hset('%s-%s' % (name, key), hash_key, value)


    def hmset(self, name, key, values):
        return self._db.hmset('%s-%s' % (name, key), values)


    def hgetall(self, name, key):
        return self._db.hgetall('%s-%s' % (name, key))


    def incr(self, name, key):
        return self._db.incr('%s-%s' % (name, key))


    def exists(self, name, key):
        return self._db.exists('%s-%s' % (name, key))


    def persist(self, name, key):
        return self._db.persist('%s-%s' % (name, key))


    def sadd(self, name, key, member):
        return self._db.sadd('%s-%s' % (name, key), member)


    def srem(self, name, key, member):
        return self._db.srem('%s-%s' % (name, key), member)


    def smembers(self, name, key):
        return self._db.smembers('%s-%s' % (name, key))


    def zadd(self, key, score, member):
        return self._db.zadd(key, score, member)


    def zrem(self, key, member):
        return self._db.zrem(key, member)


    def zscore(self, key, member):
        return self._db.zscore(key, member)


    def zrevrange(self, key, start, stop):
        return self._db.zrevrange(key, start, stop)


    def zrevrangeall(self, key):
        return self._db.zrevrange(key, 0, -1)


    def zincrby(self, key, increment, member):
        return self._db.zincrby(key, increment, member)


    def scan_iter(self, name, prefix=None):
        """
        Make an iterator using the SCAN command.

        ``key`` ICC key prefix
        """
        prefix_len = len(name) + 1
        if not prefix:
            prefix = ''
        for key in self._db.scan_iter('%s-%s*' % (name, prefix), count=1000):
            yield key[prefix_len:]
