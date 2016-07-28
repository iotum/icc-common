import ConfigParser
import redis
import glob
import sys

this_module = sys.modules[__name__]

config = ConfigParser.ConfigParser()
config_files = \
    glob.glob("/etc/iccpmm/conf.d/*.conf") + \
    glob.glob("/etc/iccfhd/conf.d/*.conf")
config_files.sort()
config.read(config_files)

REDIS_SERVER = config.get('redis', 'host')
REDIS_PORT = config.getint('redis', 'port')

db = redis.StrictRedis(host=REDIS_SERVER, port=REDIS_PORT, db=0)


def db_get(name, key):
    return db.get('%s-%s' % (name, key))


def db_set(name, key, value):
    return db.set('%s-%s' % (name, key), value)


def db_delete(name, key):
    return db.delete('%s-%s' % (name, key))


def db_expire(name, key, expiry):
    return db.expire('%s-%s' % (name, key), expiry)


def db_rename(name, key, new_key):
    db.persist('%s-%s' % (name, key))
    return db.rename('%s-%s' % (name, key), '%s-%s' % (name, new_key))


def db_hget(name, key, hash_key):
    return db.hget('%s-%s' % (name, key), hash_key)


def db_hdel(name, key, hash_key):
    db.persist('%s-%s' % (name, key))
    return db.hdel('%s-%s' % (name, key), hash_key)


def db_hset(name, key, hash_key, value):
    db.persist('%s-%s' % (name, key))
    return db.hset('%s-%s' % (name, key), hash_key, value)


def db_hmset(name, key, values):
    db.persist('%s-%s' % (name, key))
    return db.hmset('%s-%s' % (name, key), values)


def db_hgetall(name, key):
    return db.hgetall('%s-%s' % (name, key))


def db_incr(name, key):
    db.persist('%s-%s' % (name, key))
    return db.incr('%s-%s' % (name, key))


def db_exists(name, key):
    return db.exists('%s-%s' % (name, key))


def db_persist(name, key):
    return db.persist('%s-%s' % (name, key))


def db_sadd(name, key, member):
    db.persist('%s-%s' % (name, key))
    return db.sadd('%s-%s' % (name, key), member)


def db_srem(name, key, member):
    db.persist('%s-%s' % (name, key))
    return db.srem('%s-%s' % (name, key), member)


def db_smembers(name, key):
    return db.smembers('%s-%s' % (name, key))


def db_zadd(key, score, member):
    return db.zadd(key, score, member)


def db_zrem(key, member):
    return db.zrem(key, member)


def db_zscore(key, member):
    return db.zscore(key, member)


def db_zrevrange(key, start, stop):
    return db.zrevrange(key, start, stop)


def db_zrevrangeall(key):
    return db.zrevrange(key, 0, -1)


def db_zincrby(key, increment, member):
    return db.zincrby(key, increment, member)


# TODO(spd/2015-02-26): Depricate the above and make this thing a proper class
class DBWrapper():
    def __getattr__(self, name):
        def handler(*args, **kwargs):
            return getattr(this_module, 'db_'+name)(*args, **kwargs)
        return handler
