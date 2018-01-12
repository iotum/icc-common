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


    def __getattr__(self, attr):
        """ Passes all other Redis commands not defined in DBWrapper to StrictRedis. """
        return getattr(self._db, attr)


    def get(self, name, key):
        """ Get the value of key.
        If the key does not exist the special value nil is returned.
        An error is returned if the value stored at key is not a string, because GET only handles string values.
        """
        return self._db.get('%s-%s' % (name, key))


    def set(self, name, key, value):
        """ Set key to hold the string value.
        If key already holds a value, it is overwritten, regardless of its type.
        Any previous time to live associated with the key is discarded on successful SET operation.
        """
        return self._db.set('%s-%s' % (name, key), value)


    def del(self, name, key):
        """ Removes the specified keys. A key is ignored if it does not exist. """
        return self._db.del('%s-%s' % (name, key))


    def delete(self, name, key):
        """ Alias to del(). """
        return self.del(name, key)


    def expire(self, name, key, expiry):
        """ Set a timeout on key.
        After the timeout has expired, the key will automatically be deleted.
        A key with an associated timeout is often said to be volatile in Redis terminology.

        :reference: https://redis.io/commands/expire
        """
        return self._db.expire('%s-%s' % (name, key), expiry)


    def rename(self, name, key, new_key):
        """ Renames key to newkey.
        It returns an error when key does not exist.
        If newkey already exists it is overwritten, when this happens RENAME executes an implicit DEL operation,
        so if the deleted key contains a very big value it may cause high latency even if RENAME itself is usually a constant-time operation.
        """
        return self._db.rename('%s-%s' % (name, key), '%s-%s' % (name, new_key))


    def hget(self, name, key, hash_key):
        """ Returns the value associated with field in the hash stored at key. """
        return self._db.hget('%s-%s' % (name, key), hash_key)


    def hdel(self, name, key, hash_key):
        """ Removes the specified fields from the hash stored at key.
        Specified fields that do not exist within this hash are ignored.
        If key does not exist, it is treated as an empty hash and this command returns 0.
        """
        return self._db.hdel('%s-%s' % (name, key), hash_key)


    def hset(self, name, key, hash_key, value):
        """ Sets field in the hash stored at key to value.
        If key does not exist, a new key holding a hash is created.
        If field already exists in the hash, it is overwritten.
        """
        return self._db.hset('%s-%s' % (name, key), hash_key, value)


    def hmset(self, name, key, values):
        """ Sets the specified fields to their respective values in the hash stored at key.
        This command overwrites any specified fields already existing in the hash.
        If key does not exist, a new key holding a hash is created.
        """
        return self._db.hmset('%s-%s' % (name, key), values)


    def hgetall(self, name, key):
        """ Returns all fields and values of the hash stored at key.
        In the returned value, every field name is followed by its value, so the length of the reply is twice the size of the hash.
        """
        return self._db.hgetall('%s-%s' % (name, key))


    def hexists(self, name, key, hash_key):
        """ Returns if field is an existing field in the hash stored at key. """
        return self._db.hexists('%s-%s' % (name, key), hash_key)


    def hlen(self, name, key):
        """ Returns the number of fields contained in the hash stored at key. """
        return self._db.hlen('%s-%s' % (name, key))


    def incr(self, name, key):
        """ Increments the number stored at key by one.
        If the key does not exist, it is set to 0 before performing the operation.

        An error is returned if the key contains a value of the wrong type or contains a string that can not be represented as integer.

        This operation is limited to 64 bit signed integers.
        """
        return self._db.incr('%s-%s' % (name, key))


    def exists(self, name, key):
        """ Returns if key exists. """
        return self._db.exists('%s-%s' % (name, key))


    def persist(self, name, key):
        """ Remove the existing timeout on key, turning the key
        from volatile (a key with an expire set)
        to persistent (a key that will never expire as no timeout is associated).
        """
        return self._db.persist('%s-%s' % (name, key))


    def sadd(self, name, key, member):
        """ Add the specified members to the set stored at key.
        Specified members that are already a member of this set are ignored.
        If key does not exist, a new set is created before adding the specified members.

        An error is returned when the value stored at key is not a set.
        """
        return self._db.sadd('%s-%s' % (name, key), member)


    def srem(self, name, key, member):
        """ Remove the specified members from the set stored at key.
        Specified members that are not a member of this set are ignored.
        If key does not exist, it is treated as an empty set and this command returns 0.

        An error is returned when the value stored at key is not a set.
        """
        return self._db.srem('%s-%s' % (name, key), member)


    def smembers(self, name, key):
        """ Returns all the members of the set value stored at key. """
        return self._db.smembers('%s-%s' % (name, key))


    def sismember(self, name, key, member):
        """ Returns if member is a member of the set stored at key. """
        return self._db.sismember('%s-%s' % (name, key), member)


    def scard(self, name, key):
        """ Returns the set cardinality (number of elements) of the set stored at key. """
        return self._db.scard('%s-%s' % (name, key))


    def zadd(self, key, score, member):
        """ Adds all the specified members with the specified scores to the sorted set stored at key.
        It is possible to specify multiple score / member pairs.
        If a specified member is already a member of the sorted set, the score is updated and the element reinserted at the right position to ensure the correct ordering.

        If key does not exist, a new sorted set with the specified members as sole members is created, like if the sorted set was empty.
        If the key exists but does not hold a sorted set, an error is returned.

        The score values should be the string representation of a double precision floating point number.
        +inf and -inf values are valid values as well.
        """
        return self._db.zadd(key, score, member)


    def zrem(self, key, member):
        """ Removes the specified members from the sorted set stored at key.
        Non existing members are ignored.

        An error is returned when key exists and does not hold a sorted set.
        """
        return self._db.zrem(key, member)


    def zscore(self, key, member):
        """ Returns the score of member in the sorted set at key.

        If member does not exist in the sorted set, or key does not exist, nil is returned.
        """
        return self._db.zscore(key, member)


    def zrevrange(self, key, start, stop):
        """ Returns the specified range of elements in the sorted set stored at key.
        The elements are considered to be ordered from the highest to the lowest score.
        Descending lexicographical order is used for elements with equal score.

        Apart from the reversed ordering, ZREVRANGE is similar to ZRANGE.
        """
        return self._db.zrevrange(key, start, stop)


    def zrevrangeall(self, key):
        """ Returns all elements in the sorted set stored at key.
        The elements are considered to be ordered from the highest to the lowest score.
        Descending lexicographical order is used for elements with equal score.
        """
        return self._db.zrevrange(key, 0, -1)


    def zincrby(self, key, increment, member):
        """ Increments the score of member in the sorted set stored at key by increment.
        If member does not exist in the sorted set, it is added with increment as its score (as if its previous score was 0.0).
        If key does not exist, a new sorted set with the specified member as its sole member is created.

        An error is returned when key exists but does not hold a sorted set.

        The score value should be the string representation of a numeric value, and accepts double precision floating point numbers.
        It is possible to provide a negative value to decrement the score.
        """
        return self._db.zincrby(key, increment, member)


    def scan_iter(self, name, prefix=None):
        """ Iterates the set of keys in the currently selected Redis database.

        ``prefix`` ICC key prefix
        """
        prefix_len = len(name) + 1
        if not prefix:
            prefix = ''
        for key in self._db.scan_iter('%s-%s*' % (name, prefix), count=1000):
            yield key[prefix_len:]
