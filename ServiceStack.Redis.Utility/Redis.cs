using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NLogUtility;
using ServiceStack.Text;

namespace ServiceStack.Redis.Utility
{
    public class Redis
    {

        #region hash
        /// <summary>
        /// hashid key value的递增
        /// </summary>
        /// <param name="hashId"></param>
        /// <param name="key"></param>
        /// <param name="incrementBy"></param>
        /// <param name="connectStr"></param>
        public static long IncreameValue(string hashId, string key, int incrementBy,string connectStr=null)
        {
            long res = 0;
            try
            {
                using (var redisClient = RedisClientManager.GetClient(connectStr))
                {
                    res = redisClient.IncrementValueInHash(hashId, key, incrementBy);
                }
            }
            catch (Exception ex)
            {
                Logger.Error(ex, "hashId:{0},key:{1},incrementBy:{2},connectStr:{3}", hashId, key, incrementBy,connectStr);
            }

            return res;
        }

        /// <summary>
        /// 通过hasId删除缓存
        /// </summary>
        /// <param name="hashId"></param>
        /// <param name="connectStr"></param>
        /// <returns></returns>
        public static bool RemoveHashRedisValueByHashId(string hashId,string connectStr=null)
        {
            bool result = true;
            try
            {
                using (var redisClient = RedisClientManager.GetClient(connectStr))
                {
                    List<string> keyList = redisClient.GetHashKeys(hashId);
                    foreach (string key in keyList)
                    {
                        var removed = redisClient.RemoveEntryFromHash(hashId, key);
                        result = result && removed;
                    }
                }
            }
            catch (Exception ex)
            {
                Logger.Error(ex, "hashId:{0},connectStr:{1}", hashId, connectStr);
                result = false;
            }

            return result;
        }

        /// <summary>
        /// 通过hashId和key删除缓存
        /// </summary>
        /// <param name="hash"></param>
        /// <param name="key"></param>
        /// <param name="connectStr"></param>
        /// <returns></returns>
        public static bool RemoveHasRedisValueByHasdIdAndKey(string hash, string key, string connectStr = null)
        {
            bool result = false;
            try
            {
                using (var redisClient = RedisClientManager.GetClient(connectStr))
                {
                    if (redisClient.HashContainsEntry(hash, key))
                    {
                        result = redisClient.RemoveEntryFromHash(hash, key);
                    }
                    else
                    {
                        result = true;
                    }
                }
            }
            catch (Exception ex)
            {
                Logger.Error(ex, "hashId:{0},key:{1},connectStr:{2}", hash, key, connectStr);
            }

            return result;
        }

        /// <summary>
        /// 设置HashRedis
        /// </summary>
        /// <param name="hash"></param>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <param name="connectStr"></param>
        /// <returns></returns>
        public static bool SetHashRedisValue(string hash, string key, string value, string connectStr = null)
        {
            bool result = false;
            try
            {
                using (var redisClient = RedisClientManager.GetClient(connectStr))
                {
                    result = redisClient.SetEntryInHash(hash, key, value);
                }
            }
            catch (Exception ex)
            {
                Logger.Error(ex, "hashId:{0},key:{1},value:{2},connectStr:{3}", hash, key, value, connectStr);
            }

            return result;
        }

        /// <summary>
        /// 获取HashId下的所有Keys
        /// </summary>
        /// <param name="hash"></param>
        /// <param name="connectStr"></param>
        /// <returns></returns>
        public static List<string> GetHashKeys(string hash, string connectStr = null)
        {
            List<string> value = null;
            try
            {
                using (var redisClient = RedisClientManager.GetClient(connectStr))
                {
                    value = redisClient.GetHashKeys(hash);
                }
            }
            catch (Exception ex)
            {
                Logger.Error(ex, "hashId:{0},connectStr:{1}", hash, connectStr);
            }

            return value ?? new List<string>();
        }

        /// <summary>
        /// 获取Hashredis
        /// </summary>
        /// <param name="hash"></param>
        /// <param name="key"></param>
        /// <param name="connectStr"></param>
        /// <returns></returns>
        public static string GetHashRedisValue(string hash, string key, string connectStr = null)
        {
            string value = null;
            try
            {
                using (var redisClient = RedisClientManager.GetClient(connectStr))
                {
                    value = redisClient.GetValueFromHash(hash, key);
                }
            }
            catch (Exception ex)
            {
                Logger.Error(ex, "hashId:{0},key:{1},connectStr:{2}", hash, key, connectStr);
            }
            return value;
        }

        /// <summary>
        /// 判断hashid是否存在
        /// </summary>
        /// <param name="hash"></param>
        /// <param name="key"></param>
        /// <param name="connectStr"></param>
        /// <returns></returns>
        public static bool HashContainsEntry(string hash, string key, string connectStr = null)
        {
            bool exist = false;
            try
            {
                using (var redisClient = RedisClientManager.GetClient(connectStr))
                {
                    exist = redisClient.HashContainsEntry(hash, key);
                }
            }
            catch (Exception ex)
            {
                Logger.Error(ex, "hashId:{0},key:{1},connectStr:{2}", hash, key, connectStr);
            }
            return exist;
        }

        #endregion


        /// <summary>
        /// 通过keytype删除缓存
        /// </summary>
        /// <param name="keyType"></param>
        /// <param name="connectStr"></param>
        /// <returns></returns>
        public static bool RemoveRedisValueByKeyType(string keyType, string connectStr = null)
        {
            bool result = true;
            try
            {
                using (var redisClient = RedisClientManager.GetClient(connectStr))
                {
                    List<string> keyList = redisClient.SearchKeys(keyType + "*");
                    foreach (string key in keyList)
                    {
                        if (key.StartsWith(keyType, StringComparison.OrdinalIgnoreCase))
                        {
                            var removed = redisClient.Remove(key);
                            result = result && removed;
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                result = false;
                Logger.Error(ex, "keyType:{0},connectStr:{1}", keyType, connectStr);
            }
            return result;
        }

        /// <summary>
        /// 通过key，删除redis
        /// </summary>
        /// <param name="key"></param>
        /// <param name="connectStr"></param>
        /// <returns></returns>
        public static bool RemoveRedisValueByKey(string key, string connectStr = null)
        {
            bool result = false;
            try
            {
                using (var redisClient = RedisClientManager.GetClient(connectStr))
                {
                    result = redisClient.Remove(key);
                }
            }
            catch (Exception ex)
            {
                Logger.Error(ex, "key:{0},connectStr:{1}", key, connectStr);
            }
            return result;
        }

        /// <summary>
        /// 获取redis
        /// </summary>
        /// <param name="key"></param>
        /// <param name="connectStr"></param>
        /// <returns></returns>
        public static string GetRedisValue(string key, string connectStr = null)
        {
            string value = null;
            try
            {
                using (var redisClient = RedisClientManager.GetClient(connectStr))
                {
                    value = redisClient.GetValue(key);
                }
            }
            catch (Exception ex)
            {
                Logger.Error(ex, "key:{0},connectStr:{1}", key, connectStr);
            }
            return value;
        }
        
        /// <summary>
        /// 获取redis
        /// </summary>
        /// <param name="key"></param>
        /// <param name="connectStr"></param>
        /// <returns></returns>
        public static object GetRedisObjectValue(string key, string connectStr = null)
        {
            object value = null;
            try
            {
                using (var redisClient = RedisClientManager.GetClient(connectStr))
                {
                    value = redisClient.Get<object>(key);
                }
            }
            catch (Exception ex)
            {
                Logger.Error(ex, "key:{0},connectStr:{1}", key, connectStr);
            }
            return value;
        }

        /// <summary>
        /// 获取redis
        /// </summary>
        /// <param name="key"></param>
        /// <param name="connectStr"></param>
        /// <returns></returns>
        public static T GetRedisValue<T>(string key, string connectStr = null)
        {
            T value = default(T);
            try
            {
                using (var redisClient = RedisClientManager.GetClient(connectStr))
                {
                    value = redisClient.Get<T>(key);
                }
            }
            catch (Exception ex)
            {
                Logger.Error(ex, "key:{0},connectStr:{1}", key, connectStr);
            }
            return value;
        }

        /// <summary>
        /// 过期时间
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <param name="expiresAt"></param>
        /// <param name="connectStr"></param>
        /// <returns></returns>
        public static bool SetRedisValue<T>(string key, T value, DateTime expiresAt, string connectStr = null)
        {
            try
            {
                using (var redisClient = RedisClientManager.GetClient(connectStr))
                {
                    return redisClient.Set(key, value, expiresAt);
                }
            }
            catch (Exception ex)
            {
                Logger.Error(ex, "key:{0},value:{1},connectStr:{2}", key, JsonSerializer.SerializeToString(value), connectStr);
                return false;
            }
        }

        /// <summary>
        /// 设置缓存
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <param name="expiresAt"></param>
        /// <param name="connectStr"></param>
        /// <returns></returns>
        public static bool SetRedisValue<T>(string key, T value, TimeSpan expiresAt, string connectStr = null)
        {
            try
            {
                using (var redisClient = RedisClientManager.GetClient(connectStr))
                {
                    return redisClient.Set(key, value, expiresAt);
                }
            }
            catch (Exception ex)
            {
                Logger.Error(ex, "key:{0},value:{1},connectStr:{2}", key, JsonSerializer.SerializeToString(value), connectStr);
                return false;
            }
        }

        /// <summary>
        /// 设置无过期时间缓存
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <param name="connectStr"></param>
        /// <returns></returns>
        public static bool SetRedisValue<T>(string key, T value, string connectStr = null)
        {
            try
            {
                using (var redisClient = RedisClientManager.GetClient(connectStr))
                {
                    return redisClient.Set(key, value);
                }
            }
            catch (Exception ex)
            {
                Logger.Error(ex, "key:{0},value:{1},connectStr:{2}", key, JsonSerializer.SerializeToString(value), connectStr);
                return false;
            }
        }


        #region list

        /// <summary>
        /// 入列
        /// </summary>
        /// <param name="listId"></param>
        /// <param name="value"></param>
        /// <param name="connectStr"></param>
        /// <returns></returns>
        public static bool Enqueue(string listId, string value, string connectStr = null)
        {
            bool result = false;
            try
            {
                using (var redisClient = RedisClientManager.GetClient(connectStr))
                {
                    redisClient.EnqueueItemOnList(listId, value);
                    result = true;
                }
            }
            catch (Exception ex)
            {
                Logger.Error(ex, "listId:{0},value:{1},connectStr:{2}", listId, value, connectStr);
            }

            return result;
        }

        /// <summary>
        /// 入列
        /// </summary>
        /// <param name="listId"></param>
        /// <param name="value"></param>
        /// <param name="connectStr"></param>
        /// <returns></returns>
        public static bool Enqueue<T>(string listId, T value, string connectStr = null)
        {
            bool result = false;
            try
            {
                using (var redisClient = RedisClientManager.GetClient(connectStr))
                {
                    redisClient.EnqueueItemOnList(listId, JsonSerializer.SerializeToString(value));
                    result = true;
                }
            }
            catch (Exception ex)
            {
                Logger.Error(ex, "listId:{0},value:{1},connectStr:{2}", listId, value, connectStr);
            }

            return result;
        }

        /// <summary>
        /// 出列
        /// </summary>
        /// <param name="listId"></param>
        /// <param name="connectStr"></param>
        /// <returns></returns>
        public static T Dequeue<T>(string listId, string connectStr = null)
        {
            T value = default(T);
            try
            {
                using (var redisClient = RedisClientManager.GetClient(connectStr))
                {
                    var item = redisClient.DequeueItemFromList(listId);
                    if (item != null)
                    {
                        value = JsonSerializer.DeserializeFromString<T>(item);
                    }
                }
            }
            catch (Exception ex)
            {
                Logger.Error(ex, "listId:{0},connectStr:{1}", listId, connectStr);
            }
            return value;
        }
        
        /// <summary>
        /// 队列数量
        /// </summary>
        /// <param name="listId"></param>
        /// <param name="connectStr"></param>
        /// <returns></returns>
        public static long QueueCount(string listId, string connectStr = null)
        {
            try
            {
                using (var redisClient = RedisClientManager.GetClient(connectStr))
                {
                    return redisClient.GetListCount(listId);
                }
            }
            catch (Exception ex)
            {
                Logger.Error(ex, "listId:{0},connectStr:{1}", listId, connectStr);
                return 0;
            }
        }

        #endregion
    }
}
