using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;

namespace ServiceStack.Redis.Utility
{
    public class RedisClientManager
    {
        private static PooledRedisClientManager _defaultManager;
        private static readonly Dictionary<string, PooledRedisClientManager> ManagerDictionary;

        static RedisClientManager()
        {
            _defaultManager = CreateDefaultManager();
            ManagerDictionary = new Dictionary<string, PooledRedisClientManager>();
        }
        private static string[] SplitString(string strSource, string split)
        {
            return strSource.Split(split.ToArray(), StringSplitOptions.RemoveEmptyEntries);
        }

        private static PooledRedisClientManager CreateDefaultManager()
        {
            string defaultRedisServer = ConfigurationManager.AppSettings["DefaultRedisServer"];
            string defaultReadRedisServer = ConfigurationManager.AppSettings["DefaultReadRedisServer"];
            string defaultWriteRedisServer = ConfigurationManager.AppSettings["DefaultWriteRedisServer"];
            if (string.IsNullOrEmpty(defaultReadRedisServer))
            {
                defaultReadRedisServer = defaultRedisServer;
            }
            if (string.IsNullOrEmpty(defaultWriteRedisServer))
            {
                defaultWriteRedisServer = defaultRedisServer;
            }
            string[] writeServerList = SplitString(defaultWriteRedisServer, ",");
            string[] readServerList = SplitString(defaultReadRedisServer, ",");
            return new PooledRedisClientManager(readServerList, writeServerList,
                new RedisClientManagerConfig()
                {
                    AutoStart = true,
                    MaxWritePoolSize = 100,
                    MaxReadPoolSize = 100
                });
        }

        /// <summary>
        /// 创建redis应用程序池
        /// </summary>
        /// <param name="connectStr"></param>
        /// <returns></returns>
        private static PooledRedisClientManager CreateManager(string connectStr)
        {
            string[] arrStr = connectStr.Split(':');
            string host = arrStr.Length > 0 ? arrStr[0] : "";
            int port = 6379;
            if (arrStr.Length > 1)
            {
                int.TryParse(arrStr[1], out port);
            }

            long db = 0;
            if (arrStr.Length > 2)
            {
                long.TryParse(arrStr[2], out db);
            }

            RedisClientManagerConfig config = new RedisClientManagerConfig { MaxReadPoolSize = 100, MaxWritePoolSize = 100, AutoStart = true };
            return new PooledRedisClientManager(new[] { host + ":" + port }, new[] { host + ":" + port },
                config, db, null, null);
        }

        public static IRedisClient GetClient(string connectStr = null)
        {
            if (string.IsNullOrEmpty(connectStr))
            {
                if (_defaultManager == null)
                {
                    _defaultManager = CreateDefaultManager();
                }
                return _defaultManager.GetClient();
            }

            connectStr = connectStr.Trim();
            var manager = ManagerDictionary[connectStr];
            if (manager == null)
            {
                manager = CreateManager(connectStr);
                ManagerDictionary[connectStr] = manager;
            }
            return manager.GetClient();
        }
    }
}
