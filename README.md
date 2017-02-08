# A Redis Utility Using ServiceStack.Redis
[![Build status](https://ci.appveyor.com/api/projects/status/7b79ftxith3rqq2h?svg=true)](https://ci.appveyor.com/project/kinshines/servicestack-redis-utility)   [![NuGet package](https://badge.fury.io/nu/RedisUtility.svg)](https://www.nuget.org/packages/RedisUtility/)
## Configuration
### This Library need config the AppSetting:'DefaultRedisServer' value in App.config Or Web.config file.
### If the ReadRedisServer and WriteRedisServer are Not Same,you should config the AppSetting :'DefaultReadRedisServer' value and 'DefaultWriteRedisServer' value separately.
### You can also connect other redis server by passing the server address when calling the method directly
## Usage
### For Example: Redis.SetHashRedisValue("hashId","key","value")
