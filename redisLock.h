#ifndef MAGOX_REDIS_LOCK_H
#define MAGOX_REDIS_LOCK_H


#include <fuse.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <net/if.h>
#include <sys/ioctl.h>
#include <netinet/in.h>
#include <getopt.h>
#include <stdarg.h>

//#include "hiredis.h"
#include <hiredis.h>

/**
 * Handle to the redis server.
 */
redisContext *_m_redis_c = NULL;


/**
 * redis host and password
 */
int _m_redis_port = 6379;
char _m_redis_host[100] = { "127.0.0.1" };
char _m_redis_password[100] = { "123456" };
char _lock_prefix[100] = { "magox_lock" };

//取锁睡眠等待时间
int _locking_sleep = 1000;

//取锁超时时间
struct timeval doing_timeout = { 1,1000000 };

/**
 * get millisecond time
 * 取具体的毫秒数
 */
int
get_millisecond_time(struct timeval *second_times)
{
    return (*second_times.tv_sec*1000 + *second_times.tv_usec/1000);
}

/**
 * get now millisecond time
 * 取当前毫秒
 */
int
get_now_millisecond_time()
{
    struct timeval tv;
    gettimeofday(&tv,NULL);
    return get_millisecond_time(&tv);
}
/**
 * 上锁
 */
int
magox_redis_lock( char key_name )
{
    char setnx_millisecond_time_char[15];
    char back_getset_time_char[15];

    int geting = 1;

    redisReplay *reply = NULL;

    //lock timeout //1 seconds
    struct timeval lock_timeout = { 1,1000000 };

    int     begin_time,
            setnx_millisecond_time,
            now_time,
            back_setnx_time,
            back_getset_time;

    begin_time = get_now_millisecond_time();

    _magox_redis_keep_alive();

    while ( get_now_millisecond_time() < (begin_time+get_millisecond_time(&doing_timeout))){
        //try to get a lock
        setnx_millisecond_time = get_now_millisecond_time();

        snprintf(setnx_millisecond_time_char, 15, "%d", setnx_millisecond_time);

        reply = redisCommand(_m_redis,"SETNX %s:lock:%s %d",_lock_prefix,key_name,setnx_millisecond_time);

        if( reply->type == REDIS_REPLY_NIL )
        {
            freeReplyObject(reply);
            continue;
        }


        if( reply!=NULL && reply->integer==1)
        {
            freeReplyObject(reply);
            return 0;
        }

        while ( get_now_millisecond_time() < ( begin_time+get_millisecond_time(&doing_timeout) ) )
        {

            reply = redisCommand(_m_redis,"GET %s:lock:%s",_lock_prefix,this_lock_name);

            if( reply->type == REDIS_REPLY_NIL )
            {
                freeReplyObject(reply);
                break;
            }

            if( reply!=NULL && reply->str!=NULL )
            {
                back_setnx_time = atoi(reply->str);

                freeReplyObject(reply);

                now_time = get_now_millisecond_time();

                //if back time > timeout
                if(now_time>back_setnx_time+get_millisecond_time(&lock_timeout))
                {
                    //doing getset;
                    reply = redisCommand(_m_redis,"GETSET %s:lock:%s %d",_lock_prefix,this_lock_name,now_time);

                    if( reply->type == REDIS_REPLY_NIL )
                    {
                        freeReplyObject(reply);
                        break;
                    }
                    if ((reply != NULL) && (reply->type == REDIS_REPLY_STRING))
                    {
                        back_getset_time = atoi(reply->str);
                        freeReplyObject(reply);

                        if(back_getset_time==back_setnx_time)
                        {
                            return 0;
                        }
                    }

                }

            }

            sleep(_locking_sleep);
        }
    }
}
/**
 * 解锁
 */
int
magox_redis_unlock( char key_name )
{
    _magox_redis_keep_alive();

    while ( get_now_millisecond_time() < (begin_time+get_millisecond_time(&doing_timeout)) )
    {
        //doing unlock;
        reply = redisCommand(_m_redis,"DEL %s:lock:%s",_lock_prefix,this_lock_name);
        //del ok return
        if( reply!=NULL && reply->integer==1)
        {
            freeReplyObject(reply);
            return 0;
        }
        sleep(_locking_sleep);
    }

}

/**
 * keep redis alive.
 */
void
_magox_redis_keep_alive()
{
    struct timeval timeout = { 1, 5000000 };
    redisReply *reply = NULL;

    if (_m_redis_c != NULL && _m_redis_c->errstr!= NULL )
    {
        reply = redisCommand(_m_redis_c, "PING");

        if ((reply != NULL) &&
            (reply->str != NULL) &&
            ((strcmp(reply->str, "PONG") == 0)||(strcmp(reply->str, "QUEUED") == 0)))
        {
            freeReplyObject(reply);
            return;
        }
        else
        {
            if (reply != NULL)
                freeReplyObject(reply);
        }
    }

    _m_redis_c = redisConnectWithTimeout(_m_redis_host, _m_redis_port, timeout);
    if (_m_redis_c == NULL || _m_redis_c -> err)
    {
        if (_m_redis_c) {
            fprintf(stderr, "Failedi to connect to redis on :[%s]",
                    _m_redis_c->errstr);
            redisFree(_m_redis_c);
        }else{
            fprintf(stderr, "Failed to connect to redis on [%s:%d].\n",
                    _m_redis_host, _m_redis_port);
        }
        exit(1);
    }
    else
    {
        reply= redisCommand(_m_redis_c, "AUTH %s", _m_redis_password);
        if (reply->type == REDIS_REPLY_ERROR) {
            fprintf(stderr, "password is wrong [%s] check it right.\n",
                    _m_redis_password);
            exit(1);
        }
        freeReplyObject(reply);
    }
}

#endif