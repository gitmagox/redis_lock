#ifndef MAGOX_REDIS_LOCK_H
#define MAGOX_REDIS_LOCK_H

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

#include "hiredis.h"

/**
 * Handle to the redis server.
 */
redisContext *_m_redis_c = NULL;

/**
 * redis host and password
 */
//是否验证密码
int _m_redis_is_auth = 0;
int _m_redis_port = 6379;
char _m_redis_host[100] = { "127.0.0.1" };
char _m_redis_password[100] = { "123456" };
char _lock_prefix[100] = { "magox_lock" };


//取锁睡眠等待时间
int _locking_sleep = 1000000;

//取锁超时时间
struct timeval doing_timeout = { 1,1000000 };

void magox_redis_keep_alive();
int get_millisecond_time(struct timeval *second_times);
int get_now_millisecond_time();
int magox_redis_lock( char *key_name );
int magox_redis_unlock( char *key_name,int old_time );


void ltrim ( char *s )
{
    char *p;
    p = s;
    while ( *p == ' ' || *p == '\t'  || *p=='\"' ) {*p++;}
    strcpy ( s,p );
}
 
void rtrim ( char *s )
{
    int i;
 
    i = strlen ( s )-1;
    while ( ( s[i] == ' ' || s[i] == '\t' || s[i] == '\"' ) && i >= 0 ) {i--;};
    s[i+1] = '\0';
}
 
void trim ( char *s )
{
    ltrim ( s );
    rtrim ( s );
}


/**
 * get millisecond time
 * 取具体的毫秒数
 */
int
get_millisecond_time(struct timeval *second_times)
{
    return ((*second_times).tv_sec*1000 + (*second_times).tv_usec/1000);
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
magox_redis_lock( char *key_name )
{

    redisReply *reply = NULL;

    //lock timeout //1 seconds
    // struct timeval lock_timeout = { 1,1000000 };

    int     begin_time,
            setnx_millisecond_time,
            back_setnx_time,
            lock_time,
            result_value;

    result_value = 0;
    begin_time = get_now_millisecond_time();
    lock_time = get_millisecond_time(&doing_timeout);
    magox_redis_keep_alive();

    while ( get_now_millisecond_time() < (begin_time+lock_time) ){
        //try to get a lock
        setnx_millisecond_time = get_now_millisecond_time()+lock_time;

        reply = redisCommand(_m_redis_c,"SETNX %s:lock:%s \"%d\"",&_lock_prefix,key_name,setnx_millisecond_time);

        if( reply!=NULL && reply->integer==1)
        {
            result_value = setnx_millisecond_time;
            freeReplyObject(reply);
            break;
        }

        reply = redisCommand(_m_redis_c,"GET %s:lock:%s",&_lock_prefix,key_name);

        if( reply!=NULL && reply->str!=NULL )
        {
            back_setnx_time = atoi(trim(reply->str));

            freeReplyObject(reply);

            setnx_millisecond_time = get_now_millisecond_time()+lock_time;

            //if back time > timeout
            if(setnx_millisecond_time>back_setnx_time)
            {
                //doing getset;
                reply = redisCommand(_m_redis_c,"GETSET %s:lock:%s %d",&_lock_prefix,key_name,setnx_millisecond_time);

                if( reply!=NULL && reply->integer==1)
                {
                    result_value = setnx_millisecond_time;
                    freeReplyObject(reply);
                    break;
                }
            }
        }
        continue;
    }

    return result_value;
}
/**
 * 解锁
 */
int
magox_redis_unlock( char *key_name, int old_time)
{
    redisReply *reply = NULL;
    magox_redis_keep_alive();
    int back_setnx_time;

    reply = redisCommand(_m_redis_c,"GET %s:lock:%s",&_lock_prefix,key_name);

    if( reply!=NULL && reply->str!=NULL )
    {
        back_setnx_time = atoi(trim(reply->str));
        freeReplyObject(reply);
        if(old_time==back_setnx_time){
            //doing unlock;
            reply = redisCommand(_m_redis_c,"DEL %s:lock:%s",&_lock_prefix,key_name);
            //del ok return
            if( reply!=NULL && reply->integer==1)
            {
                freeReplyObject(reply);
                return 0;
            }
        }
    }
    return 0;
}

/**
 * keep redis alive.
 */
void
magox_redis_keep_alive()
{
    redisReply *reply = NULL;

    struct timeval timeout = { 5, 1000000 };

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
        if(_m_redis_is_auth)
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
}

#endif
