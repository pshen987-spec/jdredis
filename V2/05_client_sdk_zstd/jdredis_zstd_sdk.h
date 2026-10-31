#ifndef JDREDIS_ZSTD_SDK_H
#define JDREDIS_ZSTD_SDK_H

// Small reusable C++ client wrapper for the v2 workload. The application gives
// this layer raw bytes; only this layer knows the on-wire/on-Redis frame format.
#include <hiredis/hiredis.h>
#include <zstd.h>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

namespace jdredis_sdk {

struct Metrics {
    std::atomic<long long> encoded, decoded, encode_fail, decode_fail;
    std::atomic<long long> raw_bytes, stored_bytes, encode_us, decode_us;
    Metrics():encoded(0),decoded(0),encode_fail(0),decode_fail(0),
              raw_bytes(0),stored_bytes(0),encode_us(0),decode_us(0){}
};
static Metrics metrics[3]; // 0=prewarm, 1=stage2, 2=stage3

inline long long tick_us() {
    return std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
}
inline void put32(std::string& s,uint32_t n) {
    for(int i=0;i<4;i++) s.push_back((char)(n>>(8*i)));
}
inline void put64(std::string& s,uint64_t n) {
    for(int i=0;i<8;i++) s.push_back((char)(n>>(8*i)));
}
inline uint32_t get32(const char* s) {
    uint32_t n=0;for(int i=0;i<4;i++) n|=((uint32_t)(unsigned char)s[i])<<(8*i);return n;
}
inline uint64_t get64(const char* s) {
    uint64_t n=0;for(int i=0;i<8;i++) n|=((uint64_t)(unsigned char)s[i])<<(8*i);return n;
}
inline uint64_t hash64(const std::string& s) {
    uint64_t h=14695981039346656037ULL;
    for(size_t i=0;i<s.size();i++){h^=(unsigned char)s[i];h*=1099511628211ULL;}
    return h;
}

// Frame: JDZ1 | raw_len:u32le | compressed_len:u32le | FNV64:u64le | Zstd.
// APPEND concatenates complete frames; decode() walks them in order.
inline bool encode(const std::string& raw,std::string& frame,int stage,std::string& error) {
    long long t=tick_us();
    frame.clear();error.clear();
    if(raw.size()>64*1024*1024ULL){error="raw frame too large";metrics[stage].encode_fail++;return false;}
    size_t bound=ZSTD_compressBound(raw.size());
    std::string compressed(bound,'\0');
    size_t n=ZSTD_compress(&compressed[0],bound,raw.data(),raw.size(),1);
    if(ZSTD_isError(n)){error=ZSTD_getErrorName(n);metrics[stage].encode_fail++;return false;}
    compressed.resize(n);
    frame.reserve(20+n);
    frame.append("JDZ1",4);
    put32(frame,(uint32_t)raw.size());put32(frame,(uint32_t)n);
    put64(frame,hash64(raw));frame.append(compressed);
    metrics[stage].encoded++;metrics[stage].raw_bytes+=(long long)raw.size();
    metrics[stage].stored_bytes+=(long long)frame.size();metrics[stage].encode_us+=tick_us()-t;
    return true;
}

inline bool decode(const char* wire,size_t wire_len,std::string& raw,int stage,std::string& error) {
    long long t=tick_us();raw.clear();error.clear();
    size_t off=0;
    while(off<wire_len){
        if(wire_len-off<20||memcmp(wire+off,"JDZ1",4)!=0){error="unknown or mixed legacy frame";break;}
        uint32_t raw_len=get32(wire+off+4),compressed_len=get32(wire+off+8);
        uint64_t expected=get64(wire+off+12);
        off+=20;
        if(raw_len>64*1024*1024U||compressed_len>wire_len-off){error="invalid frame length";break;}
        std::string part(raw_len,'\0');
        size_t n=ZSTD_decompress(raw_len?&part[0]:NULL,raw_len,wire+off,compressed_len);
        if(ZSTD_isError(n)||n!=raw_len||hash64(part)!=expected){error="decompression or checksum mismatch";break;}
        raw+=part;off+=compressed_len;metrics[stage].decoded++;
    }
    if(!error.empty()){metrics[stage].decode_fail++;raw.clear();return false;}
    metrics[stage].decode_us+=tick_us()-t;
    return true;
}

inline bool decode_reply(redisReply* reply,std::vector<std::string>& logical,
                         int stage,std::string& error) {
    logical.clear();error.clear();
    if(!reply||reply->type==REDIS_REPLY_ERROR)return true;
    if(reply->type==REDIS_REPLY_STRING){
        logical.resize(1);
        return decode(reply->str,reply->len,logical[0],stage,error);
    }
    if(reply->type==REDIS_REPLY_ARRAY){
        logical.resize(reply->elements);
        for(size_t i=0;i<reply->elements;i++){
            redisReply* e=reply->element[i];
            if(e&&e->type==REDIS_REPLY_STRING&&
               !decode(e->str,e->len,logical[i],stage,error))return false;
        }
    }
    return true;
}

inline redisReply* set(redisContext* c,const std::string& key,const std::string& raw,
                       int ttl,int stage,std::string& error){
    std::string frame;
    if(!encode(raw,frame,stage,error))return NULL;
    return (redisReply*)redisCommand(c,"SET %s %b EX %d",key.c_str(),frame.data(),frame.size(),ttl);
}
inline bool append_set(redisContext* c,const std::string& key,const std::string& raw,
                       int ttl,int stage,std::string& error){
    std::string frame;
    if(!encode(raw,frame,stage,error))return false;
    return redisAppendCommand(c,"SET %s %b EX %d",key.c_str(),frame.data(),frame.size(),ttl)==REDIS_OK;
}
inline redisReply* append(redisContext* c,const std::string& key,const std::string& raw,
                          int stage,std::string& error){
    std::string frame;
    if(!encode(raw,frame,stage,error))return NULL;
    return (redisReply*)redisCommand(c,"APPEND %s %b",key.c_str(),frame.data(),frame.size());
}
inline redisReply* get(redisContext* c,const std::string& key,
                       std::vector<std::string>& logical,int stage,std::string& error){
    redisReply* r=(redisReply*)redisCommand(c,"GET %s",key.c_str());
    if(!decode_reply(r,logical,stage,error)){if(r)freeReplyObject(r);return NULL;}
    return r;
}
inline redisReply* mget(redisContext* c,int argc,const char** argv,const size_t* lens,
                        std::vector<std::string>& logical,int stage,std::string& error){
    redisReply* r=(redisReply*)redisCommandArgv(c,argc,argv,lens);
    if(!decode_reply(r,logical,stage,error)){if(r)freeReplyObject(r);return NULL;}
    return r;
}
} // namespace jdredis_sdk
#endif
