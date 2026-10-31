// opt6a_io_2c2t: same v2 workload; only Redis I/O threads and CPU affinity change.
// Build on 186: g++ -O3 -std=c++11 stress_test_opt6a_io.cpp -o stress_test_opt6a_io -pthread -lhiredis
#include <hiredis/hiredis.h>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <random>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>
#include <algorithm>
#include <sys/resource.h>
using namespace std;
using namespace std::chrono;
static const char* BANNER="=== 测试版本: opt6a_io_2c2t_v2 ===";
static const char* WORKLOAD="workload=full_pool_warm_normal_set_fixed200k_append_fixed200k_20set_20mget_40get_5append";
static const int TTL=1, MGET_KEYS=100, S2=30, S3=60;
static const int WARM_BATCH=1000;
static const long long LIMIT_US=5000;
static atomic<int> phase(0);
struct Stat {
    atomic<long long> offered,ok,err,slow,sum,max,hit,miss,bytes,illegal;
    Stat():offered(0),ok(0),err(0),slow(0),sum(0),max(0),hit(0),miss(0),bytes(0),illegal(0){}
};
static Stat stats[2][4];
static const char* names[4]={"MGET","SET","GET","APPEND"};
static vector<string> values;
static string abnormal_value;
static long long clock_us(){return duration_cast<microseconds>(steady_clock::now().time_since_epoch()).count();}
static double client_cpu_sec(){
    rusage ru;
    if(getrusage(RUSAGE_SELF,&ru)!=0)return -1;
    return ru.ru_utime.tv_sec+ru.ru_utime.tv_usec/1e6+
           ru.ru_stime.tv_sec+ru.ru_stime.tv_usec/1e6;
}
static void update_max(atomic<long long>& x,long long n){long long old=x.load();while(old<n&&!x.compare_exchange_weak(old,n)){}}
static redisContext* connect_to(const string& host,int port){
    timeval tv={0,500000};
    redisContext* c=redisConnectWithTimeout(host.c_str(),port,tv);
    if(!c||c->err){if(c)redisFree(c);return nullptr;}
    timeval rt={0,100000};redisSetTimeout(c,rt);return c;
}
static bool number(const char* s,double& out){char* end=nullptr;out=strtod(s,&end);return end!=s&&*end=='\0'&&isfinite(out);}
static int sample_kib(mt19937& rng){
    normal_distribution<double> d(200.0,190.0/3.0);
    for(;;){int n=(int)round(d(rng));if(n>=10&&n<=390)return n;}
}
static string payload(mt19937& rng,int kib){
    string v((size_t)kib*1024,'\0');
    uniform_int_distribution<int> digit(0,255);
    for(size_t i=0;i<v.size();++i)v[i]=(char)digit(rng);
    return v;
}
static string key(int n){return "test:v2:data:"+to_string(n);}
static bool prewarm_pool(redisContext* c,mt19937& rng,int pool,long long& bytes){
    timeval rt={20,0};
    if(redisSetTimeout(c,rt)!=REDIS_OK)return false;
    int pending=0;
    auto flush=[&]()->bool{
        for(int i=0;i<pending;i++){
            void* raw=nullptr;
            if(redisGetReply(c,&raw)!=REDIS_OK||!raw)return false;
            redisReply* r=(redisReply*)raw;
            bool ok=r->type==REDIS_REPLY_STATUS;
            freeReplyObject(r);
            if(!ok)return false;
        }
        pending=0;
        return true;
    };
    for(int i=0;i<pool;i++){
        const string k=key(i);
        const string& v=values[sample_kib(rng)-10];
        if(redisAppendCommand(c,"SET %s %b EX %d",k.c_str(),v.data(),v.size(),TTL)!=REDIS_OK)return false;
        bytes+=(long long)v.size();
        if(++pending==WARM_BATCH&&!flush())return false;
    }
    return pending==0||flush();
}
static bool mark(redisContext*& c,const string& host,int port,int p){
    for(int attempt=0;attempt<5;attempt++){
        if(!c)c=connect_to(host,port);
        if(c){
            timeval rt={1,0};redisSetTimeout(c,rt);
            redisReply* r=(redisReply*)redisCommand(c,"SET test:phase:marker %d EX 120",p);
            if(r){bool ok=r->type!=REDIS_REPLY_ERROR;freeReplyObject(r);if(ok)return true;}
            redisFree(c);c=nullptr;
        }
        this_thread::sleep_for(milliseconds(100));
    }
    return false;
}
static void report(const char* label,int idx,double mult,int pool,const int workers[4]){
    cout<<"["<<label<<"] concurrency_scale="<<mult<<" key_pool="<<pool
        <<" mget_threads="<<workers[0]<<" set_threads="<<workers[1]
        <<" get_threads="<<workers[2]<<" append_threads="<<workers[3]<<endl;
    for(int k=0;k<4;k++){
        Stat& s=stats[idx][k];long long n=s.ok.load();long long avg=n?s.sum.load()/n:-1;
        cout<<"["<<label<<" "<<names[k]<<"] offered="<<s.offered<<" success="<<n<<" error="<<s.err
            <<" success_ops="<<(double)n/(idx==0?S2:S3)
            <<" rtt_gt_5ms="<<s.slow<<" avg_us="<<avg<<" max_us="<<(n?s.max.load():-1)
            <<" hit="<<s.hit<<" miss="<<s.miss<<" response_bytes="<<s.bytes
            <<" illegal_input="<<s.illegal<<endl;
    }
    cout<<"["<<label<<" RTT(us)]";
    for(int k=0;k<4;k++){Stat& s=stats[idx][k];long long n=s.ok.load();
        cout<<(k?" | ":" ")<<names[k]<<" avg="<<(n?s.sum.load()/n:-1)
            <<" max="<<(n?s.max.load():-1);}
    cout<<endl;
}
int main(int argc,char** argv){
    if(argc==2&&string(argv[1])=="--identify"){
        cout<<BANNER<<" MGET总聚合=100  RTT超时阈值=5ms  网络读保护超时=100ms"<<endl;
        cout<<WORKLOAD<<endl;
        cout<<"预热整个Key池; 预热Value截断正态分布=200KiB±63.33KiB 限界10..390KiB; 正式SET固定200KiB TTL=1s; APPEND每次固定200KiB"<<endl;return 0;
    }
    if(argc==2&&string(argv[1])=="--self-test"){
        mt19937 g(20261028);long long sum=0;int lo=391,hi=0;
        for(int i=0;i<100000;i++){int x=sample_kib(g);sum+=x;lo=min(lo,x);hi=max(hi,x);}
        cout<<"self-test mean_KiB="<<(double)sum/100000<<" min_KiB="<<lo<<" max_KiB="<<hi<<endl;
        return fabs((double)sum/100000-200)<2&&lo>=10&&hi<=390?0:1;
    }
    if(argc<3||argc>7){
        cerr<<"usage: "<<argv[0]<<" HOST PORT [THREAD_MULTIPLIER=1] [POOL=400000] [ABNORMAL_SET_SHARE=0] [SEED=20261028]"<<endl;return 2;
    }
    string host=argv[1];int port=atoi(argv[2]);double mult=1,abnormal=0;
    int pool=400000;unsigned seed=20261028;
    if(argc>3&&!number(argv[3],mult))return 2;
    if(argc>4)pool=atoi(argv[4]);
    if(argc>5&&!number(argv[5],abnormal))return 2;
    if(argc>6)seed=(unsigned)strtoul(argv[6],nullptr,10);
    if(port<1||port>65535||mult<=0||mult>6||pool<100||pool>400000||abnormal<0||abnormal>1){cerr<<"invalid arguments"<<endl;return 2;}
    int workers[4]={(int)round(20*mult),(int)round(20*mult),
                    (int)round(40*mult),(int)round(5*mult)};
    for(int k=0;k<4;k++)workers[k]=max(workers[k],1);
    mt19937 warm_rng(seed);redisContext* warm=connect_to(host,port);if(!warm){cerr<<"Redis connect failed"<<endl;return 3;}
    values.reserve(381);
    for(int kib=10;kib<=390;kib++)values.push_back(payload(warm_rng,kib));
    abnormal_value=payload(warm_rng,512);
    cout<<BANNER<<" MGET总聚合=100  RTT超时阈值=5ms  网络读保护超时=100ms"<<endl;
    cout<<WORKLOAD<<endl;
    cout<<"warmup Value: truncated Normal(200KiB,63.33KiB), 10..390KiB; workload SET Value=200KiB; SET TTL=1s; APPEND payload=200KiB; seed="<<seed<<endl;
    cout<<"concurrency: MGET="<<workers[0]<<" SET="<<workers[1]<<" GET="<<workers[2]
        <<" APPEND="<<workers[3]<<" abnormal_SET="<<abnormal<<endl;
    // 与旧基线一样先完整写入Key池；尽管TTL仅1秒，预热产生的分配/过期历史仍是负载的一部分。
    cout<<"[预热] keys="<<pool<<" TTL=1s batch="<<WARM_BATCH<<endl;
    auto warm_started=steady_clock::now();
    long long warm_bytes=0;
    if(!prewarm_pool(warm,warm_rng,pool,warm_bytes)){
        cerr<<"prewarm failed: "<<warm->errstr<<endl;
        redisFree(warm);
        return 4;
    }
    cout<<"[预热完成] keys="<<pool<<" written_bytes="<<warm_bytes
        <<" elapsed_seconds="<<duration_cast<seconds>(steady_clock::now()-warm_started).count()<<endl;
    cout<<"[keyspace] uniform_key_pool="<<pool<<" no_separate_keepalive"<<endl;
    if(!mark(warm,host,port,2)){cerr<<"phase2 marker failed"<<endl;return 5;}
    phase=2;
    auto started=steady_clock::now();
    double cpu_stage2_start=client_cpu_sec();
    vector<thread> ts;
    try {
    for(int kind=0;kind<4;kind++)for(int worker=0;worker<workers[kind];worker++)
        ts.emplace_back([=](){
            mt19937 rng(seed+kind*10007+worker*101);uniform_int_distribution<int> pick(0,pool-1);
            uniform_real_distribution<double> coin(0,1);
            redisContext* c=nullptr;
            string append_key;
            int append_step=0;
            while(phase.load()!=4){
                int p=phase.load();
                if(p==0){this_thread::sleep_for(milliseconds(1));continue;}
                if(p!=2&&p!=3)break;
                if(p==2&&(kind==2||kind==3)){this_thread::sleep_for(milliseconds(10));continue;}
                Stat& st=stats[p-2][kind];st.offered++;
                if(!c)c=connect_to(host,port);
                if(!c){st.err++;this_thread::sleep_for(milliseconds(10));continue;}
                redisReply* r=nullptr;long long begin=clock_us();bool illegal=false;
                if(kind==1){
                    bool ab=coin(rng)<abnormal;
                    const string& v=ab?abnormal_value:values[SET_VALUE_KIB-10];string k=key(pick(rng));
                    illegal=v.size()>=500*1024;if(illegal)st.illegal++;
                    r=(redisReply*)redisCommand(c,"SET %s %b EX 1",k.c_str(),v.data(),v.size());
                }else if(kind==2){
                    string k=key(pick(rng));r=(redisReply*)redisCommand(c,"GET %s",k.c_str());
                }else if(kind==0){
                    vector<string> keys;keys.reserve(MGET_KEYS);
                    unordered_set<int> selected;
                    while((int)keys.size()<MGET_KEYS){
                        int id=pick(rng);
                        if(selected.insert(id).second)keys.push_back(key(id));
                    }
                    vector<const char*> args(MGET_KEYS+1);vector<size_t> lens(MGET_KEYS+1);
                    args[0]="MGET";lens[0]=4;
                    for(int i=0;i<MGET_KEYS;i++){args[i+1]=keys[i].c_str();lens[i+1]=keys[i].size();}
                    r=(redisReply*)redisCommandArgv(c,MGET_KEYS+1,args.data(),lens.data());
                }else{
                    if(append_step==0)append_key=key(pick(rng));
                    const string& v=values[SET_VALUE_KIB-10]; // APPEND每次固定追加200KiB
                    r=(redisReply*)redisCommand(c,"APPEND %s %b",append_key.c_str(),v.data(),v.size());
                    if(r&&r->type==REDIS_REPLY_INTEGER&&r->integer>=500*1024)st.illegal++;
                    // 保持Redis原生语义：已有Key的TTL不变；不存在的Key由APPEND创建且没有TTL。
                    append_step=(append_step+1)%5;
                }
                long long elapsed=clock_us()-begin;
                if(!r||r->type==REDIS_REPLY_ERROR){
                    st.err++;
                    if(r)freeReplyObject(r);
                    if(!r){redisFree(c);c=nullptr;}
                    this_thread::sleep_for(milliseconds(10));
                    continue;
                }
                st.ok++;st.sum+=elapsed;update_max(st.max,elapsed);if(elapsed>LIMIT_US)st.slow++;
                if(r->type==REDIS_REPLY_NIL)st.miss++;
                else if(r->type==REDIS_REPLY_STRING){st.hit++;st.bytes+=r->len;}
                else if(r->type==REDIS_REPLY_ARRAY){for(size_t i=0;i<r->elements;i++){redisReply* item=r->element[i];if(item->type==REDIS_REPLY_STRING){st.hit++;st.bytes+=item->len;}else if(item->type==REDIS_REPLY_NIL)st.miss++;}}
                freeReplyObject(r);
                if(kind==1)this_thread::sleep_for(milliseconds(2));
                if(kind==3&&append_step==0)this_thread::sleep_for(milliseconds(100));
            }
            if(c)redisFree(c);
        });
    }catch(const std::exception& e){
        phase=4;
        for(auto& t:ts)if(t.joinable())t.join();
        cerr<<"client thread creation failed: "<<e.what()<<endl;
        return 6;
    }
    this_thread::sleep_until(started+seconds(S2));
    phase=0;
    this_thread::sleep_for(milliseconds(150)); // 等待阶段2在途请求记账完成
    double cpu_stage2_end=client_cpu_sec();
    if(!mark(warm,host,port,3)){cerr<<"phase3 marker failed"<<endl;phase=4;for(auto& t:ts)t.join();return 5;}
    cout<<"[阶段2结束] measured_seconds=30"<<endl;report("阶段2",0,mult,pool,workers);
    cout<<"[客户端CPU 阶段2] average_cores="<<(cpu_stage2_end-cpu_stage2_start)/S2<<endl;
    double cpu_stage3_start=client_cpu_sec();
    phase=3;
    auto stage3_started=steady_clock::now();
    this_thread::sleep_until(stage3_started+seconds(S3));
    phase=4;
    for(auto& t:ts)t.join();
    redisFree(warm);
    cout<<"[全部结束] measured_seconds=60"<<endl;report("阶段3",1,mult,pool,workers);
    cout<<"[客户端CPU 阶段3] average_cores="<<(client_cpu_sec()-cpu_stage3_start)/S3<<endl;
    return 0;
}
