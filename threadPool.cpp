#include <iostream>
#include <iomanip>
#include <thread>
#include <vector>
#include <mutex>
#include <deque>
#include <queue>
#include <functional>
#include <condition_variable>
#include <future>
#include <atomic>
#include <stdexcept>
#include <optional>
#include <random>
#include <algorithm>
#include <chrono>
#include <array>
#include <string>
#include <cassert>
#include <climits>
#include <map>
#include <cmath>
#include <memory>
using namespace std;

constexpr size_t CACHE_LINE_SIZE = 64;
enum class Priority { LOW = 0, NORMAL = 1, HIGH = 2 };

struct CancellationException : exception {
    const char* what() const noexcept override { return "task cancelled"; }
};

class CancellationToken {
    alignas(CACHE_LINE_SIZE) atomic<bool> cancelled_{false};
public:
    void cancel()             { cancelled_.store(true, memory_order_release); }
    bool is_cancelled() const { return cancelled_.load(memory_order_acquire); }
    void throw_if_cancelled() const {
        if (is_cancelled()) throw CancellationException{};
    }
};
using CancellationTokenPtr = shared_ptr<CancellationToken>;
CancellationTokenPtr make_token() { return make_shared<CancellationToken>(); }

struct TaskHandle {
    CancellationTokenPtr token;
    shared_future<void>  future;
    bool cancel() {
        if (!token || is_done()) return false;
        token->cancel(); return true;
    }
    void wait() {
        if (!future.valid()) return;
        try { future.get(); }
        catch (const CancellationException&) {}
        catch (...) { throw; }   // fix: was {throw} missing semicolon
    }
    bool is_done() const {
        return future.valid() &&
               future.wait_for(chrono::seconds(0)) == future_status::ready;
    }
    bool is_cancelled() const { return token && token->is_cancelled(); }
};

struct Task {
    Priority priority         = Priority::NORMAL;
    int      arrival_sequence = 0;
    function<void()> work;
    chrono::steady_clock::time_point submit_time = chrono::steady_clock::now();
};

struct SchedulerStats {
    alignas(CACHE_LINE_SIZE) atomic<long> tasks_submitted{0};
    alignas(CACHE_LINE_SIZE) atomic<long> tasks_executed{0};
    alignas(CACHE_LINE_SIZE) atomic<long> tasks_stolen{0};
    alignas(CACHE_LINE_SIZE) atomic<long> total_latency_us{0};
    alignas(CACHE_LINE_SIZE) atomic<long> min_latency_us{LONG_MAX};
    alignas(CACHE_LINE_SIZE) atomic<long> max_latency_us{0};
    struct alignas(CACHE_LINE_SIZE) WorkerStats {
        atomic<long> tasks_executed{0};
        atomic<long> tasks_stolen{0};
        atomic<long> times_idle{0};
    };
    vector<WorkerStats> workers;
    explicit SchedulerStats(int n) : workers(n) {}

    void record_latency(long us) {
        total_latency_us.fetch_add(us, memory_order_relaxed);
        long c = min_latency_us.load(memory_order_relaxed);
        while (us < c && !min_latency_us.compare_exchange_weak(c, us, memory_order_relaxed, memory_order_relaxed)) {}
        c = max_latency_us.load(memory_order_relaxed);
        while (us > c && !max_latency_us.compare_exchange_weak(c, us, memory_order_relaxed, memory_order_relaxed)) {}
    }
    void print() const {
        long s=tasks_submitted.load(memory_order_relaxed);
        long e=tasks_executed .load(memory_order_relaxed);
        long t=tasks_stolen   .load(memory_order_relaxed);
        cout << "\n╔══════════════════════════════════════╗\n"
             <<   "║        SCHEDULER STATISTICS          ║\n"
             <<   "╠══════════════════════════════════════╣\n"
             <<   "║  Submitted: " << setw(12) << s << "               ║\n"
             <<   "║  Executed:  " << setw(12) << e << "               ║\n"
             <<   "║  Stolen:    " << setw(12) << t << "               ║\n";
        if (e>0) cout << "║  Steal%:    " << setw(11) << (t*100/e) << "%              ║\n"
                      << "║  Avg lat:   " << setw(10)
                      << total_latency_us.load(memory_order_relaxed)/e << "us             ║\n";
        cout << "╚══════════════════════════════════════╝\n";
    }
    void verify() const {
        long s=tasks_submitted.load(memory_order_relaxed);
        long e=tasks_executed .load(memory_order_relaxed);
        if (s!=e) cerr<<"❌ LOST submitted="<<s<<" executed="<<e<<"\n";
        else      cout<<"✅ No lost tasks: "<<s<<" == "<<e<<"\n";
    }
    void analyze_utilization() const {
        long total=0; vector<long> pw;
        for (auto& w:workers){pw.push_back(w.tasks_executed.load(memory_order_relaxed));total+=pw.back();}
        int n=(int)pw.size();
        if (!n||!total) return;
        double ideal=(double)total/n;
        cout<<"\n  Worker utilization (ideal="<<fixed<<setprecision(0)<<ideal<<"):\n";
        for (int i=0;i<n;i++){
            double dev=fabs(pw[i]-ideal)/ideal*100.0;
            cout<<"    W"<<i<<": "<<setw(7)<<pw[i]<<" exec | "
                <<setw(6)<<workers[i].tasks_stolen.load(memory_order_relaxed)
                <<" stolen | "<<setprecision(1)<<dev<<"% dev\n";
        }
    }
};

class BucketedPriorityDeque {
    array<deque<Task>,3> buckets;
    mutable mutex m;
    alignas(CACHE_LINE_SIZE) atomic<int> approx_size{0};
    static int idx(Priority p) { return static_cast<int>(p); }
public:
    void push(Task task) {
        { lock_guard<mutex> lk(m); buckets[idx(task.priority)].push_back(move(task)); }
        approx_size.fetch_add(1, memory_order_relaxed);
    }
    vector<Task> pop_batch(int mx) {
        vector<Task> b; b.reserve(mx);
        { lock_guard<mutex> lk(m);
          for (int i=2;i>=0&&(int)b.size()<mx;i--)
              while (!buckets[i].empty()&&(int)b.size()<mx)
                  {b.push_back(move(buckets[i].back()));buckets[i].pop_back();}
        }
        if (!b.empty()) approx_size.fetch_sub((int)b.size(), memory_order_relaxed);
        return b;
    }
    optional<Task> steal_front() {
        optional<Task> r;
        { lock_guard<mutex> lk(m);
          for (int i=0;i<=2;i++) if (!buckets[i].empty())
              {r=move(buckets[i].front());buckets[i].pop_front();break;}
        }
        if (r) approx_size.fetch_sub(1, memory_order_relaxed);
        return r;
    }
    bool empty() const {
        lock_guard<mutex> lk(m);
        for (auto& b:buckets) if (!b.empty()) return false;
        return true;
    }
    int size_approx() const { return approx_size.load(memory_order_relaxed); }
};

class DynamicThreadPool {
private:
    struct Worker {
        BucketedPriorityDeque deque;
        thread             t;
        mutex              cv_m;
        condition_variable cv;
        int                id = 0;
        atomic<bool>       is_retired{false};  // fix: was "retired" throughout
        atomic<bool>       is_idle{false};
    };

    vector<unique_ptr<Worker>> workers;
    mutex workers_m;              // fix: was workers_m_ (trailing underscore) in several places

    alignas(CACHE_LINE_SIZE) atomic<bool> stop_flag{false};
    alignas(CACHE_LINE_SIZE) atomic<int>  next_worker{0};
    alignas(CACHE_LINE_SIZE) atomic<int>  id_generator{0};
    alignas(CACHE_LINE_SIZE) atomic<int>  total_tasks{0};
    alignas(CACHE_LINE_SIZE) atomic<int>  active_count{0};
    alignas(CACHE_LINE_SIZE) atomic<int>  worker_count{0};

    const int min_workers, max_workers, idle_timeout_sec, tasks_per_worker;
    static constexpr int BATCH_SIZE = 8;

public:
    SchedulerStats stats;

private:
    optional<Task> try_steal(int my_id) {
        int n=worker_count.load(memory_order_acquire);
        int busiest=-1; int max_sz=0;
        for (int i=0;i<n;i++){
            if (i==my_id) continue;
            int s=workers[i]->deque.size_approx();
            if (s>max_sz){max_sz=s;busiest=i;}
        }
        if (busiest>=0&&max_sz>0){
            auto t=workers[busiest]->deque.steal_front();
            if (t){total_tasks.fetch_sub(1,memory_order_acq_rel);return t;}
        }
        for (int i=1;i<n;i++){
            int tgt=(my_id+i)%n;
            auto t=workers[tgt]->deque.steal_front();
            if (t){total_tasks.fetch_sub(1,memory_order_acq_rel);return t;}
        }
        return nullopt;
    }

    void notify_one_idle_worker(int exclude_id) {
        int n=worker_count.load(memory_order_acquire);
        for (int i=0;i<n;i++){
            if (workers[i]->id==exclude_id) continue;
            if (workers[i]->is_idle.load(memory_order_relaxed))
                {workers[i]->cv.notify_one();return;}
        }
    }

    void run_task(int my_id, Task task) {
        auto us=chrono::duration_cast<chrono::microseconds>(
            chrono::steady_clock::now()-task.submit_time).count();
        stats.record_latency(us);
        try { task.work(); }
        catch (const exception& e) { cerr<<"Worker "<<my_id<<" exception: "<<e.what()<<"\n"; }
        catch (...) { cerr<<"Worker "<<my_id<<" unknown exception\n"; }
    }

    void worker_loop(int my_id) {
        Worker& me=*workers[my_id];
        while (true){
            while (true){
                auto batch=me.deque.pop_batch(BATCH_SIZE);
                if (batch.empty()) break;
                for (auto& t:batch){
                    total_tasks.fetch_sub(1,memory_order_acq_rel);
                    stats.tasks_executed.fetch_add(1,memory_order_relaxed);
                    stats.workers[my_id].tasks_executed.fetch_add(1,memory_order_relaxed);
                    run_task(my_id,t);
                }
            }
            {
                auto task=try_steal(my_id);
                if (task){
                    stats.tasks_executed.fetch_add(1,memory_order_relaxed);
                    stats.tasks_stolen  .fetch_add(1,memory_order_relaxed);
                    stats.workers[my_id].tasks_executed.fetch_add(1,memory_order_relaxed);
                    stats.workers[my_id].tasks_stolen  .fetch_add(1,memory_order_relaxed);
                    run_task(my_id,*task);
                    continue;
                }
            }
            if (stop_flag.load(memory_order_acquire)&&total_tasks.load(memory_order_acquire)==0){
                while (auto t=try_steal(my_id)){
                    stats.tasks_executed.fetch_add(1,memory_order_relaxed);
                    stats.tasks_stolen  .fetch_add(1,memory_order_relaxed);
                    stats.workers[my_id].tasks_executed.fetch_add(1,memory_order_relaxed);
                    stats.workers[my_id].tasks_stolen  .fetch_add(1,memory_order_relaxed);
                    run_task(my_id,*t);
                }
                active_count.fetch_sub(1,memory_order_relaxed);
                return;
            }
            {
                unique_lock<mutex> lock(me.cv_m);
                me.is_idle.store(true,memory_order_relaxed);
                stats.workers[my_id].times_idle.fetch_add(1,memory_order_relaxed);

                // fix: was cv.wait() — cv.wait has no timeout; must be cv.wait_for
                bool got_work=me.cv.wait_for(lock,
                    chrono::seconds(idle_timeout_sec),
                    [&]{ return stop_flag.load(memory_order_acquire)
                              ||!me.deque.empty()
                              ||total_tasks.load(memory_order_acquire)>0; });

                me.is_idle.store(false,memory_order_relaxed);

                if (!got_work&&me.deque.empty()){
                    int cur=active_count.load(memory_order_relaxed);
                    if (cur>min_workers){
                        if (active_count.compare_exchange_strong(cur,cur-1,memory_order_acq_rel)){
                            cout<<"  [pool] W"<<my_id<<" retiring (active="<<(cur-1)<<"/"<<max_workers<<")\n";
                            me.is_retired.store(true,memory_order_release); // fix: was me.retired
                            return;
                        }
                    }
                }
            }
            // fix: was stop_flag_ and total_tasks_ (with trailing underscore)
            if (stop_flag.load(memory_order_acquire)&&total_tasks.load(memory_order_acquire)==0) return;
        }
    }

    void spawn_worker(int id) {
        workers[id]->id=id;
        workers[id]->is_retired.store(false,memory_order_relaxed); // fix: was retired
        workers[id]->is_idle.store(false,memory_order_relaxed);
        workers[id]->t=thread(&DynamicThreadPool::worker_loop,this,id);   // fix: was Workers[id] (capital W)
        active_count.fetch_add(1,memory_order_relaxed);
        worker_count.store(id+1,memory_order_release);
    }

    void maybe_scale_up() {
        if (stop_flag.load(memory_order_acquire)) return;
        int cur=active_count.load(memory_order_relaxed);
        int q  =total_tasks .load(memory_order_relaxed);
        if (q<=cur*tasks_per_worker||cur>=max_workers) return;

        lock_guard<mutex> lk(workers_m);
        cur=active_count.load(memory_order_relaxed);
        if (cur>=max_workers||stop_flag.load(memory_order_acquire)) return;

        int slot=-1;
        for (int i=0;i<(int)workers.size();i++){
            if (workers[i]->is_retired.load(memory_order_relaxed)){ // fix: was retired
                if (workers[i]->t.joinable()) workers[i]->t.join();
                slot=i; break;
            }
        }
        if (slot<0&&(int)workers.size()<max_workers){
            workers.push_back(make_unique<Worker>());
            slot=(int)workers.size()-1;
        }
        if (slot<0) return;
        cout<<"  [pool] Scale-up: W"<<slot<<" (active="<<(cur+1)<<"/"<<max_workers<<")\n";
        spawn_worker(slot);
    }

    void push_task(Task task, int preferred_worker) {
        int target;
        {
            lock_guard<mutex> lk(workers_m);
            int n=worker_count.load(memory_order_acquire);
            if (n==0) throw runtime_error("no workers available");
            target=-1;
            if (preferred_worker>=0&&preferred_worker<n&&
                !workers[preferred_worker]->is_retired.load(memory_order_relaxed)) // fix: was retired
                target=preferred_worker;
            if (target<0){
                for (int a=0;a<n;a++){
                    int t=next_worker.fetch_add(1,memory_order_relaxed)%n;
                    if (!workers[t]->is_retired.load(memory_order_relaxed)) // fix: was retired
                        {target=t;break;}
                }
            }
            if (target<0) target=0;
            workers[target]->deque.push(move(task));
        }
        total_tasks.fetch_add(1,memory_order_acq_rel);
        stats.tasks_submitted.fetch_add(1,memory_order_relaxed);
        workers[target]->cv.notify_one();
        notify_one_idle_worker(target); // fix: was notify_idle (function doesn't exist)
        maybe_scale_up();
    }

public:
    explicit DynamicThreadPool(int min_w=2, int max_w=-1, int idle_sec=5, int thr_per_w=4)
        : min_workers(min_w)
        , max_workers(max_w<0?max(min_w,(int)thread::hardware_concurrency()):max_w)
        , idle_timeout_sec(idle_sec)
        , tasks_per_worker(thr_per_w)  // fix: was tasks_per_worker_thr (wrong member name)
        , stats(max(min_w,max_w<0?(int)thread::hardware_concurrency():max_w))
    {
        workers.reserve(max_workers);
        for (int i=0;i<min_workers;i++){
            workers.push_back(make_unique<Worker>());
            spawn_worker(i);
        }
        cout<<"[ThreadPool] min="<<min_workers<<" max="<<max_workers
            <<" idle_timeout="<<idle_timeout_sec<<"s\n"; // fix: was idle_timeout_sec_
    }

    template<typename F>
    auto submit(F task, Priority priority=Priority::NORMAL, int preferred_worker=-1)
        -> future<decltype(task())>
    {
        using RT=decltype(task());
        if (stop_flag.load(memory_order_acquire))
            throw runtime_error("ThreadPool is stopped");
        auto pt=make_shared<packaged_task<RT()>>(move(task));
        future<RT> result=pt->get_future();
        int seq=id_generator.fetch_add(1,memory_order_relaxed); // fix: was sequence_counter (not declared)
        push_task(Task{priority,seq,[pt](){(*pt)();}},preferred_worker); // fix: was missing semicolon
        return result;
    }

    TaskHandle submit_cancellable(
        function<void()> task,
        Priority priority=Priority::NORMAL,     // fix: was "priotity" (typo)
        CancellationTokenPtr token=nullptr,
        int preferred_worker=-1)
    {
        if (stop_flag.load(memory_order_acquire)) // fix: was stop_flag_ (trailing underscore)
            throw runtime_error("pool is stopped");
        if (!token) token=make_token();
        auto pt=make_shared<packaged_task<void()>>(
            [fn=move(task),tok=token](){
                tok->throw_if_cancelled();
                fn();
            });
        shared_future<void> result=pt->get_future().share();
        int seq=id_generator.fetch_add(1,memory_order_relaxed); // fix: was seq_counter (not declared)
        push_task(Task{priority,seq,[pt](){(*pt)();}},preferred_worker);
        return TaskHandle{token,result};
    }

    void shutdown() {
        bool expected=false;
        if (!stop_flag.compare_exchange_strong(expected,true,
                memory_order_release,memory_order_relaxed)) return;
        int n;
        {
            lock_guard<mutex> lk(workers_m);
            n=(int)workers.size(); // fix: was workers_.size() (trailing underscore)
            for (auto& w:workers){
                {lock_guard<mutex> cm(w->cv_m);}
                w->cv.notify_all();
            }
        }
        for (int i=0;i<n;i++)
            if (workers[i]->t.joinable()) workers[i]->t.join();
    }

    ~DynamicThreadPool() { shutdown(); }
    DynamicThreadPool(const DynamicThreadPool&)=delete;
    DynamicThreadPool& operator=(const DynamicThreadPool&)=delete;
    int active_workers() const { return active_count.load(memory_order_relaxed); }
};

class TimerThread {
    struct Entry {
        chrono::steady_clock::time_point deadline;
        function<void()> work;
        Priority         priority;
        string           name;
        bool operator>(const Entry& o) const { return deadline>o.deadline; }
    };

    priority_queue<Entry,vector<Entry>,greater<Entry>> queue_;
    mutex              m_;
    condition_variable cv_;
    atomic<bool>       stop_{false};
    DynamicThreadPool&        pool_;
    thread             t_;

    void loop() {
        while (true){
            unique_lock<mutex> lock(m_);
            if (queue_.empty()){
                cv_.wait(lock,[&]{return stop_.load()||!queue_.empty();});
            } else {
                auto next=queue_.top().deadline;
                cv_.wait_until(lock,next,[&]{
                    return stop_.load()||
                           (!queue_.empty()&&queue_.top().deadline<=chrono::steady_clock::now());
                });
            }
            if (stop_.load()&&queue_.empty()) return; // fix: was stop_ (already ok here)

            auto now=chrono::steady_clock::now();
            while (!queue_.empty()&&queue_.top().deadline<=now){
                Entry e=queue_.top(); queue_.pop();
                lock.unlock();
                cout<<"  [timer] '"<<e.name<<"' firing\n";
                try{ pool_.submit_cancellable(e.work,e.priority); } catch(...){}
                lock.lock();
                now=chrono::steady_clock::now();
            }
        }
    }

public:
    explicit TimerThread(DynamicThreadPool& p):pool_(p){t_=thread(&TimerThread::loop,this);}

    void schedule_after(chrono::milliseconds delay, function<void()> work,
                        string name="timer_task", Priority priority=Priority::NORMAL)
    {
        { lock_guard<mutex> lk(m_); // fix: was lk(m_) already correct; original had lk(m_) with wrong name
          queue_.push({chrono::steady_clock::now()+delay,move(work),priority,move(name)}); }
        cv_.notify_one();
    }

    void schedule_at(chrono::steady_clock::time_point dl, function<void()> work,
                     string name="timed_task", Priority priority=Priority::NORMAL)
    {
        { lock_guard<mutex> lk(m_);
          queue_.push({dl,move(work),priority,move(name)}); }
        cv_.notify_one();
    }

    void shutdown(){stop_.store(true,memory_order_release);cv_.notify_all();if(t_.joinable())t_.join();}
    ~TimerThread(){shutdown();}
};

struct TaskNode {                              // fix: was "truct" — missing 's'
    string   name;
    int      id=0;
    function<void()> work;
    Priority priority=Priority::NORMAL;

    CancellationTokenPtr token=make_token();

    atomic<int>       dep_count{0};
    vector<TaskNode*> dependents;

    enum class State { PENDING, READY, RUNNING, DONE, CANCELLED };
    atomic<State> state{State::PENDING};

    shared_future<void> completion_future;
private:
    promise<void> completion_promise_;        // fix: was "promise" — clashes with std::promise type name
public:
    TaskNode(string n, function<void()> w, Priority p=Priority::NORMAL)
        :name(move(n)),work(move(w)),priority(p)
    { completion_future=completion_promise_.get_future().share(); }

    void fulfill(exception_ptr ex=nullptr){
        if (ex) completion_promise_.set_exception(ex); // fix: was promise_.xxx (member renamed)
        else    completion_promise_.set_value();
    }

    TaskNode(const TaskNode&)=delete;
    TaskNode& operator=(const TaskNode&)=delete;
};

class DAGScheduler {
    DynamicThreadPool&                  pool_;
    vector<unique_ptr<TaskNode>> nodes_;       // fix: was "nodes" (no underscore) throughout
    mutex                        graph_m_;     // fix: was "graph_m" (no underscore) in add_task
    atomic<int>                  node_id_{0};
    atomic<int>                  pending_{0};
    mutex                        wait_m_;
    condition_variable           all_done_cv_;

    bool validate() {
        enum class C{WHITE,GRAY,BLACK};
        map<TaskNode*,C> color;
        for (auto& n:nodes_) color[n.get()]=C::WHITE;
        function<bool(TaskNode*)> dfs=[&](TaskNode* n)->bool{
            color[n]=C::GRAY;
            for (TaskNode* d:n->dependents){
                if (color[d]==C::GRAY){cerr<<"❌ CYCLE: "<<n->name<<"→"<<d->name<<"\n";return false;}
                if (color[d]==C::WHITE&&!dfs(d)) return false;
            }
            color[n]=C::BLACK; return true;
        };
        for (auto& n:nodes_) if (color[n.get()]==C::WHITE&&!dfs(n.get())) return false;
        return true;
    }

    void cascade_then_complete(TaskNode* node, bool cancelled) {
        for (TaskNode* dep:node->dependents){
            if (cancelled) dep->token->cancel();
            int old=dep->dep_count.fetch_sub(1,memory_order_acq_rel);
            if (old!=1) continue;
            if (dep->token->is_cancelled()){
                TaskNode::State expected=TaskNode::State::PENDING;
                if (dep->state.compare_exchange_strong(
                        expected,TaskNode::State::CANCELLED,memory_order_acq_rel)){
                    try{throw CancellationException{};}
                    catch(...){dep->fulfill(current_exception());}
                    cascade_then_complete(dep,true);
                }
            } else {
                activate(dep);
            }
        }
    }

    void activate(TaskNode* node) {
        TaskNode::State expected=TaskNode::State::PENDING;
        if (!node->state.compare_exchange_strong(
                expected,TaskNode::State::READY,memory_order_acq_rel)) return;
        pending_.fetch_add(1,memory_order_relaxed);
        pool_.submit_cancellable([this,node](){
            node->state.store(TaskNode::State::RUNNING,memory_order_relaxed);
            exception_ptr ex=nullptr;
            try{ node->token->throw_if_cancelled(); node->work(); }
            catch(...){ ex=current_exception(); }
            bool cancelled=node->token->is_cancelled();
            node->state.store(
                cancelled?TaskNode::State::CANCELLED:TaskNode::State::DONE,
                memory_order_relaxed);
            node->fulfill(ex);
            cascade_then_complete(node,cancelled);
            pending_.fetch_sub(1,memory_order_relaxed);
            all_done_cv_.notify_all();
        }, node->priority, node->token);
    }

public:
    explicit DAGScheduler(DynamicThreadPool& p):pool_(p){}

    TaskNode* add_task(string name, function<void()> work, Priority priority=Priority::NORMAL){
        lock_guard<mutex> lk(graph_m_);
        auto n=make_unique<TaskNode>(move(name),move(work),priority); // fix: was "p" (undefined var)
        n->id=node_id_.fetch_add(1,memory_order_relaxed);
        nodes_.push_back(move(n));
        return nodes_.back().get();
    }

    void add_dependency(TaskNode* dependent, TaskNode* dependency){
        lock_guard<mutex> lk(graph_m_);
        dependent->dep_count.fetch_add(1,memory_order_relaxed);
        dependency->dependents.push_back(dependent);
    }

    void execute(){
        if (!validate()) throw runtime_error("DAG contains a cycle");
        cout<<"  [DAG] "<<nodes_.size()<<" nodes\n";
        for (auto& n:nodes_)
            if (n->dep_count.load(memory_order_relaxed)==0)
                {cout<<"  [DAG] root: "<<n->name<<"\n";activate(n.get());}
    }

    void wait_all(){
        unique_lock<mutex> lock(wait_m_);
        all_done_cv_.wait(lock,[this]{return pending_.load(memory_order_relaxed)==0;});
    }

    void wait(TaskNode* node){
        try{node->completion_future.get();}
        catch(const CancellationException&){}
    }

    void print_graph() const {
        cout<<"  DAG structure:\n";
        for (auto& n:nodes_){
            cout<<"    ["<<n->name<<"]";
            if (!n->dependents.empty()){
                cout<<" → ";
                for (int i=0;i<(int)n->dependents.size();i++){
                    if (i) cout<<", ";
                    cout<<n->dependents[i]->name;
                }
            }
            cout<<"\n";
        }
    }
};

// ═══════════════════════════════════════════════════════════════════
//  TESTS
// ═══════════════════════════════════════════════════════════════════

void test_correctness(){
    cout<<"\n╔══════════════════════════════════════╗\n"
        <<  "║  TEST: Correctness (50k tasks)       ║\n"
        <<  "╚══════════════════════════════════════╝\n";
    DynamicThreadPool pool(4,8,30,4);
    const int N=50000;
    atomic<int>counter{0};
    vector<future<void>>futs;futs.reserve(N);
    auto t0=chrono::steady_clock::now();
    for(int i=0;i<N;i++)
        futs.push_back(pool.submit([&counter](){
            counter.fetch_add(1,memory_order_relaxed);}));
    for(auto&f:futs)f.get();
    auto ms=chrono::duration_cast<chrono::milliseconds>(
        chrono::steady_clock::now()-t0).count();
    assert(counter.load()==N);
    pool.stats.verify();
    cout<<"  Time: "<<ms<<"ms";
    if(ms>0)cout<<"  ("<<N*1000/ms<<" tasks/sec)";
    cout<<"\n";
    pool.shutdown();
    pool.stats.print();
    pool.stats.analyze_utilization();
}

void test_dynamic_scaling(){
    cout<<"\n╔══════════════════════════════════════╗\n"
        <<  "║  TOPIC 1: Dynamic Scaling            ║\n"
        <<  "╚══════════════════════════════════════╝\n";
    DynamicThreadPool pool(/*min*/2,/*max*/6,/*idle_sec*/2,/*thr*/2);
    cout<<"  Initial active workers: "<<pool.active_workers()<<"\n";
    vector<future<void>>futs;
    for(int i=0;i<30;i++)
        futs.push_back(pool.submit([](){
            this_thread::sleep_for(chrono::milliseconds(80));}));
    this_thread::sleep_for(chrono::milliseconds(200));
    cout<<"  Active workers during burst: "<<pool.active_workers()<<"\n";
    for(auto&f:futs)f.get();
    cout<<"  All done. Waiting for idle timeout (scale-down)...\n";
    this_thread::sleep_for(chrono::seconds(3));
    cout<<"  Active workers after idle: "<<pool.active_workers()<<" (min=2)\n";
    pool.shutdown();
}

void test_cancel_before_run(){
    cout<<"\n╔══════════════════════════════════════╗\n"
        <<  "║  TOPIC 2: Cancel Before Run          ║\n"
        <<  "╚══════════════════════════════════════╝\n";
    DynamicThreadPool pool(2,4,30,4);
    mutex bm; condition_variable bcv; bool unblock=false;
    vector<future<void>>blockers;
    for(int i=0;i<4;i++)
        blockers.push_back(pool.submit([&](){
            unique_lock<mutex>lk(bm);
            bcv.wait(lk,[&]{return unblock;});}));
    auto token=make_token();
    auto handle=pool.submit_cancellable([](){
        cout<<"  target task ran - should NOT happen!\n";
    },Priority::NORMAL,token);
    cout<<"  cancel()="<<handle.cancel()<<"\n";
    {lock_guard<mutex>lk(bm);unblock=true;}
    bcv.notify_all();
    for(auto&f:blockers)f.get();
    handle.wait();
    cout<<"  is_cancelled()="<<handle.is_cancelled()<<"\n";
    pool.shutdown();
}

void test_cancel_mid_run(){
    cout<<"\n╔══════════════════════════════════════╗\n"
        <<  "║  TOPIC 2: Cancel Mid-Run             ║\n"
        <<  "╚══════════════════════════════════════╝\n";
    DynamicThreadPool pool(2,4,30,4);
    auto token=make_token();
    atomic<int>iters{0};
    auto handle=pool.submit_cancellable([&,token](){
        for(int i=0;i<10000;i++){
            iters.fetch_add(1,memory_order_relaxed);
            this_thread::sleep_for(chrono::microseconds(100));
            token->throw_if_cancelled();
        }
        cout<<"  ran to completion\n";
    },Priority::NORMAL,token);
    this_thread::sleep_for(chrono::milliseconds(50));
    cout<<"  Cancelling after ~"<<iters.load()<<" iterations\n";
    handle.cancel();
    handle.wait();
    cout<<"  Stopped at iteration "<<iters.load()<<"\n";
    cout<<"  is_cancelled()="<<handle.is_cancelled()<<"\n";
    pool.shutdown();
}

void test_timer_basic(){
    cout<<"\n╔══════════════════════════════════════╗\n"
        <<  "║  TOPIC 3: Delayed Scheduling         ║\n"
        <<  "╚══════════════════════════════════════╝\n";
    DynamicThreadPool pool(2,4,30,4);
    TimerThread timer(pool);
    auto start=chrono::steady_clock::now();
    auto ms=[&](){ return chrono::duration_cast<chrono::milliseconds>(
        chrono::steady_clock::now()-start).count(); };
    mutex done_m; condition_variable done_cv; int done_count=0;
    timer.schedule_after(chrono::milliseconds(100),
        [&](){cout<<"  [+"<<ms()<<"ms] A fired\n";
              {lock_guard<mutex>lk(done_m);done_count++;}done_cv.notify_one();},"A");
    timer.schedule_after(chrono::milliseconds(250),
        [&](){cout<<"  [+"<<ms()<<"ms] B fired\n";
              {lock_guard<mutex>lk(done_m);done_count++;}done_cv.notify_one();},"B");
    timer.schedule_after(chrono::milliseconds(400),
        [&](){cout<<"  [+"<<ms()<<"ms] C fired\n";
              {lock_guard<mutex>lk(done_m);done_count++;}done_cv.notify_one();},"C");
    {
        unique_lock<mutex>lk(done_m);
        done_cv.wait_for(lk,chrono::seconds(2),[&]{return done_count>=3;});
    }
    cout<<"  Expected: A~100ms  B~250ms  C~400ms\n";
    timer.shutdown();
    pool.shutdown();
}

void test_timer_cancellation_timeout(){
    cout<<"\n╔══════════════════════════════════════╗\n"
        <<  "║  TOPIC 3+2: Timer Cancels Slow Task  ║\n"
        <<  "╚══════════════════════════════════════╝\n";
    DynamicThreadPool pool(2,4,30,4);
    TimerThread timer(pool);
    auto token=make_token();
    auto handle=pool.submit_cancellable([token](){
        for(int i=0;i<1000;i++){
            this_thread::sleep_for(chrono::milliseconds(1));
            token->throw_if_cancelled();
        }
        cout<<"  slow task completed - should be cancelled!\n";
    },Priority::NORMAL,token);
    timer.schedule_after(chrono::milliseconds(100),
        [token](){ cout<<"  [timer] timeout! cancelling slow task\n"; token->cancel(); },"timeout");
    handle.wait();
    cout<<"  cancelled="<<handle.is_cancelled()<<"\n";
    timer.shutdown();
    pool.shutdown();
}

void test_dag_linear(){
    cout<<"\n╔══════════════════════════════════════╗\n"
        <<  "║  DAG TEST: Linear A->B->C            ║\n"
        <<  "╚══════════════════════════════════════╝\n";
    DynamicThreadPool pool(4,8,30,4);
    DAGScheduler dag(pool);
    atomic<int>counter{0}; vector<int>order; mutex om;
    auto A=dag.add_task("A",[&](){ this_thread::sleep_for(chrono::milliseconds(10));
        lock_guard<mutex>lk(om);order.push_back(counter.fetch_add(1,memory_order_relaxed));});
    auto B=dag.add_task("B",[&](){
        lock_guard<mutex>lk(om);order.push_back(counter.fetch_add(1,memory_order_relaxed));});
    auto C=dag.add_task("C",[&](){
        lock_guard<mutex>lk(om);order.push_back(counter.fetch_add(1,memory_order_relaxed));});
    dag.add_dependency(B,A); dag.add_dependency(C,B);
    dag.print_graph(); dag.execute(); dag.wait_all();
    assert(order==vector<int>({0,1,2}));
    cout<<"  A->B->C executed in correct order\n";
    pool.shutdown();
}

void test_dag_diamond(){
    cout<<"\n╔══════════════════════════════════════╗\n"
        <<  "║  DAG TEST: Diamond A->B,C->D         ║\n"
        <<  "╚══════════════════════════════════════╝\n";
    DynamicThreadPool pool(4,8,30,4);
    DAGScheduler dag(pool);
    atomic<bool>a_done{false},b_done{false},c_done{false};
    atomic<int> d_runs{0};
    auto A=dag.add_task("A",[&](){this_thread::sleep_for(chrono::milliseconds(20));a_done=true;},Priority::HIGH);
    auto B=dag.add_task("B",[&](){assert(a_done);this_thread::sleep_for(chrono::milliseconds(10));b_done=true;});
    auto C=dag.add_task("C",[&](){assert(a_done);this_thread::sleep_for(chrono::milliseconds(15));c_done=true;});
    auto D=dag.add_task("D",[&](){assert(b_done&&c_done);d_runs.fetch_add(1,memory_order_relaxed);},Priority::HIGH);
    dag.add_dependency(B,A); dag.add_dependency(C,A);
    dag.add_dependency(D,B); dag.add_dependency(D,C);
    dag.print_graph(); dag.execute(); dag.wait_all();
    assert(d_runs.load()==1);
    cout<<"  D ran exactly "<<d_runs.load()<<" time (not duplicated)\n";
    pool.shutdown();
}

void test_dag_cancellation_cascade(){
    cout<<"\n╔══════════════════════════════════════╗\n"
        <<  "║  DAG TEST: Cancellation Cascade      ║\n"
        <<  "╚══════════════════════════════════════╝\n";
    cout<<"  Graph: A->B->D and A->C->D\n"
        <<"  Cancel C; D should be cascade-cancelled\n\n";
    DynamicThreadPool pool(4,8,30,4);
    DAGScheduler dag(pool);
    atomic<bool>d_ran{false};
    auto A=dag.add_task("A",[](){ this_thread::sleep_for(chrono::milliseconds(10)); });
    auto B=dag.add_task("B",[](){ this_thread::sleep_for(chrono::milliseconds(10)); });
    auto C=dag.add_task("C",[](){ this_thread::sleep_for(chrono::milliseconds(50)); });
    auto D=dag.add_task("D",[&d_ran](){ d_ran=true; cout<<"  D ran!\n"; });
    dag.add_dependency(B,A); dag.add_dependency(C,A);
    dag.add_dependency(D,B); dag.add_dependency(D,C);
    dag.print_graph();
    DynamicThreadPool helper(1,2,30,4);
    auto hf=helper.submit([&,C](){
        this_thread::sleep_for(chrono::milliseconds(30));
        cout<<"  [test] cancelling node C\n";
        C->token->cancel();
    });
    dag.execute(); hf.get(); dag.wait_all();
    cout<<"  d_ran="<<d_ran.load()<<" (expected false)\n";
    cout<<"  C.state="<<(C->state.load()==TaskNode::State::CANCELLED?"CANCELLED":"other")<<"\n";
    cout<<"  D.state="<<(D->state.load()==TaskNode::State::CANCELLED?"CANCELLED":"other")<<"\n";
    assert(!d_ran.load());
    helper.shutdown(); pool.shutdown();
}

// ═══════════════════════════════════════════════════════════════════
//  MAIN
// ═══════════════════════════════════════════════════════════════════

int main(){
    cout<<"╔══════════════════════════════════════╗\n"
        <<"║  SCHEDULER: 4-FEATURE UPGRADE        ║\n"
        <<"║  1. Dynamic Thread Management        ║\n"
        <<"║  2. Task Cancellation System         ║\n"
        <<"║  3. Delayed / Timed Scheduling       ║\n"
        <<"║  4. Cancellation + DAG Dependencies  ║\n"
        <<"╚══════════════════════════════════════╝\n";

    test_correctness();

    cout<<"\n--- TOPIC 1: DYNAMIC THREAD MANAGEMENT ---\n";
    test_dynamic_scaling();

    cout<<"\n--- TOPIC 2: TASK CANCELLATION ---\n";
    test_cancel_before_run();
    test_cancel_mid_run();

    cout<<"\n--- TOPIC 3: TIMED SCHEDULING ---\n";
    test_timer_basic();
    test_timer_cancellation_timeout();

    cout<<"\n--- TOPIC 4: DAG + CANCELLATION CASCADE ---\n";
    test_dag_linear();
    test_dag_diamond();
    test_dag_cancellation_cascade();

    cout<<"\nAll tests complete\n";
    return 0;
}