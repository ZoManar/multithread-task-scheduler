#include <iostream>
#include <iomanip>
#include <thread>
#include <vector>
#include <mutex>
#include <deque>
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
using namespace std;

// _____PRIORITY______________________________________________________________________

enum class Priority {
    LOW = 0,
    NORMAL = 1,
    HIGH = 2
};

// _____TASK__________________________________________________________________________

struct Task {
    Priority priority = Priority::NORMAL;
    int arrival_sequence = 0;
    function<void()> work;
    chrono::steady_clock::time_point submit_time = chrono::steady_clock::now(); // for latency measurement
};

// ___CACHE LINE SIZE___________________________________________________________

constexpr size_t CACHE_LINE_SIZE = 64;

//___SCHEDULER STATS___________________________________________________________________

struct SchedulerStats {

    // pool-level
    alignas(CACHE_LINE_SIZE) atomic<long> tasks_submitted{0};
    alignas(CACHE_LINE_SIZE) atomic<long> tasks_executed{0};
    alignas(CACHE_LINE_SIZE) atomic<long> tasks_stolen{0};
    // timing
    alignas(CACHE_LINE_SIZE) atomic<long> total_latency_us{0}; // from submission until execution start
    alignas(CACHE_LINE_SIZE) atomic<long> min_latency_us{LONG_MAX};
    alignas(CACHE_LINE_SIZE) atomic<long> max_latency_us{0};

    // per-worker
    struct alignas(CACHE_LINE_SIZE) WorkerStats{
        atomic<long> tasks_executed{0};
        atomic<long> tasks_stolen{0};
        atomic<long> times_idle{0};
        atomic<long> idle_time_us{0};
    };
    vector<WorkerStats> workers;

    explicit SchedulerStats(int num_workers)
        : workers(num_workers){}

    void record_latency(long us) {
        total_latency_us.fetch_add(us);
        long cur_min = min_latency_us.load();
        while (us < cur_min && !min_latency_us.compare_exchange_weak(cur_min, us)){}
        long cur_max = max_latency_us.load();
        while (us > cur_max && !max_latency_us.compare_exchange_weak(cur_max, us)){}
    }

    void print() const{
        long submitted = tasks_submitted.load();
        long executed = tasks_executed.load();
        long stolen = tasks_stolen.load();
        cout << "\n╔══════════════════════════════════════╗\n";
        cout <<   "║        SCHEDULER STATISTICS          ║\n";
        cout <<   "╠══════════════════════════════════════╣\n";
        cout <<   "║  Submitted:   " << setw(10) << submitted   << "           ║\n";
        cout <<   "║  Executed:    " << setw(10) << executed    << "           ║\n";
        cout <<   "║  Stolen:      " << setw(10) << stolen      << "           ║\n";

        if (executed > 0 ) {
            cout << "║  Steal rate:  " << setw(9)
                 << (stolen * 100 / executed) << "%           ║\n";
            cout << "║  Avg latency: " << setw(8)
                 << (total_latency_us / executed) << "us           ║\n";
        }
        if (min_latency_us.load() != LONG_MAX) {
            cout << "║  Min latency: " << setw(8)
                 << min_latency_us.load() << "us           ║\n";
            cout << "║  Max latency: " << setw(8)
                 << max_latency_us.load() << "us           ║\n";
        }

        cout << "╠══════════════════════════════════════╣\n";
        cout << "║  Per-Worker Breakdown:               ║\n";
        for (int i = 0; i < (int)workers.size(); i++) {
            cout << "║   W" << i
                 << ": exec=" << setw(6) << workers[i].tasks_executed
                 << "  stolen=" << setw(6) << workers[i].tasks_stolen
                 << "  idle=" << setw(4) << workers[i].times_idle
                 << "  ║\n";
        }
        cout << "╚══════════════════════════════════════╝\n";
    }

    void verify() const {
        long submitted = tasks_submitted.load();
        long executed = tasks_executed.load();
        if (submitted != executed) {
            cerr << "LOST TASKS: submitted=" << submitted
                << "executed=" << executed << "\n";
        }else {
            cout << "NO LOST TASKS: submitted=" << submitted
                << "executed=" << executed << "\n";
        }
    }
    

    void analyze_utilization() const {
        cout << "\n╔══════════════════════════════════════╗\n";
        cout <<   "║     WORKER UTILIZATION ANALYSIS      ║\n";
        cout <<   "╚══════════════════════════════════════╝\n";

        long total_executed = 0;
        vector<long> per_worker;
        for (auto& w : workers) {
            per_worker.push_back(w.tasks_executed.load());
            total_executed += per_worker.back();
        }

        int n = (int)per_worker.size();
        if (n == 0 || total_executed == 0) {
            cout << "  (no tasks executed yet)\n";
            return;
        }

        double ideal_share = (double)total_executed / n;
        cout << "  Ideal share per worker: "
             << fixed << setprecision(1) << ideal_share << " tasks\n\n";

        double max_deviation = 0;
        for (int i = 0; i < n; i++) {
            double deviation = fabs(per_worker[i] - ideal_share)
                                / ideal_share * 100.0;
            max_deviation = max(max_deviation, deviation);

            string verdict = deviation < 10 ? "balanced  " :
                              deviation < 30 ? "uneven    " :
                                               "imbalanced";

            cout << "  W" << i << ": " << setw(8) << per_worker[i]
                 << " executed  | " << setw(8)
                 << workers[i].tasks_stolen.load() << " stolen  | "
                 << verdict << " (" << setprecision(1)
                 << deviation << "% dev)\n";
        }

        cout << "\n  Max deviation: " << max_deviation << "%\n";

        double steal_pct = (double)tasks_stolen.load()
                            / total_executed * 100.0;
        cout << "  Overall steal rate: " << steal_pct << "%\n";

        if (max_deviation < 15 && steal_pct < 30) {
            cout << "  Load well-distributed, minimal steal overhead\n";
        } else if (max_deviation > 30) {
            cout << "  Significant imbalance, investigate"
                 << " distribution/stealing logic\n";
        } else {
            cout << "  Moderate imbalance, within acceptable range\n";
        }
    }
};


// ___BUCKETED PRIORITY DEQUEUE___________________________________________________________

class BucketedPriorityDeque{
private:
    // one deque per priority level (indexed by priorities, low=0, normal=1, high=2)
    array<deque<Task>, 3> buckets; // [LOW=0][NORMAL=1][HIGH=2]
    mutable mutex m;
    alignas(CACHE_LINE_SIZE) atomic<int> approx_size{0}; // updated OUTSIDE the lock

    static int idx(Priority p) {
        return static_cast<int>(p);
    }

public:
    // append to correct bucket O(1)
    void push(Task task){
        {
            lock_guard<mutex> lock(m);
            buckets[idx(task.priority)].push_back(std::move(task));
        }
        approx_size.fetch_add(1);   // updated OUTSIDE the lock
    }

    optional<Task> pop_back() {
        optional<Task> result;
        {
            lock_guard<mutex> lock(m);
            // owner pops highest priority from back
            for (int i = 2; i >= 0; i--) {
                if (!buckets[i].empty()) {
                    result = std::move(buckets[i].back());
                    buckets[i].pop_back();
                    break;
                }
            }
        }
        if (result) approx_size.fetch_sub(1);
        return result;
    }

    // owner pops up to max_count tasks in a single lick acquisition
    vector<Task> pop_batch(int max_count) {
        vector<Task> batch;
        batch.reserve(max_count);
        {
            lock_guard<mutex> lock(m);
            for (int i = 2; i >= 0 && (int)batch.size() < max_count; i--) {
                while (!buckets[i].empty() && (int)batch.size() < max_count) {
                    batch.push_back(std::move(buckets[i].back()));
                    buckets[i].pop_back();
                }
            }
        }
        if (!batch.empty())
            approx_size.fetch_sub((int)batch.size());
        return batch;
    }

    // thief steals lowest priority from FRONT — O(1)
    optional<Task> steal_front() {
        optional<Task> result;
        {
            lock_guard<mutex> lock(m);
            for (int i = 0; i <= 2; i++){
                if (!buckets[i].empty()){
                    result = std::move(buckets[i].front());
                    buckets[i].pop_front();
                    break;
                }
            }
        }
        if (result) approx_size.fetch_sub(1);
        return result;
    }
    
    bool empty() const{
        lock_guard<mutex> lock(m);
        for (auto& b : buckets)
            if (!b.empty()) return false;
        return true;
    }

    int size_approx() const {
        return approx_size.load();
    }
};


class ThreadPool {
private:

    // ── Worker ───────────────────────────────────────────────────
    struct Worker{
    // each worker owns its deque and knows about all other workers
        BucketedPriorityDeque deque;
        thread t;
        mutex cv_m;
        condition_variable cv;  // per-worker sleep/wake
        int id = 0;
        atomic<bool> is_idle{false}; // true when sleeping
    };
    
    vector<unique_ptr<Worker>> workers;

    alignas(CACHE_LINE_SIZE) atomic<bool> stop_flag{false};  
    alignas(CACHE_LINE_SIZE) atomic<int> next_worker{0};     // round-robin counter 
    alignas(CACHE_LINE_SIZE) atomic<int> id_generator{0};
    alignas(CACHE_LINE_SIZE) atomic<int> total_tasks{0};     // tasks across ALL queues

    static constexpr int BATCH_SIZE = 8;

public:
    SchedulerStats stats;

private:

    // ── stealing ─────────────────────────────────────────────────
    optional<Task> try_steal(int my_id){
        int n = (int)workers.size();
        
        // first try steal from busiest
        int busiest_id = -1;
        size_t max_size = 0;
        for (int i = 0; i < n; i++){
            if (i == my_id) continue;
            int s = workers[i]->deque.size_approx();
            if (s > max_size) {
                max_size = s; 
                busiest_id = i;
            }
        }    

        if (busiest_id >= 0 && max_size > 0){
            auto task = workers[busiest_id]->deque.steal_front();
            if (task) {
                total_tasks.fetch_sub(1);
                return task;
            }
        }
        // random sweep if busiest attempt failed
        for (int i = 1; i < n; i++) {
            int target = (my_id + i) % n;
            auto task = workers[target]->deque.steal_front();  // decrement on steal
            if (task) {
                total_tasks.fetch_sub(1);
                return task;
            } 
        }
        return std::nullopt;
    }

    // ── notify idle workers ──────────────────────────────────────
    // called when a task arrives but target worker is busy
    void notify_one_idle_worker(int exclude_id){
        for (auto& w : workers) {
            if (w->id == exclude_id) continue;
            if (w->is_idle.load()){
                w->cv.notify_one();
                return;
            }
        }
    }

    // ── task execution ───────────────────────────────────────────
    void run_task(int my_id, Task task) {
        auto now    = chrono::steady_clock::now();
        auto latency = chrono::duration_cast<chrono::microseconds> 
                        (now-task.submit_time).count();
        stats.record_latency(latency);

        try {
                task.work();
            } catch (const exception& e) {
                cerr << "Worker " << my_id
                    << " exception: " << e.what() << "\n";
            } catch (...) {
                cerr << "Worker " << my_id
                    << " unknown exception\n";
            }
    }

    // ── worker loop ──────────────────────────────────────────────
    void worker_loop(int my_id){
        Worker& me = *workers[my_id];

        while (true){

            // ── drain own queue in BATCHES -one lock──────────────
            while (true)
            {
                auto batch = me.deque.pop_batch(BATCH_SIZE);
                if (batch.empty()) break;

                for (auto& task : batch) {
                    total_tasks.fetch_sub(1);
                    stats.tasks_executed.fetch_add(1);
                    stats.workers[my_id].tasks_executed.fetch_add(1);
                    run_task(my_id, task);
                }
            }
            
            // ── try stealing ─────────────────────────────
            {
                // try steal from busiest worker
                auto task = try_steal(my_id);
                if(task){
                    stats.tasks_executed.fetch_add(1);
                    stats.tasks_stolen.fetch_add(1);
                    stats.workers[my_id].tasks_executed.fetch_add(1);
                    stats.workers[my_id].tasks_stolen.fetch_add(1);
                    run_task(my_id, *task);
                    continue;
                }
            }

            // ── check shutdown before sleeping ───────────
            if (stop_flag.load() && total_tasks.load()==0){
                // drain any remaining tasks before exit
                while (true)
                {
                    auto task = try_steal(my_id);
                    if (!task) break;
                    stats.tasks_executed.fetch_add(1);
                    stats.tasks_stolen.fetch_add(1);
                    stats.workers[my_id].tasks_executed.fetch_add(1);
                    stats.workers[my_id].tasks_stolen.fetch_add(1);
                    run_task(my_id, *task);
                }
                
                return;     // clean exit
            }

            // ── go idle — event-driven sleep ────────────
            auto idle_start = chrono::steady_clock::now();
            {
                unique_lock<mutex> lock(me.cv_m);
                me.is_idle.store(true);
                stats.workers[my_id].times_idle.fetch_add(1);

                me.cv.wait(lock, [&] {
                    return stop_flag.load() 
                        || !me.deque.empty()
                        || total_tasks.load() > 0;
                    });

                me.is_idle.store(false);
            }
            auto idle_us = chrono::duration_cast<chrono::microseconds>
                            (chrono::steady_clock::now()- idle_start).count();
            stats.workers[my_id].idle_time_us.fetch_add(idle_us);

            // ── re-check exit after wakeup ──────────────
            if (stop_flag.load() && total_tasks.load() == 0){
                return;
            }
        }
    }
        
public:
    explicit ThreadPool(int num_threads) 
        : stats(num_threads)

{
    workers.reserve(num_threads);
    for (int i = 0; i < num_threads; i++){
        workers.push_back(make_unique<Worker>());
        workers.back()->id = i;
        workers.back()->t = thread(
            &ThreadPool::worker_loop, this, i
        );
    }
}
    

    // ── Submit ───────────────────────────────────────────────────
    template<typename F> 
    auto submit(F task, Priority priority = Priority::NORMAL,int preferred_worker = -1) -> future<decltype(task())>{

        using ReturnType = decltype(task());

        if (stop_flag.load())
        {
            throw runtime_error("ThreadPool is stopped : submit() called after shutdown");
        }

        // Wrap task in a packaged_task
        // packaged_task handles: running task + storing result + exceptions
        auto pt = make_shared<packaged_task<ReturnType()>>(std::move(task));

        // Extract the future BEFORE moving pt into the queue
        future<ReturnType> result = pt -> get_future();


        int target = (preferred_worker >= 0 && preferred_worker < (int)workers.size())
            ? preferred_worker
            :next_worker.fetch_add(1) % workers.size();

        workers[target]->deque.push(Task{
            priority,
            id_generator.fetch_add(1),
            [pt](){ (*pt)(); },
            chrono::steady_clock::now()
        });
        total_tasks.fetch_add(1);
        stats.tasks_submitted.fetch_add(1);

        // notify target worker directly
        workers[target]->cv.notify_one();

        // if taret is busy, notify an idle worker to steal
        if (!workers[target]->is_idle.load()) {
            notify_one_idle_worker(target);
        }
        return result;
    }

    // graceful shutdown - first finish All queue tasks then exit
    void shutdown()
    {
        bool expected = false;
        if(!stop_flag.compare_exchange_strong(expected, true)) {
            return;
        }
      
        for (auto &w: workers){
            {
                lock_guard<mutex> lock(w->cv_m);
            }
            w->cv.notify_all();               
        }

        for(auto& w : workers)
            if (w->t.joinable()) w->t.join();
    }
    
    ~ThreadPool(){
        shutdown();
    }

    ThreadPool(const ThreadPool&) = delete;
    ThreadPool& operator=(const ThreadPool&) = delete;

    int num_workers() const {return (int)workers.size(); }

};

// ═══════════════════════════════════════════════════════════════════
//  CORRECTNESS TESTS  (must still pass after optimization)
// ═══════════════════════════════════════════════════════════════════

void test_no_lost_tasks() {
    cout << "\n╔══════════════════════════════════════╗\n";
    cout <<   "║  TEST: No Lost Tasks (100k)          ║\n";
    cout <<   "╚══════════════════════════════════════╝\n";

    ThreadPool pool(4);

    const int N = 100000;
    atomic<int> counter{0};
    vector<future<void>> futures;
    futures.reserve(N);

    auto start = chrono::steady_clock::now();

    for (int i = 0; i < N; i++) {
        futures.push_back(pool.submit([&counter]() {
            counter.fetch_add(1);
        }));
    }
    for (auto& f : futures) f.get();

    auto ms = chrono::duration_cast<chrono::milliseconds>
              (chrono::steady_clock::now() - start).count();

    assert(counter.load() == N);
    pool.stats.verify();
    cout << "Time: " << ms << "ms";
    if (ms > 0) cout << "  |  Throughput: " << (N * 1000 / ms) << " tasks/sec";
    cout << "\n";

    pool.shutdown();
    pool.stats.print();
    pool.stats.analyze_utilization();
}

void test_heavy_stealing() {
    cout << "\n╔══════════════════════════════════════╗\n";
    cout <<   "║  TEST: Heavy Stealing (imbalanced)   ║\n";
    cout <<   "╚══════════════════════════════════════╝\n";

    ThreadPool pool(4);

    const int N = 5000;
    atomic<int> counter{0};
    vector<future<void>> futures;
    futures.reserve(N);

    // all tasks go to Worker 0 — forces Workers 1,2,3 to steal
    for (int i = 0; i < N; i++) {
        futures.push_back(pool.submit(
            [&counter]() { counter.fetch_add(1); },
            Priority::NORMAL,
            0
        ));
    }
    for (auto& f : futures) f.get();

    assert(counter.load() == N);
    pool.stats.verify();

    pool.shutdown();
    pool.stats.print();
    pool.stats.analyze_utilization();
}

void test_exception_safety() {
    cout << "\n╔══════════════════════════════════════╗\n";
    cout <<   "║  TEST: Exception Safety              ║\n";
    cout <<   "╚══════════════════════════════════════╝\n";

    ThreadPool pool(4);

    const int N = 100;
    atomic<int> completed{0};
    atomic<int> exceptions_caught{0};
    vector<future<int>> futures;
    futures.reserve(N);

    for (int i = 0; i < N; i++) {
        futures.push_back(pool.submit([i, &completed]() -> int {
            if (i % 10 == 0)
                throw runtime_error("intentional error " + to_string(i));
            completed.fetch_add(1);
            return i;
        }));
    }

    for (int i = 0; i < N; i++) {
        try {
            futures[i].get();
        } catch (runtime_error&) {
            exceptions_caught.fetch_add(1);
        }
    }

    auto f = pool.submit([]() { return 42; });
    assert(f.get() == 42);

    cout << "Survived " << exceptions_caught << " exceptions, "
         << completed << " normal tasks completed, pool still alive\n";

    pool.shutdown();
}

// ═══════════════════════════════════════════════════════════════════
//  PERFORMANCE BENCHMARKS
// ═══════════════════════════════════════════════════════════════════

void benchmark_throughput_scaling() {
    cout << "\n╔══════════════════════════════════════╗\n";
    cout <<   "║  BENCHMARK: Throughput Scaling       ║\n";
    cout <<   "╚══════════════════════════════════════╝\n";

    const int N = 200000;

    for (int num_workers : {1, 2, 4, 8}) {
        ThreadPool pool(num_workers);

        atomic<int> counter{0};
        vector<future<void>> futures;
        futures.reserve(N);

        auto start = chrono::steady_clock::now();

        for (int i = 0; i < N; i++) {
            futures.push_back(pool.submit([&counter]() {
                counter.fetch_add(1);
            }));
        }
        for (auto& f : futures) f.get();

        auto ms = chrono::duration_cast<chrono::milliseconds>
                  (chrono::steady_clock::now() - start).count();

        assert(counter.load() == N);
        cout << "  Workers: " << setw(2) << num_workers
             << " | Time: " << setw(6) << ms << "ms";
        if (ms > 0)
            cout << " | Throughput: " << setw(10)
                 << (N * 1000 / ms) << " tasks/sec\n";
        else
            cout << " | <1ms (too fast to measure precisely)\n";

        pool.shutdown();
    }
}

void benchmark_steal_rate() {
    cout << "\n╔══════════════════════════════════════╗\n";
    cout <<   "║  BENCHMARK: Steal Rate               ║\n";
    cout <<   "╚══════════════════════════════════════╝\n";

    const int N = 20000;

    for (auto& scenario : vector<pair<string,int>>{
        {"balanced   (round-robin)", -1},
        {"imbalanced (all Worker 0)", 0}
    }) {
        ThreadPool pool(4);
        vector<future<void>> futures;
        futures.reserve(N);

        auto start = chrono::steady_clock::now();
        for (int i = 0; i < N; i++) {
            futures.push_back(pool.submit(
                [](){}, Priority::NORMAL, scenario.second
            ));
        }
        for (auto& f : futures) f.get();
        auto ms = chrono::duration_cast<chrono::milliseconds>
                  (chrono::steady_clock::now() - start).count();

        long executed = pool.stats.tasks_executed.load();
        long stolen   = pool.stats.tasks_stolen.load();

        cout << "  " << scenario.first
             << " | Time: " << setw(5) << ms << "ms"
             << " | Steal rate: "
             << (executed > 0 ? stolen * 100 / executed : 0) << "%\n";

        pool.shutdown();
    }
}

void benchmark_batch_vs_single() {
    cout << "\n╔══════════════════════════════════════╗\n";
    cout <<   "║  BENCHMARK: Batch Pop vs Single Pop  ║\n";
    cout <<   "╚══════════════════════════════════════╝\n";

    const int N = 200000;

    auto run = [&](int batch_size) {
        BucketedPriorityDeque deque;
        for (int i = 0; i < N; i++) {
            deque.push(Task{Priority::NORMAL, i, [](){}});
        }

        auto start = chrono::steady_clock::now();
        int popped = 0;
        if (batch_size == 1) {
            while (auto t = deque.pop_back()) popped++;
        } else {
            while (true) {
                auto batch = deque.pop_batch(batch_size);
                if (batch.empty()) break;
                popped += (int)batch.size();
            }
        }
        auto us = chrono::duration_cast<chrono::microseconds>
                  (chrono::steady_clock::now() - start).count();

        assert(popped == N);
        cout << "  batch_size=" << setw(2) << batch_size
             << " | " << N << " pops in " << us << "us"
             << " (" << fixed << setprecision(3)
             << (double)us / N << "us/task)\n";
    };

    run(1);
    run(4);
    run(8);
    run(32);
}

// ═══════════════════════════════════════════════════════════════════
//  MAIN
// ═══════════════════════════════════════════════════════════════════

int main() {
    cout << "╔══════════════════════════════════════╗\n";
    cout << "║   OPTIMIZED SCHEDULER TEST SUITE     ║\n";
    cout << "╚══════════════════════════════════════╝\n";

    cout << "\n--- CORRECTNESS (must still hold) ---\n";
    test_no_lost_tasks();
    test_heavy_stealing();
    test_exception_safety();

    cout << "\n--- PERFORMANCE BENCHMARKS ---\n";
    benchmark_throughput_scaling();
    benchmark_steal_rate();
    benchmark_batch_vs_single();

    cout << "\nAll tests and benchmarks complete\n";
    return 0;
}
/*
struct TaskNode{
    string name;
    int id = 0;
    function<void()> work;
    Priority priority = Priority::NORMAL;

    atomic<int> dep_count{0};        // unfinished dependencies
    vector<TaskNode*> dependents;   // tasks that depend on ME

    enum class State {PENDING, READY, RUNNING, DONE };
    atomic<State> state{State::PENDING};
    shared_future<void> completion_future;

private:
    promise<void> completion_promise;

public:
    TaskNode(string n, function<void()> w, Priority p = Priority::NORMAL)
        : name(std::move(n)) , work(std::move(w)) , priority(p)
        {
            completion_future = completion_promise.get_future().share();
        }

        void fulfill(exception_ptr ex = nullptr) {
            if (ex)
                completion_promise.set_exception(ex);
            else
                completion_promise.set_value();
        }
    
        TaskNode(const TaskNode&) = delete;
        TaskNode& operator=(const TaskNode&) = delete;
};

class DAGScheduler {
private:
    ThreadPool& pool;
    vector<unique_ptr<TaskNode>> nodes; 
    mutex graph_m;
    atomic<int> node_id_counter{0};
    atomic<int> pending_tasks{0};
    mutex wait_m;
    condition_variable all_done_cv;

    // ── Cycle Detection ──────────────────────────────────────────
    bool validate() {
        enum class Color {WHITE, GRAY, BLACK };
        map<TaskNode*, Color> color;
        for (auto& n : nodes) color[n.get()] = Color::WHITE;

        function<bool(TaskNode*)> dfs = [&](TaskNode* n ) -> bool {
            color[n] = Color::GRAY;
            for (TaskNode* dep : n->dependents) {
                if (color[dep] == Color::GRAY) {
                    cerr << "CYCLE: "
                         << n->name << " ──► " << dep->name << "\n";
                    return false;
                }
                
                if (color[dep] == Color::WHITE)
                    if (!dfs(dep)) return false;
            }
            color[n] = Color::BLACK;
            return true;
        };

        for (auto& n : nodes)
            if (color[n.get()] == Color::WHITE)
                if (!dfs(n.get())) return false;
        return true;
    }

    void activate(TaskNode* node) {
        // atomically PENDING → READY — only one thread succeeds
        TaskNode::State expected = TaskNode::State::PENDING;
        if (!node->state.compare_exchange_strong(
                expected, TaskNode::State::READY)) {
            return;  // another thread already activated it
        }
        
        pending_tasks.fetch_add(1);
        pool.submit([this, node]() {
            // mark RUNNING
            node->state.store(TaskNode::State::RUNNING);
            cout << "  Running: " << node->name << "\n";

            auto task_start = chrono::steady_clock::now();
            // execute work
            exception_ptr ex = nullptr;
            try {
                node->work();
            } catch (...) {
                ex = current_exception();
                cerr << " Exception in task: "
                     << node->name << "\n";
            }

            auto ms = chrono::duration_cast<chrono::milliseconds>
                      (chrono::steady_clock::now() - task_start).count();
            cout << "  Done:    " << node->name
                 << " (" << ms << "ms)\n";

            // mark DONE and fulfill promise
            node->state.store(TaskNode::State::DONE);
            node->fulfill(ex);

            // notify dependents — the cascade
            for (TaskNode* dependent : node->dependents) {
                // fetch_sub returns OLD value
                int old = dependent->dep_count.fetch_sub(1);
                if (old == 1) {
                    // WE are the last dependency — activate it
                    // exactly ONE thread ever sees old==1 ✅
                    activate(dependent);
                }
            }

            pending_tasks.fetch_sub(1);
            all_done_cv.notify_all();

        }, node->priority);
    }

public:
    explicit DAGScheduler(ThreadPool& p) : pool(p) {}
    // ── Add Task ─────────────────────────────────────────────────
    TaskNode* add_task(string name,
                       function<void()> work,
                       Priority priority = Priority::NORMAL)
    {
        lock_guard<mutex> lock(graph_m);
        auto node = make_unique<TaskNode>(
            std::move(name), std::move(work), priority
        );
        node->id = node_id_counter.fetch_add(1);
        nodes.push_back(std::move(node));
        return nodes.back().get();
    }
    
    // ── Add Dependency ───────────────────────────────────────────
    // "dependent cannot run until dependency finishes"
    // usage: add_dependency(B, A) means A ──► B
    void add_dependency(TaskNode* dependent, TaskNode* dependency) {
        lock_guard<mutex> lock(graph_m);
        dependent->dep_count.fetch_add(1);
        dependency->dependents.push_back(dependent);
    }

    // ── Execute Graph ────────────────────────────────────────────
    void execute() {
        if (!validate())
            throw runtime_error("DAG contains a cycle");

        cout << "\n Graph has " << nodes.size() << " tasks\n\n";

        // activate all root nodes (no dependencies)
        for (auto& node : nodes) {
            if (node->dep_count.load() == 0) {
                cout << "  Root: " << node->name << "\n";
                activate(node.get());
            }
        }
    }

    // ── Wait For All ─────────────────────────────────────────────
    void wait_all() {
        unique_lock<mutex> lock(wait_m);
        all_done_cv.wait(lock, [this] {
            return pending_tasks.load() == 0;
        });
    }

    // ── Wait For Specific Task ───────────────────────────────────
    void wait(TaskNode* node) {
        node->completion_future.get();
    }

    // ── Print Graph Structure ────────────────────────────────────
    void print_graph() {
        cout << "\n DAG Structure:\n";
        for (auto& node : nodes) {
            cout << "  [" << node->name << "]";
            if (!node->dependents.empty()) {
                cout << " ──► ";
                for (int i = 0; i < (int)node->dependents.size(); i++) {
                    if (i > 0) cout << ", ";
                    cout << node->dependents[i]->name;
                }
            }
            cout << "\n";
        }
    }
};

// ═══════════════════════════════════════════════════════════════════
//  TESTS
// ═══════════════════════════════════════════════════════════════════

// ── Test 1: Linear Chain A → B → C → D ──────────────────────────
void test_linear_chain() {
    cout << "\n╔══════════════════════════════════════╗\n";
    cout <<   "║  TEST 1: Linear Chain A→B→C→D       ║\n";
    cout <<   "╚══════════════════════════════════════╝\n";

    ThreadPool pool(4);
    DAGScheduler dag(pool);

    atomic<int> order{0};
    vector<int> execution_order;
    mutex order_m;

    auto A = dag.add_task("A", [&]() {
        this_thread::sleep_for(chrono::milliseconds(10));
        lock_guard<mutex> lock(order_m);
        execution_order.push_back(order.fetch_add(1));
    });
    auto B = dag.add_task("B", [&]() {
        lock_guard<mutex> lock(order_m);
        execution_order.push_back(order.fetch_add(1));
    });
    auto C = dag.add_task("C", [&]() {
        lock_guard<mutex> lock(order_m);
        execution_order.push_back(order.fetch_add(1));
    });
    auto D = dag.add_task("D", [&]() {
        lock_guard<mutex> lock(order_m);
        execution_order.push_back(order.fetch_add(1));
    });

    // A → B → C → D
    dag.add_dependency(B, A);
    dag.add_dependency(C, B);
    dag.add_dependency(D, C);

    dag.print_graph();
    dag.execute();
    dag.wait_all();

    // verify sequential ordering
    assert(execution_order == vector<int>({0,1,2,3}));
    cout << "✅ Linear chain executed in correct order\n";

    pool.shutdown();
}

// ── Test 2: Diamond A → B,C → D ─────────────────────────────────
void test_diamond() {
    cout << "\n╔══════════════════════════════════════╗\n";
    cout <<   "║  TEST 2: Diamond A→B,C→D            ║\n";
    cout <<   "╚══════════════════════════════════════╝\n";

    ThreadPool pool(4);
    DAGScheduler dag(pool);

    atomic<bool> a_done{false};
    atomic<bool> b_done{false};
    atomic<bool> c_done{false};
    atomic<int>  d_run_count{0};  // must be exactly 1

    auto A = dag.add_task("A", [&]() {
        this_thread::sleep_for(chrono::milliseconds(20));
        a_done.store(true);
    }, Priority::HIGH);

    auto B = dag.add_task("B", [&]() {
        assert(a_done.load()); // A must be done
        this_thread::sleep_for(chrono::milliseconds(10));
        b_done.store(true);
    });

    auto C = dag.add_task("C", [&]() {
        assert(a_done.load()); // A must be done
        this_thread::sleep_for(chrono::milliseconds(15));
        c_done.store(true);
    });

    auto D = dag.add_task("D", [&]() {
        assert(b_done.load()); // B must be done
        assert(c_done.load()); // C must be done
        d_run_count.fetch_add(1);
    }, Priority::HIGH);

    //   A
    //  / \
    // B   C
    //  \ /
    //   D
    dag.add_dependency(B, A);
    dag.add_dependency(C, A);
    dag.add_dependency(D, B);
    dag.add_dependency(D, C);

    dag.print_graph();
    dag.execute();
    dag.wait_all();

    assert(d_run_count.load() == 1);  // D runs EXACTLY once
    cout << "Diamond executed correctly\n";
    cout << "D ran exactly " << d_run_count << " time\n";

    pool.shutdown();
}

// ── Test 3: Wide Fan-out A → B,C,D,E,F ──────────────────────────
void test_fan_out() {
    cout << "\n╔══════════════════════════════════════╗\n";
    cout <<   "║  TEST 3: Fan-Out A→B,C,D,E,F        ║\n";
    cout <<   "╚══════════════════════════════════════╝\n";

    ThreadPool pool(4);
    DAGScheduler dag(pool);

    atomic<bool> a_done{false};
    atomic<int>  children_done{0};

    auto A = dag.add_task("A", [&]() {
        this_thread::sleep_for(chrono::milliseconds(10));
        a_done.store(true);
    }, Priority::HIGH);

    vector<TaskNode*> children;
    for (char c = 'B'; c <= 'F'; c++) {
        children.push_back(dag.add_task(string(1,c), [&]() {
            assert(a_done.load());  // A must be done
            children_done.fetch_add(1);
        }));
        dag.add_dependency(children.back(), A);
    }

    dag.print_graph();
    dag.execute();
    dag.wait_all();

    assert(children_done.load() == 5);
    cout << " All " << children_done
         << " children ran after A completed\n";

    pool.shutdown();
}

// ── Test 4: Complex Pipeline (like a build system) ───────────────
void test_build_pipeline() {
    cout << "\n╔══════════════════════════════════════╗\n";
    cout <<   "║  TEST 4: Build Pipeline              ║\n";
    cout <<   "╚══════════════════════════════════════╝\n";

    ThreadPool pool(4);
    DAGScheduler dag(pool);

    //  compile_A ──► link_A ──┐
    //  compile_B ──► link_B ──┼──► link_final ──► package
    //  compile_C ──► link_C ──┘

    auto compile_A = dag.add_task("compile_A", []{
        this_thread::sleep_for(chrono::milliseconds(20));
        cout << "    [compiled A.cpp]\n";
    });
    auto compile_B = dag.add_task("compile_B", []{
        this_thread::sleep_for(chrono::milliseconds(15));
        cout << "    [compiled B.cpp]\n";
    });
    auto compile_C = dag.add_task("compile_C", []{
        this_thread::sleep_for(chrono::milliseconds(25));
        cout << "    [compiled C.cpp]\n";
    });

    auto link_A = dag.add_task("link_A", []{
        cout << "    [linked A.o]\n";
    });
    auto link_B = dag.add_task("link_B", []{
        cout << "    [linked B.o]\n";
    });
    auto link_C = dag.add_task("link_C", []{
        cout << "    [linked C.o]\n";
    });

    auto link_final = dag.add_task("link_final", []{
        this_thread::sleep_for(chrono::milliseconds(10));
        cout << "    [linked final binary]\n";
    }, Priority::HIGH);

    auto package = dag.add_task("package", []{
        cout << "    [packaged release]\n";
    }, Priority::HIGH);

    // compile → link (per file)
    dag.add_dependency(link_A, compile_A);
    dag.add_dependency(link_B, compile_B);
    dag.add_dependency(link_C, compile_C);

    // all links → final link
    dag.add_dependency(link_final, link_A);
    dag.add_dependency(link_final, link_B);
    dag.add_dependency(link_final, link_C);

    // final link → package
    dag.add_dependency(package, link_final);

    dag.print_graph();

    auto start = chrono::steady_clock::now();
    dag.execute();
    dag.wait_all();
    auto ms = chrono::duration_cast<chrono::milliseconds>
              (chrono::steady_clock::now() - start).count();

    cout << "Build pipeline complete in " << ms << "ms\n";
    // compiles run in parallel — should be ~25ms not 60ms
    cout << "   (sequential would take ~60ms, parallel took "
         << ms << "ms)\n";

    pool.shutdown();
}

// ── Test 5: Cycle Detection ──────────────────────────────────────
void test_cycle_detection() {
    cout << "\n╔══════════════════════════════════════╗\n";
    cout <<   "║  TEST 5: Cycle Detection             ║\n";
    cout <<   "╚══════════════════════════════════════╝\n";

    ThreadPool pool(4);
    DAGScheduler dag(pool);

    auto A = dag.add_task("A", []{});
    auto B = dag.add_task("B", []{});
    auto C = dag.add_task("C", []{});

    // create a cycle: A → B → C → A
    dag.add_dependency(B, A);
    dag.add_dependency(C, B);
    dag.add_dependency(A, C);  // ← creates cycle

    try {
        dag.execute();
        cerr << "Should have thrown!\n";
    } catch (runtime_error& e) {
        cout << "Cycle correctly detected: "
             << e.what() << "\n";
    }

    pool.shutdown();
}
*/
/*
// ═══════════════════════════════════════════════════════════════════
//  MAIN
// ═══════════════════════════════════════════════════════════════════

int main() {
    cout << "╔══════════════════════════════════════╗\n";
    cout << "║      DAG SCHEDULER TEST SUITE        ║\n";
    cout << "╚══════════════════════════════════════╝\n";

    test_linear_chain();
    test_diamond();
    test_fan_out();
    test_build_pipeline();
    test_cycle_detection();

    cout << "\n✅ All DAG tests complete\n";
    return 0;
}
*/
/*

// ═══════════════════════════════════════════════════════════════════
//  PHASE 1 — CORRECTNESS TESTS
// ═══════════════════════════════════════════════════════════════════

void test_tiny_tasks() {
    cout << "\n╔══════════════════════════════════════╗\n";
    cout <<   "║  TEST 1: Tiny Tasks (100k)           ║\n";
    cout <<   "╚══════════════════════════════════════╝\n";

    ThreadPool pool(4);

    const int N = 100000;
    atomic<int> counter{0};
    vector<future<void>> futures;
    futures.reserve(N);

    auto start = chrono::steady_clock::now();

    for (int i = 0; i < N; i++) {
        futures.push_back(pool.submit([&counter]() {
            counter.fetch_add(1);
        }));
    }
    for (auto& f : futures) f.get();

    auto ms = chrono::duration_cast<chrono::milliseconds>
              (chrono::steady_clock::now() - start).count();

    assert(counter.load() == N);
    pool.stats.verify();
    cout << "⏱  Time:       " << ms << "ms\n";
    if (ms > 0)
        cout << "⚡ Throughput:  " << (N * 1000 / ms) << " tasks/sec\n";

    pool.shutdown();
    pool.stats.print();
}

// ── Test 2 — Long Tasks ──────────────────────────────────────────
void test_long_tasks() {
    cout << "\n╔══════════════════════════════════════╗\n";
    cout <<   "║  TEST 2: Long Tasks                  ║\n";
    cout <<   "╚══════════════════════════════════════╝\n";

    ThreadPool pool(4);

    const int N = 16;
    atomic<int> counter{0};
    vector<future<void>> futures;

    auto start = chrono::steady_clock::now();

    for (int i = 0; i < N; i++) {
        futures.push_back(pool.submit([&counter, i]() {
            this_thread::sleep_for(
                chrono::milliseconds(50 + (i % 4) * 10)
            );
            counter.fetch_add(1);
        }));
    }
    for (auto& f : futures) f.get();

    auto ms = chrono::duration_cast<chrono::milliseconds>
              (chrono::steady_clock::now() - start).count();

    assert(counter.load() == N);
    pool.stats.verify();
    cout << "⏱  Total time: " << ms << "ms\n";

    pool.shutdown();
    pool.stats.print();
}

// ── Test 3 — Heavy Stealing ──────────────────────────────────────
void test_heavy_stealing() {
    cout << "\n╔══════════════════════════════════════╗\n";
    cout <<   "║  TEST 3: Heavy Stealing              ║\n";
    cout <<   "╚══════════════════════════════════════╝\n";

    ThreadPool pool(4);

    const int N = 1000;
    atomic<int> counter{0};
    vector<future<void>> futures;
    futures.reserve(N);

    // all tasks go to Worker 0 — forces Workers 1,2,3 to steal
    for (int i = 0; i < N; i++) {
        futures.push_back(pool.submit(
            [&counter]() { counter.fetch_add(1); },
            Priority::NORMAL,
            0   // ← all to Worker 0
        ));
    }
    for (auto& f : futures) f.get();

    assert(counter.load() == N);
    pool.stats.verify();

    pool.shutdown();
    pool.stats.print();
}

// ── Test 4 — Mixed Workload ──────────────────────────────────────
void test_mixed_workload() {
    cout << "\n╔══════════════════════════════════════╗\n";
    cout <<   "║  TEST 4: Mixed Workload              ║\n";
    cout <<   "╚══════════════════════════════════════╝\n";

    ThreadPool pool(4);

    const int N = 200;
    atomic<int> high_completed{0};
    atomic<int> low_completed{0};
    vector<future<void>> futures;
    futures.reserve(N);

    // submit LOW first
    for (int i = 0; i < N/2; i++) {
        futures.push_back(pool.submit(
            [&low_completed]() { low_completed.fetch_add(1); },
            Priority::LOW
        ));
    }

    // submit HIGH after — should execute before remaining LOWs
    for (int i = 0; i < N/2; i++) {
        futures.push_back(pool.submit(
            [&high_completed]() { high_completed.fetch_add(1); },
            Priority::HIGH
        ));
    }

    for (auto& f : futures) f.get();

    assert((high_completed + low_completed) == N);
    pool.stats.verify();
    cout << "HIGH completed: " << high_completed << "\n";
    cout << "LOW completed:  " << low_completed  << "\n";

    pool.shutdown();
    pool.stats.print();
}

// ── Test 5 — Shutdown Safety ─────────────────────────────────────
void test_shutdown_safety() {
    cout << "\n╔══════════════════════════════════════╗\n";
    cout <<   "║  TEST 5: Shutdown Safety (RAII)      ║\n";
    cout <<   "╚══════════════════════════════════════╝\n";

    atomic<int> counter{0};
    const int N = 10000;

    {
        ThreadPool pool(4);

        for (int i = 0; i < N; i++) {
            try {
                pool.submit([&counter]() {
                    counter.fetch_add(1);
                });
            } catch (runtime_error&) {
                break;  // pool shut down mid-submission
            }
        }
        // RAII — destructor calls shutdown() here automatically
    }

    cout << "✅ Pool destroyed cleanly via RAII\n";
    cout << "📊 Tasks completed before shutdown: "
         << counter.load() << "\n";
}

// ── Test 6 — Exception Safety ────────────────────────────────────
void test_exception_safety() {
    cout << "\n╔══════════════════════════════════════╗\n";
    cout <<   "║  TEST 6: Exception Safety            ║\n";
    cout <<   "╚══════════════════════════════════════╝\n";

    ThreadPool pool(4);

    const int N = 100;
    atomic<int> completed{0};
    atomic<int> exceptions_caught{0};
    vector<future<int>> futures;
    futures.reserve(N);

    for (int i = 0; i < N; i++) {
        futures.push_back(pool.submit([i, &completed]() -> int {
            if (i % 10 == 0) {
                throw runtime_error("intentional error " +
                                    to_string(i));
            }
            completed.fetch_add(1);
            return i;
        }));
    }

    for (int i = 0; i < N; i++) {
        try {
            futures[i].get();
        } catch (runtime_error&) {
            exceptions_caught.fetch_add(1);
        }
    }

    // pool must still be alive after exceptions
    auto f = pool.submit([]() { return 42; });
    assert(f.get() == 42);

    cout << "Pool survived " << exceptions_caught
         << "exceptions\n";
    cout << "Normal tasks completed: " << completed << "\n";
    cout << "Pool still functional after exceptions\n";

    pool.shutdown();
    pool.stats.print();
}

// ═══════════════════════════════════════════════════════════════════
//  PHASE 2 — FAIRNESS ANALYSIS
// ═══════════════════════════════════════════════════════════════════

void analyze_priority_fairness() {
    cout << "\n╔══════════════════════════════════════╗\n";
    cout <<   "║  FAIRNESS: Priority Ordering         ║\n";
    cout <<   "╚══════════════════════════════════════╝\n";

    // use 1 worker for pure priority ordering
    ThreadPool pool(1);

    const int N = 30;
    vector<pair<int, string>> execution_log;
    mutex log_m;
    atomic<int> order{0};
    vector<future<void>> futures;
    futures.reserve(N);

    for (int i = 0; i < 10; i++) {
        futures.push_back(pool.submit([&, i]() {
            lock_guard<mutex> lock(log_m);
            execution_log.push_back(
                {order.fetch_add(1), "LOW:   " + to_string(i)});
        }, Priority::LOW));

        futures.push_back(pool.submit([&, i]() {
            lock_guard<mutex> lock(log_m);
            execution_log.push_back(
                {order.fetch_add(1), "HIGH:  " + to_string(i)});
        }, Priority::HIGH));

        futures.push_back(pool.submit([&, i]() {
            lock_guard<mutex> lock(log_m);
            execution_log.push_back(
                {order.fetch_add(1), "NORMAL:" + to_string(i)});
        }, Priority::NORMAL));
    }

    for (auto& f : futures) f.get();

    cout << "Execution order (first 15):\n";
    for (int i = 0; i < min(15, (int)execution_log.size()); i++) {
        cout << "  " << setw(3) << execution_log[i].first
             << ": " << execution_log[i].second << "\n";
    }

    // count how many HIGH tasks ran in the first 10 slots
    int high_in_first_third = 0;
    for (int i = 0; i < N/3; i++) {
        if (execution_log[i].second.find("HIGH") != string::npos)
            high_in_first_third++;
    }
    cout << "📊 HIGH tasks in first " << N/3 << " slots: "
         << high_in_first_third << "/" << 10 << "\n";

    pool.shutdown();
}

// ═══════════════════════════════════════════════════════════════════
//  PHASE 3 — BENCHMARKS
// ═══════════════════════════════════════════════════════════════════

void benchmark_throughput() {
    cout << "\n╔══════════════════════════════════════╗\n";
    cout <<   "║  BENCHMARK: Throughput Scaling       ║\n";
    cout <<   "╚══════════════════════════════════════╝\n";

    const int N = 100000;

    for (int num_workers : {1, 2, 4, 8}) {
        ThreadPool pool(num_workers);

        atomic<int> counter{0};
        vector<future<void>> futures;
        futures.reserve(N);

        auto start = chrono::steady_clock::now();

        for (int i = 0; i < N; i++) {
            futures.push_back(pool.submit([&counter]() {
                counter.fetch_add(1);
            }));
        }
        for (auto& f : futures) f.get();

        auto ms = chrono::duration_cast<chrono::milliseconds>
                  (chrono::steady_clock::now() - start).count();

        assert(counter.load() == N);
        cout << "  Workers: " << setw(2) << num_workers
             << " | Time: " << setw(6) << ms << "ms"
             << " | Throughput: ";
        if (ms > 0)
            cout << setw(10) << (N * 1000 / ms) << " tasks/sec\n";
        else
            cout << "   <1ms (too fast to measure)\n";

        pool.shutdown();
    }
}

void benchmark_steal_rate() {
    cout << "\n╔══════════════════════════════════════╗\n";
    cout <<   "║  BENCHMARK: Steal Rate               ║\n";
    cout <<   "╚══════════════════════════════════════╝\n";

    const int N = 10000;

    for (auto& scenario : vector<pair<string,int>>{
        {"balanced  (round-robin)", -1},
        {"imbalanced (all Worker 0)", 0}
    }) {
        ThreadPool pool(4);

        vector<future<void>> futures;
        futures.reserve(N);

        for (int i = 0; i < N; i++) {
            futures.push_back(pool.submit(
                [](){},
                Priority::NORMAL,
                scenario.second  // -1 = round-robin, 0 = all to W0
            ));
        }
        for (auto& f : futures) f.get();

        long executed = pool.stats.tasks_executed.load();
        long stolen   = pool.stats.tasks_stolen.load();

        cout << "  " << scenario.first
             << " | Steal rate: "
             << (executed > 0 ? stolen * 100 / executed : 0)
             << "%\n";

        pool.shutdown();
    }
}

void benchmark_data_structures() {
    cout << "\n╔══════════════════════════════════════╗\n";
    cout <<   "║  BENCHMARK: Data Structure (push)    ║\n";
    cout <<   "╚══════════════════════════════════════╝\n";

    const int N = 100000;

    BucketedPriorityDeque bpd;
    auto start = chrono::steady_clock::now();

    for (int i = 0; i < N; i++) {
        Priority p = static_cast<Priority>(i % 3);
        bpd.push(Task{p, i, [](){}});
    }

    auto us = chrono::duration_cast<chrono::microseconds>
              (chrono::steady_clock::now() - start).count();

    cout << "  BucketedPriorityDeque: "
         << N << " pushes in " << us << "us"
         << " (" << us / N << "us/op) — O(1)\n";
}

// ═══════════════════════════════════════════════════════════════════
//  MAIN — FULL VALIDATION SUITE
// ═══════════════════════════════════════════════════════════════════

int main() {
    cout << "╔══════════════════════════════════════╗\n";
    cout << "║    SCHEDULER VALIDATION SUITE        ║\n";
    cout << "╚══════════════════════════════════════╝\n";

    // ── Phase 1: Correctness ─────────────────────────────────────
    cout << "\n━━━ PHASE 1: CORRECTNESS TESTS ━━━\n";
    test_tiny_tasks();
    test_long_tasks();
    test_heavy_stealing();
    test_mixed_workload();
    test_shutdown_safety();
    test_exception_safety();

    // ── Phase 2: Fairness ────────────────────────────────────────
    cout << "\n━━━ PHASE 2: FAIRNESS ANALYSIS ━━━\n";
    analyze_priority_fairness();

    // ── Phase 3: Performance ─────────────────────────────────────
    cout << "\n━━━ PHASE 3: BENCHMARKS ━━━\n";
    benchmark_throughput();
    benchmark_steal_rate();
    benchmark_data_structures();

    cout << "\n All validation phases complete\n";
    return 0;
}
    */