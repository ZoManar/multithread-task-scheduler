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
using namespace std;

// _____Priority______________________________________________________________________
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

//___Scheduler Stats___________________________________________________________________
struct SchedulerStats
{
    // pool-level
    atomic<long> tasks_submitted{0};
    atomic<long> tasks_executed{0};
    atomic<long> tasks_stolen{0};
    // timing
    atomic<long> total_latency_us{0}; // from submission until execution start
    atomic<long> min_latency_us{LONG_MAX};
    atomic<long> max_latency_us{0};

    // per-worker
    struct WorkerStats{
        atomic<long> tasks_executed{0};
        atomic<long> tasks_stolen{0};
        atomic<long> times_idle{0};
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

};



// ___BucketedPriorityDeque___________________________________________________________
class BucketedPriorityDeque{
private:
    // one deque per priority level (indexed by priorities, low=0, normal=1, high=2)
    array<deque<Task>, 3> buckets; 
    mutable mutex m;
    size_t total = 0;
    static int idx(Priority p) {
        return static_cast<int>(p);
    }

public:
    // append to correct bucket O(1)
    void push(Task task){
        lock_guard<mutex> lock(m);
        buckets[idx(task.priority)].push_back(std::move(task));
        total++;
    }

    optional<Task> pop_back() {
        lock_guard<mutex> lock(m);
        // owner pops highest priority from back
        for (int i = 2; i >= 0; i--){
            if (!buckets[i].empty()){
                Task t = std::move(buckets[i].back());
                buckets[i].pop_back();
                total--;
                return t;
            }
        }
        return nullopt;
    }

    optional<Task> steal_front() {
        lock_guard<mutex> lock(m);
        for (int i = 0; i <= 2; i++){
            if (!buckets[i].empty()){
                Task t = std::move(buckets[i].front());
                buckets[i].pop_front();
                total--;
                return t;
            }
        }
        return nullopt;
    }
    
    bool empty() const{
        lock_guard<mutex> lock(m);
        return total == 0;
    }

    size_t size() const {
        lock_guard<mutex> lock(m);
        size_t total = 0;
        for (auto& b : buckets) total += b.size();
        return total;
    }
};


class ThreadPool {
private:
    struct Worker{
    // each worker owns its deque and knows about all other workers
        BucketedPriorityDeque deque;
        thread t;
        mutex cv_m;
        condition_variable cv;
        int id = 0;
        atomic<bool> is_idle{false}; // true when sleeping
    };
    
    vector<unique_ptr<Worker>> workers;
    atomic<bool> stop_flag{false};  
    atomic<int> next_worker{0};     // round-robin counter 
    atomic<int> id_generator{0};
    atomic<int> total_tasks{0};     // tasks across ALL queues

public:
    SchedulerStats stats;
   
private:
    // ── stealing ─────────────────────────────────────────────────
    optional<Task> try_steal(int my_id){
        int n = workers.size();
        
        // first try steal from busiest
        int busiest_id = -1;
        size_t max_size = 0;
        for (int i = 0; i < n; i++){
            if (i == my_id) continue;
            size_t s = workers[i]->deque.size();
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
        for (int i = 0; i < n; i++) {
            int target = (my_id + 1 + i) % n;
            if (target == my_id) continue;
            auto t = workers[target]->deque.steal_front();  // decrement on steal
            if (t) {
                total_tasks.fetch_sub(1);
                return t;
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
    void run_task(int worker_id, Task task) {
        // record latency
        auto now = chrono::steady_clock::now();
        auto latency = chrono::duration_cast<chrono::microseconds>
                        (now - task.submit_time).count();
        stats.record_latency(latency);

        try {
                task.work();
            } catch (const exception& e) {
                cerr << "Worker " << worker_id
                    << " exception: " << e.what() << "\n";
            } catch (...) {
                cerr << "Worker " << worker_id
                    << " unknown exception\n";
            }
    }

    // ── worker loop ──────────────────────────────────────────────
    void worker_loop(int my_id){
        Worker& me = *workers[my_id];

        while (true){

            // ── drain own queue completely ──────────────
            while(true){
                //drain own quque before stealing
                auto task = me.deque.pop_back();
                if(!task) break;
                total_tasks.fetch_sub(1); // decrement on pop
                stats.tasks_executed.fetch_add(1);
                stats.workers[my_id].tasks_executed.fetch_add(1);
                run_task(my_id, std::move(*task));
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
                    run_task(my_id, std::move(*task));
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
                    run_task(my_id, std::move(*task));
                }
                
                return;     // clean exit
            }

            // ── go idle — event-driven sleep ────────────
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

};


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