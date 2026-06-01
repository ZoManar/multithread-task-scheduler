#include <iostream>
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
using namespace std;

// _____Priority______________________________________________________________________
enum class Priority {
    LOW = 0,
    NORMAL = 1,
    HIGH = 2
};

// _____TASK__________________________________________________________________________
struct Task {
    Priority priority;
    int arrival_sequence;
    function<void()> work;
};

// ___BucketedPriorityDeque___________________________________________________________
class BucketedPriorityDeque{
private:
    // one deque per priority level (indexed by priorities, low=0, normal=1, high=2)
    array<deque<Task>, 3> buckets; 
    mutable mutex m;
    size_t total_size = 0;
    static int idx(Priority p) {
        return static_cast<int>(p);
    }

public:
    // append to correct bucket O(1)
    void push(Task task){
        lock_guard<mutex> lock(m);
        int i = idx(task.priority);
        buckets[i].push_back(std::move(task));
        total_size++;
    }

    optional<Task> pop_back() {
        lock_guard<mutex> lock(m);
        // pop from highest non empty bucket
        for (int i = 2; i >= 0; i--){
            if (!buckets[i].empty()){
                Task t = std::move(buckets[i].back());
                buckets[i].pop_back();
                total_size--;
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
                total_size--;
                return t;
            }
        }
        return nullopt;
    }
    
    bool empty() const{
        lock_guard<mutex> lock(m);
        return total_size == 0;
    }

    size_t size() const {
        lock_guard<mutex> lock(m);
        return total_size;
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
    };
    

    vector<unique_ptr<Worker>> workers;
    atomic<bool> stop_flag{false};
    atomic<int> next_worker{0};
    atomic<int> id_generator{0};

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
            if (task) return task;
        }
        // random sweep if busiest attempt failed
        for (int i = 0; i < n; i++) {
            int target = (my_id + 1 + i) % n;
            if (target == my_id) continue;
            auto t = workers[target]->deque.steal_front();
            if (t) return t;
        }
        return std::nullopt;
    }

    void run_task(int worker_id, Task task) {
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

    void worker_loop(int my_id){
        Worker& me = *workers[my_id];
        while (true){
            while(true){
                //drain own ququq before stealing
                auto task = me.deque.pop_back();
                if(!task) break;
                run_task(my_id, std::move(*task));

            }
            
            {
                // try steal from busiest worker
                auto task = try_steal(my_id);
                if(task){
                    run_task(my_id, std::move(*task));
                    continue;
                }
            }

            if(stop_flag.load()&& me.deque.empty()){

                while (true)
                {
                    auto task = try_steal(my_id);
                    if(!task) break;
                    run_task(my_id, std::move(*task));
                }
                
                return;
            }

            {
                unique_lock<mutex> lock(me.cv_m);
                me.cv.wait_for(
                    lock,
                    chrono::milliseconds(50),  // fallback for stealing
                    [&] {
                        return stop_flag.load() || !me.deque.empty();
                    }
                );
            }
        }
    }
        
public:
    explicit ThreadPool(int num_threads) {
        workers.reserve(num_threads);

        for (int i = 0; i < num_threads; i++){
            workers.push_back(make_unique<Worker>());
            workers.back()->id = i;
            workers.back()->t = thread(
                &ThreadPool::worker_loop, this, i
            );
        }

    }

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
            [pt](){ (*pt)(); }
        });
        workers[target]->cv.notify_one();
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
                w->cv.notify_all();
            }
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

int main() {
    ThreadPool pool(4);

    const int NUM_TASKS = 100;
    vector<future<int>> futures;
    futures.reserve(NUM_TASKS);

    auto start = chrono::steady_clock::now();

    for (int i = 0; i < NUM_TASKS; i++) {
        Priority p = (i % 3 == 0) ? Priority::HIGH
                   : (i % 3 == 1) ? Priority::NORMAL
                                   : Priority::LOW;
        futures.push_back(pool.submit([i]() {
            return i * i;
        }, p));
    }

    int total = 0;
    for (auto& f : futures) total += f.get();

    auto end = chrono::steady_clock::now();
    auto ms  = chrono::duration_cast<chrono::milliseconds>
               (end - start).count();

    cout << "Completed " << NUM_TASKS << " tasks\n";
    cout << "Total: "    << total     << "\n";
    cout << "Time: "     << ms        << "ms\n";

    pool.shutdown();
}