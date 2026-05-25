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
    
    // higher priority stays at back of deque
    bool operator<(const Task& other) const {
        if (priority == other.priority)
            return arrival_sequence> other.arrival_sequence;
        return priority < other.priority;
    }
};

// _____Work stealing deque___________________________________________________________
class PriorityDeque {
private:
    deque<Task> items;
    mutable mutex m ;

    //insert task in sorted position - keep deque orderd
    void sorted_insert(Task task) {
        // find correct position using binary search logic 
        // it is an iterator points to the correct insertion position.
        auto it = lower_bound(items.begin(), items.end(), task, [](const Task& a, const Task& b) {
            return a < b; 
        }
    );
    items.insert(it, std::move(task));
    }
    
public:
    // owner pushes in sorted order
    void push(Task& task){
        lock_guard<mutex> lock(m);
        sorted_insert(std::move(task));
    }

    // owner pops highest priority from back 
    optional<Task> pop_back() {
        lock_guard<mutex> lock(m);
        if (items.empty()) return nullopt;
        Task task = std::move(items.back());
        items.pop_back(); // removes the empty shell
        return task;
    }

    // steal lowest priority from front 
    optional<Task> steal_front() {
        lock_guard<mutex> lock(m);
        if (items.empty()) return std::nullopt;
        Task task = std::move(items.front());
        items.pop_front();
        return task;
    }

    bool empty() const{
        lock_guard<mutex> lock(m);
        return items.empty();
    }

    size_t size() const {
        lock_guard<mutex> lock(m);
        return items.size();
    }
};

class ThreadPool{
private:
    struct Worker{
    // each worker owns its deque and knows about all other workers
        PriorityDeque deque;
        thread t;
    };

    vector<unique_ptr<Worker>> workers;
    mutex global_m;
    condition_variable global_cv;
    atomic<bool> stop_flag{false};
    atomic<int> next_worker{0};
    atomic<int> id_generator{0};

    // check if any queue has tasks
    bool has_any_tasks() const{
        for (auto& w : workers)
            if (!w -> deque.empty()) 
                return true;
            
            return false;
    }

    optional<function<void()>> try_steal(int my_id){
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

        if (busiest_id >= 0 && max_size > 1){
            auto task = workers[busiest_id]->deque.steal_front();
            if (task) return task;
        }
        // random sweep if busiest attempt failed
        int start = rand() % n;
        for (int i = 0; i < n; i++){
            int target = (start + i) % n;
            if(target == my_id) continue;
            auto task = workers[target]->deque.steal_front();
            if (task) return task;
        }
        return std::nullopt;
    }


    void worker_loop(int my_id){
        while (true){
            // check if my own queue has task
            auto task = workers[my_id]->deque.pop_back();

            // try stealing from others
            if(!task){
                task = try_steal(my_id);
            }

            // run the task if found
            if(task){
                try{
                    task->work();
                }catch (const exception& e) {
                    cerr <<"Worker:  " << my_id << "exception: " << e.what() <<"\n";
                }catch (...){
                    cerr <<"Worker:  " << my_id << "unknown exception\n";
                }
                continue;
            }

            {
                // nothing found - sleep
                unique_lock<mutex> lock(global_m);
                global_cv.wait_for(
                    lock,
                    chrono::milliseconds(1),
                    [this]{
                        return stop_flag.load() || has_any_tasks();
                    }
                );
            
            }
            
            if(stop_flag.load()&& !has_any_tasks()) return;

            }
        }

public:
        
    explicit ThreadPool(int num_threads) {
        workers.reserve(num_threads);

        for (int i = 0; i < num_threads; i++){
            workers.push_back(make_unique<Worker>());

            workers.back()->t = thread(
                &ThreadPool::worker_loop, this, i
            );
        }

    }

    template<typename F> 
    auto submit(F task, Priority piority = Priority::NORMAL,int preferred_worker = -1) -> future<decltype(task())>{

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
        global_cv.notify_one();
        return result;
    }

    // graceful shutdown - first finish All queue tasks then exit
    void shutdown()
    {
        bool expected = false;
        if(!stop_flag.compare_exchange_strong(expected, true)) {
            return;
        }
      
        global_cv.notify_all();
        for (auto &w: workers){
            if (w->t.joinable()) w->t.join();
        } 
    }
    
    ~ThreadPool(){
        shutdown();

    }
    ThreadPool(const ThreadPool&) = delete;
    ThreadPool& operator=(const ThreadPool&) = delete;

};

int main() {
    ThreadPool pool(3);

    vector<future<int>> futures;

    // mix of priorities — HIGH should complete before LOW
    for (int i = 0; i < 5; i++) {
        futures.push_back(pool.submit([i]() {
            cout << "LOW task " << i << " on "
                 << this_thread::get_id() << "\n";
            return i;
        }, Priority::LOW));
    }

    for (int i = 0; i < 5; i++) {
        futures.push_back(pool.submit([i]() {
            cout << "HIGH task " << i << " on "
                 << this_thread::get_id() << "\n";
            return i * 10;
        }, Priority::HIGH));
    }

    for (auto& f : futures) f.get();
    pool.shutdown();
}