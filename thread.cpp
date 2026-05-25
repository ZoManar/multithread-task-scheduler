#include <iostream>
#include <thread>
#include <queue>
#include <mutex>
#include <condition_variable>

using namespace std;

queue<int> tasks;

mutex m;

condition_variable cv;

bool finished = false;

void producer()
{
    for (int i = 1; i <= 10; i++)
    {
        {
            lock_guard<mutex> lock(m);

            tasks.push(i);

            cout << "Produced: " << i << "\n";
        }

        // Wake up one waiting thread
        cv.notify_one();
    }

    {
        lock_guard<mutex> lock(m);

        finished = true;
    }

    // Wake up consumer so it can exit
    cv.notify_one();
}

void consumer()
{
    while (true)
    {
        unique_lock<mutex> lock(m);

        cv.wait(lock, []()
        {
            return !tasks.empty() || finished;
        });

        if (finished && tasks.empty())
        {
            break;
        }

        int task = tasks.front();

        tasks.pop();

        cout << "Consumed: " << task << "\n";
    }
}

int main()
{
    thread p(producer);

    thread c(consumer);

    p.join();

    c.join();

    return 0;
}