#include <mutex>
#include <condition_variable>
#include <deque>

template<typename T>
class customQueue {
private:
    std::mutex d_mutex;
    std::condition_variable d_condition;
    std::deque<T> d_queue;
public:
    void push(T const &value) {
        {
            std::unique_lock<std::mutex> lock(this->d_mutex);
            d_queue.push_front(value);
        }
        this->d_condition.notify_all();
    }

    T pop() {
        std::unique_lock<std::mutex> lock(this->d_mutex);
        this->d_condition.wait(lock, [=] { return !this->d_queue.empty(); });
        T rc(std::move(this->d_queue.back()));
        this->d_queue.pop_back();
        return rc;
    }

    [[nodiscard]] size_t size() const {
        //std::unique_lock<std::mutex> lock(this->d_mutex);
        return d_queue.size();
    }

    std::vector<T> copy_newElements() {
        static size_t lastCopiedIndex = 0; // Tracks the last copied position
        std::vector<T> new_elements;      // To store new elements
        auto current_index = d_queue.size();
        {
            //std::unique_lock<std::mutex> lock(this->d_mutex); // Lock for thread safety
            if (lastCopiedIndex < current_index) { // Check if there are new elements
                new_elements.assign(d_queue.rbegin(), d_queue.rbegin() + (d_queue.size() - lastCopiedIndex)); // Reverse copy the new elements
                lastCopiedIndex = current_index;// Update the index for the next call
            }
        }
        return new_elements; // Return new elements in reverse order
    }


};