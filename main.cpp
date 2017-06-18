#include <iostream>
#include <deque>
#include <atomic>
#include <mutex>
#include <thread>
#include <map>
#include <fstream>
#include <condition_variable>
#include <vector>
#include <fstream>
#include <string>
#include <algorithm>
#include <locale>
#include <map>
#include <unordered_map>
#include <sstream>
#include <iterator>

#include <algorithm>
#include <functional>
#include <cctype>
#include <locale>

using namespace std;

int producer(string filename, deque<vector<string>>& d, std::mutex& mtx, atomic<bool>& done, condition_variable& cv)
{
    const uint64_t block_size = 100;
    const uint64_t max_line_size = 500;

    ifstream file(filename);
    if (!file.is_open())
        return 1;

    file.seekg(3);
    while (true)
    {
        string line;
        vector<string> lines;

        for (size_t line_index = 0; getline(file, line) && line_index < block_size; ++line_index)
        {
            int mod = line.size() / max_line_size;
            line_index += mod;
            lines.push_back(line);
        }

        if (lines.size() == 0)
        {
            break;
        }

        {
            lock_guard<mutex> lg(mtx);
            d.push_back(lines);
        }

        cv.notify_one();
    }

    done = true;
    cv.notify_all();
    return 0;
}


// trim from start
static inline std::string &ltrim(std::string &s) {
    s.erase(s.begin(), std::find_if(s.begin(), s.end(),
                                    std::ptr_fun<int, int>(std::isalnum)));
    return s;
}

// trim from end
static inline std::string &rtrim(std::string &s) {
    s.erase(std::find_if(s.rbegin(), s.rend(),
                         std::ptr_fun<int, int>(std::isalnum)).base(), s.end());
    return s;
}

// trim from both ends
static inline std::string &trim(std::string &s) {
    return ltrim(rtrim(s));
}


bool isAlnum(string& str)
{
    str = trim(str);
    return str.size() != 0;
}

int consumer(int rank, deque<vector<string>>& d, deque<unordered_map<string, uint64_t>>& asseblyDeque, std::mutex& mtx, std::atomic<bool>& done, condition_variable& cv, condition_variable& assemblyCv, mutex& assemblerMtx)
{
    int i = 0;
    while (true)
    {
        unique_lock<std::mutex> lk(mtx);
        if (!d.empty())
        {
            ++i;
            cout << "rank = " << rank << ", iter = " << i << endl;
            vector<string> wordS{(vector<string> &&) d.front()};  //take our data
            d.pop_front();
            lk.unlock();

            unordered_map<string, uint64_t> warehouse;

            //auto& warehouse = WarehouseS[rank];
            for(int i = 0; i < wordS.size(); ++i)
            {
                istringstream iss(wordS[i]);
                vector<string> tokens{ istream_iterator<string>{iss},
                                       istream_iterator<string>{} };
                for (int i = 0; i < tokens.size(); ++i)
                {
                    if (isAlnum(tokens[i]))
                    {

                        std::transform(begin(tokens[i]), end(tokens[i]), begin(tokens[i]), ::tolower);
                        warehouse[tokens[i]]++;
                    }
                }
            }
            {
                lock_guard<mutex> lg(assemblerMtx);
                asseblyDeque.push_back(warehouse);
            }

            assemblyCv.notify_one();
        }
        else
        {
            if (done)
            {
                break;
            }
            else
            {
                cv.wait(lk);
            }
        }
    }

    return 0;
}



auto flip_pair(const pair<string, uint64_t> &p)
{
    return pair<uint64_t, string>(p.second, p.first);
}


int Assembly(map<string, uint64_t>& mergedwarehouse, deque<unordered_map<string, uint64_t>>& asseblyDeque, std::atomic<bool>& isPartitionDone, condition_variable& assemblyCv, mutex& assemblerMtx)
{
    while (true)
    {
        unique_lock<std::mutex> lk(assemblerMtx);
        if (!asseblyDeque.empty())
        {
            std::unordered_map<string, uint64_t> partition {(unordered_map<string, uint64_t> &&) asseblyDeque.front()};
            asseblyDeque.pop_front();
            lk.unlock();

            for (auto pair : partition)
            {
                mergedwarehouse[pair.first] += pair.second;
            }
        }
        else
        {
            if (isPartitionDone)
            {
                break;
            }
            else
            {
                assemblyCv.wait(lk);
            }
        }
    }

    return 0;
}



void MergeByCount(map<string, uint64_t>& mergedWarehouse, multimap<uint64_t, string>& mergedWarehouseByCount)
{
//	mergebyname();

    mergedWarehouseByCount.clear();
    std::transform(mergedWarehouse.begin(), mergedWarehouse.end(),
                   std::inserter(mergedWarehouseByCount, mergedWarehouseByCount.begin()),
                   flip_pair);
}

template <typename T>
void FlushToFile(string outDir, bool isSortedByName, T& mergedWarehouse)
{

    ofstream out(outDir, ios::trunc);
    out.width(20);
    if (isSortedByName)
        out << left << "word" << "\t\t\t\t" << "occurance" << endl << endl;
    else
        out << left << "occurance" << "\t\t\t\t" << "word" << endl << endl;

    for (auto word : mergedWarehouse)
    {
        out.width(20);
        out << left << word.first << "\t\t\t\t" << left << word.second << endl;
    }
}

int main()
{
    mutex mtx;
    atomic<bool> done { false };
    atomic<bool> isPartitionDone{ false };
    condition_variable cv;
    condition_variable assemblyCv;
    mutex assemblerMtx;


    map<string, uint64_t> mergedwarehouse;
    deque<vector<string>> d;
    deque<unordered_map<string, uint64_t>> asseblyDeque;

    //warehouses.resize(4);
    thread thr1 = thread(producer,
                         "/Users/Yasya/Desktop/Results/The_H_G.txt",
                         std::ref(d), std::ref(mtx),std::ref(done), std::ref(cv));
    thread thr2 = thread(consumer, 0, std::ref(d), std::ref(asseblyDeque), std::ref(mtx), std::ref(done), std::ref(cv), std::ref(assemblyCv), std::ref(assemblerMtx));
    thread thr3 = thread(consumer, 1, std::ref(d), std::ref(asseblyDeque), std::ref(mtx), std::ref(done), std::ref(cv), std::ref(assemblyCv), std::ref(assemblerMtx));
    thread thr4 = thread(consumer, 2, std::ref(d), std::ref(asseblyDeque), std::ref(mtx), std::ref(done), std::ref(cv), std::ref(assemblyCv), std::ref(assemblerMtx));
    thread thr5 = thread(consumer, 3, std::ref(d), std::ref(asseblyDeque), std::ref(mtx), std::ref(done), std::ref(cv), std::ref(assemblyCv), std::ref(assemblerMtx));

    thread thrAssembler = thread(Assembly, std::ref(mergedwarehouse), std::ref(asseblyDeque), std::ref(isPartitionDone), std::ref(assemblyCv), std::ref(assemblerMtx));
    //thread thr4 = thread(consumer, 2);
    thr1.join();
    thr2.join();
    thr3.join();
    thr4.join();
    thr5.join();
    isPartitionDone = true;
    assemblyCv.notify_one(); //щоб уникнути дедлоку
    thrAssembler.join();

    FlushToFile("textN.txt", true, mergedwarehouse);

    multimap<uint64_t, string> mergedWarehouseByCount;
    MergeByCount(mergedwarehouse, mergedWarehouseByCount);
    FlushToFile("textC.txt", false, mergedWarehouseByCount);
}