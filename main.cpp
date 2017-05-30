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

using namespace std;

deque<vector<string>> d;
condition_variable cv;
mutex mtx;
atomic<bool> done { false };
const uint64_t block_size = 100;
vector<unordered_map<string, uint64_t> > WarehouseS;
multimap<uint64_t, string> MergedWarehouseByCount;
map<string, uint64_t> MergedWarehouse;

streampos FileEnd(ifstream& file)
{
    file.seekg(0, file.end);
    streampos end = file.tellg();
    file.seekg(0, file.beg);

    return end;
}

int producer(string fileName)
{
    ifstream file(fileName);
    if (!file.is_open())
        return 1;

    streampos end = FileEnd(file);
    while (file.tellg() != end)
    {
        string line;
        vector<string> lines;

        for (size_t line_index = 0; getline(file, line) && line_index < block_size; ++line_index)
        {
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

bool isAlnum(string str)
{
    auto it = std::find_if(std::begin(str), std::end(str), [](char c)
    {
        return c < 0 || !isalnum(c);
    });

    return it == std::end(str);
}

int consumer(int rank)
{
    int i = 0;
    while (true)
    {
        unique_lock<std::mutex> lk(mtx);
        if (!d.empty())
        {
            ++i;
            vector<string> wordS{(vector<string> &&) d.front()};  //take our data
            d.pop_front();
            lk.unlock();

            auto& warehouse = WarehouseS[rank];
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


void MergeByName()
{
    MergedWarehouse.clear();
    for (auto hashTable : WarehouseS)
    {
        for (auto word : hashTable)
        {
            MergedWarehouse[word.first] += word.second;
        }
    }
}

auto flip_pair(const pair<string, uint64_t> &p)
{
    return pair<uint64_t, string>(p.second, p.first);
}

void MergeByCount()
{
    MergeByName();

    MergedWarehouseByCount.clear();
    std::transform(MergedWarehouse.begin(), MergedWarehouse.end(),
                   std::inserter(MergedWarehouseByCount, MergedWarehouseByCount.begin()),
                   flip_pair);
}

template <typename T>
void FlushToFile(string outDir, bool isSortedByName, T& mergedWarehouse)
{
    // string fileName = isSortedByName ? "res_a.txt" : "res_n.txt";
    // ofstream out(m_outDir + fileName, ios::trunc);
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
    WarehouseS.resize(2);
    thread thr1 = thread(producer,
                         "/Users/Yasya/Desktop/Results/The_H_G.txt");
    thread thr2 = thread(consumer, 0);
    thread thr3 = thread(consumer, 1);
    thr1.join();
    thr2.join();
    thr3.join();

    bool sort_by_name = false;
    if (sort_by_name)
    {
        MergeByName();
        FlushToFile("text.txt", true, MergedWarehouse);
    }
    else
    {
        MergeByCount();
        FlushToFile("text.txt", false, MergedWarehouseByCount);
    }
}
