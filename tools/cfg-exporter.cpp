#include <iostream>
#include <fstream>
#include <vector>
#include <algorithm>
#include <string>
#include <map>
#include <filesystem>
#include <regex>


template <typename KeyType>
class Interval {
public:
    KeyType lower;
    KeyType upper;

    Interval(KeyType l, KeyType u) : lower(l), upper(u) {}
};

template <typename KeyType, typename ValueType>
class IntervalTable {
public:
    ValueType& operator[](const KeyType& key) {
        auto it = std::lower_bound(data.begin(), data.end(), key,
                                   [](const auto& lhs, const auto& rhs) { return lhs.first.upper <= rhs; });
        return it->second;
    }

    void insert(const Interval<KeyType>& interval, const ValueType& value) {
        data.push_back({interval, value});
        std::sort(data.begin(), data.end(),
                  [](const auto& lhs, const auto& rhs) { return lhs.first.lower < rhs.first.lower; });
    }


    std::vector<std::pair<Interval<KeyType>, ValueType>> data;
};

template <typename KeyType, typename ValueType>
class MultiIndexIntervalTable {
public:
    void insert(const std::string& index, const Interval<KeyType>& interval, const ValueType& value) {
        indices[index].insert(interval, value);
    }

    void insert(const std::string& index) {
        // Ensure the index exists without adding intervals
        if (indices.find(index) == indices.end()) {
            indices[index] = IntervalTable<KeyType, ValueType>();
        }
    }

    ValueType& operator[](const std::pair<std::string, KeyType>& key) {
        const std::string& index = key.first;
        const KeyType& key_value = key.second;
        return indices[index][key_value];
    }

    void printIntervals(const std::string& index) const {
        const auto& table = indices.at(index);
        for (const auto& pair : table.data) {
            std::cout << "[" << pair.first.lower << ", " << pair.first.upper << ") : " << pair.second << "\n";
        }
    }

private:
    std::map<std::string, IntervalTable<KeyType, ValueType>> indices;  // Map from index name to IntervalTable
};

template <typename KeyType, typename ValueType>
class Filter{
public:
    std::string func_name;
    MultiIndexIntervalTable<KeyType, ValueType> indices;

    Filter(const std::string& name) : func_name(name) {}
    Filter(const std::string& name, const MultiIndexIntervalTable<KeyType, ValueType>& miit)
            : func_name(name), indices(miit) {}
};


std::vector<std::string> splitStringBySpace(const std::string& input) {
    std::vector<std::string> result;
    std::istringstream stream(input);
    std::string token;

    while (std::getline(stream, token, ' ')) {
        result.push_back(token);
    }

    return result;
}

std::pair<std::string, std::string> splitIntoNumberAndRanges(const std::string& input) {
    std::regex pattern(R"((\d+)\[(.*)\])"); // Matches format "<number>[<ranges>]"
    std::smatch match;
    if (std::regex_match(input, match, pattern)) {
        return {match[1], match[2]}; // Return the number and range array
    }
    return {"", ""};
}


template <typename KeyType, typename ValueType>
IntervalTable<KeyType, ValueType> parseRanges(const std::string& ranges) {
    IntervalTable<KeyType, ValueType> table;
    std::regex range_pattern(R"((\d+):(\d+)-(\d+))"); // Matches format "<lower>:<upper>-<value>"
    auto it = std::sregex_iterator(ranges.begin(), ranges.end(), range_pattern);
    auto end = std::sregex_iterator();

    for (; it != end; ++it) {
        KeyType lower = std::stoi((*it)[1]);
        KeyType upper = std::stoi((*it)[2]);
        ValueType value = std::stoi((*it)[3]);
        table.insert(Interval<KeyType>(lower, upper), value);
    }

    return table;
}

int read_filter(std::string &fpath) {
    std::ifstream ffile(fpath);
    if (!ffile.is_open()) {
        std::cerr << "Error: Unable to open file at " << fpath << "\n";
        return -1;
    }

    std::string fline;
    std::vector<Filter<int, int>> filters;

    while (std::getline(ffile, fline)) {
        if (fline.empty()) continue; // Skip empty lines

        std::vector<std::string> substrings = splitStringBySpace(fline);
        if (substrings.empty()) continue; // Skip lines with no content

        std::string func_name = substrings.at(0);
        substrings.erase(substrings.begin());
        MultiIndexIntervalTable<int, int> indices;

        for (const auto& substring : substrings) {
            if (substring.find('[') != std::string::npos) {
                auto [number, ranges] = splitIntoNumberAndRanges(substring);
                if (!number.empty() && !ranges.empty()) {
                    IntervalTable<int, int> table = parseRanges<int, int>(ranges);
                    for (const auto& [interval, value] : table.data) {
                        indices.insert(number, interval, value);
                    }
                } else {
                    std::cerr << "Warning: Invalid range format in substring '" << substring << "'\n";
                }
            } else {
                indices.insert(substring);
            }
        }

        filters.emplace_back(func_name, indices);
    }

    std::cout << "Successfully processed filters.\n";
    return 0;
}

int main(int argc, char* argv[]) {
    // std::vector<Filter<int, std::string>> *filters = nullptr;
    MultiIndexIntervalTable<int, std::string> multiIndexTable;
    std::string fpath = "/g/g90/zhu22/repos/Recorder-CFG/tools/filters.txt";
    int i = read_filter(fpath);

/*
    multiIndexTable.insert("0", {1, 5}, "2");
    multiIndexTable.insert("0", {5, 10}, "7");
    multiIndexTable.insert("0", {10, 15}, "12");

    // Insert data into index "B"
    multiIndexTable.insert("1", {1, 3}, "2");
    multiIndexTable.insert("1", {3, 7}, "4");
    multiIndexTable.insert("1", {7, 10}, "9");

    std::cout << "Index A contents:\n";
    multiIndexTable.printIntervals("0");
    std::cout << "Index B contents:\n";
    multiIndexTable.printIntervals("1");*/
}