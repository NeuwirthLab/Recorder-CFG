#include <iostream>
#include <fstream>
#include <vector>
#include <algorithm>
#include <string>
#include <map>
#include <filesystem>
#include <regex>
#include "reader.h"

RecorderReader reader;


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

    auto begin() {
        return indices.begin();
    }

    auto end() {
        return indices.end();
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

std::vector<Filter<int, int>>* read_filter(std::string &fpath, std::vector<Filter<int, int>> *filters){
    std::ifstream ffile(fpath);
    if (!ffile.is_open()) {
        std::cerr << "Error: Unable to open file at " << fpath << "\n";
        return nullptr;
    }

    std::string fline;
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

        filters->emplace_back(func_name, indices);
    }

    std::cout << "Successfully processed filters.\n";
    return filters;
}


CST* reader_get_cst(RecorderReader* reader, int rank) {
    CST* cst = reader->csts[rank];
    return cst;
}

CFG* reader_get_cfg(RecorderReader* reader, int rank) {
    CFG* cfg;
    if (reader->metadata.interprocess_compression)
        cfg = reader->cfgs[reader->ug_ids[rank]];
    else
        cfg = reader->cfgs[rank];
    return cfg;
}


Record* reader_cs_to_record(CallSignature *cs) {

    Record *record = static_cast<Record *>(malloc(sizeof(Record)));

    char* key = static_cast<char *>(cs->key);

    int pos = 0;
    memcpy(&record->tid, key+pos, sizeof(pthread_t));
    pos += sizeof(pthread_t);
    memcpy(&record->func_id, key+pos, sizeof(record->func_id));
    pos += sizeof(record->func_id);
    memcpy(&record->call_depth, key+pos, sizeof(record->call_depth));
    pos += sizeof(record->call_depth);
    memcpy(&record->arg_count, key+pos, sizeof(record->arg_count));
    pos += sizeof(record->arg_count);

    record->args = static_cast<char **>(malloc(sizeof(char *) * record->arg_count));

    int arg_strlen;
    memcpy(&arg_strlen, key+pos, sizeof(int));
    pos += sizeof(int);

    char* arg_str = key+pos;
    int ai = 0;
    int start = 0;
    for(int i = 0; i < arg_strlen; i++) {
        if(arg_str[i] == ' ') {
            record->args[ai++] = strndup(arg_str+start, (i-start));
            start = i + 1;
        }
    }

    assert(ai == record->arg_count);
    return record;
}

std::string charPointerArrayToString(char** charArray, int size) {
    std::string result;
    for (int i = 0; i < size; ++i) {
        if (charArray[i] != nullptr) {
            result += std::string(charArray[i]);
            if (i < size - 1) {
                result += " ";
            }
        }
    }
    return result;
}

std::vector<std::string> charPointerPointerArrayToList(char** charArray, int size) {
    std::vector<std::string> args_list;
    for(int i= 0; i< size; i++){
        if (charArray[i] != nullptr){
            args_list.emplace_back(charArray[i]);
        }
    }
    return args_list;
}





#define TERMINAL_START_ID 0

void printRules(RuleHash *rule) {
    for (int i = 0; i < rule->symbols; ++i) {
        if (rule->rule_body[2 * i + 0] >= TERMINAL_START_ID) {
            std::cout << "Rule" << rule->rule_id << " : " << rule->rule_body[2 * i + 0] << "^"
                      << rule->rule_body[2 * i + 1] << std::endl;
        }
    }

}

void applyFilter(Record* record, RecorderReader *reader, std::vector<Filter<int, int>> *filters){
    std::string func_name = recorder_get_func_name(reader, record);
    std::vector<std::string> args = charPointerPointerArrayToList(record->args, record->arg_count);
    for(auto &filter:*filters){
        if(filter.func_name == func_name){
            std::vector<std::string> args_list;
            int arg_cnt = 0;
            for(auto it = filter.indices.begin(); it != filter.indices.end(); ++it){
                // for each index in the filter
                int index = stoi(it->first);
                auto &intervalTable = it->second;
                if (intervalTable.data.empty()){
                    // no intervals defined for this arg
                    args_list.push_back(args[index]);
                }else{
                    for( auto &interval : intervalTable.data){
                        //go through the intervals and check if the record->args[i] is in any of the defined intervals
                        if(std::stoi(args[index]) >= interval.first.lower && std::stoi(args[index]) < interval.first.upper){
                            args_list.push_back(std::to_string(interval.second));
                        }
                    }
                }
                arg_cnt++;
            }
            if(arg_cnt == args_list.size()){
                record->args = static_cast<char**>(malloc(sizeof (char*) * args_list.size()));
                for(int i=0; i<args_list.size(); i++){
                    record->args[i] = strdup(args_list[i].c_str());
                }
                record->arg_count = args_list.size();
            }
        }
    }

}

void rule_application(RecorderReader* reader, CFG* cfg, CST* cst, int rule_id, int free_record, std::vector<Filter<int, int>> *filters) {
    RuleHash *rule = NULL;
    HASH_FIND_INT(cfg->cfg_head, &rule_id, rule);
    assert(rule != NULL);
    //printRules(rule);
    for(int i = 0; i < rule->symbols; i++) {
        int sym_val = rule->rule_body[2*i+0];
        int sym_exp = rule->rule_body[2*i+1];
        std::cout << sym_val << "^"<<sym_exp << std::endl;
        if (sym_val >= TERMINAL_START_ID) { // terminal
            for(int j = 0; j < sym_exp; j++) {
                Record* record = reader_cs_to_record(&(cst->cs_list[sym_val]));
                applyFilter(record, reader, filters);
/*                std::string func_name = recorder_get_func_name(reader, record);
                std::string args = charPointerArrayToString(record->args, record->arg_count);

                std::cout << func_name << " "<<args << std::endl;
                // std::cout << "record->tid: " << record->func_id << std::endl;
                */
                if(free_record)
                    recorder_free_record(record);
            }
        } else {                            // non-terminal (i.e., rule)
            for(int j = 0; j < sym_exp; j++)
                rule_application(reader, cfg, cst, sym_val, free_record, filters);
        }
    }
}


int main(int argc, char* argv[]) {
    std::vector<Filter<int, int>> filters;

    std::string fpath = "/g/g90/zhu22/repos/Recorder-CFG/tools/filters.txt";
    read_filter(fpath, &filters);

    std::string rpath = "/g/g90/zhu22/iopattern/recorder-20241007/170016.899-ruby22-zhu22-ior-1614057/";
    recorder_init_reader(rpath.c_str(), &reader);
    for(int rank = 0; rank < reader.metadata.total_ranks; rank++) {
        std::cout << "Rank: " << rank << std::endl;
        CST* cst = reader_get_cst(&reader, rank);
        CFG* cfg = reader_get_cfg(&reader, rank);
        rule_application(&reader, cfg, cst, -1, 1, &filters);

    }


/*
    MultiIndexIntervalTable<int, std::string> multiIndexTable;
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