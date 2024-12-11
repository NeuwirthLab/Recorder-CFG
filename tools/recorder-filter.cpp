#include <iostream>
#include <fstream>
#include <vector>
#include <algorithm>
#include <string>
#include <map>
#include <filesystem>
#include <regex>
#include <math.h>
extern "C" {
#include "reader.h"
#include "recorder-sequitur.h"
}

static char formatting_record[32];


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

/**
 * TODO: no need to read a text file and process this mannually.
 * It would be easier just writing filters in a json file
 * and use an existing library to read; it will automatically
 * handles the strings, floats and arrays.
 */
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

/**
 * TODO: the second argument is in/out argument
 *  then why do you need to return it?
 */
std::vector<Filter<int, int>>* read_filters(std::string &fpath, std::vector<Filter<int, int>> *filters){
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

    std::cout << "Successfully read filters.\n";
    return filters;
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

void apply_filter_to_record(Record* record, RecorderReader *reader, std::vector<Filter<int, int>> *filters){
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
                } else {
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
                record->args = static_cast<char**>(malloc(sizeof(char*) * args_list.size()));
                for(int i=0; i<args_list.size(); i++){
                    record->args[i] = strdup(args_list[i].c_str());
                }
                record->arg_count = args_list.size();
                for(int i=0; i< record->arg_count; i++){
                    std::cout << record->args[i] << std::endl;
                }
            }
        }
    }
}








/**
 * helper structure for passing arguments
 * to the iterate_record() function
 */
typedef struct IterArg_t {
    int rank;
    RecorderReader* reader;
    CallSignature*  global_cst;
    Grammar*        local_cfg;
    // TODO: encapsulate filters to make it cleaner
    std::vector<Filter<int,int>>* filters;
} IterArg;


/**
 * This function addes one record to the CFG and CST
 * the implementation is identical to that of the
 * recorder-logger.c
 */
static int current_cfg_terminal = 0;
void grow_cst_cfg(Grammar* cfg, CallSignature* cst, Record* record) {
    int key_len;
    char* key = compose_cs_key(record, &key_len);

    CallSignature *entry = NULL;
    HASH_FIND(hh, cst, key, key_len, entry);
    if(entry) {                         // Found
        entry->count++;
        free(key);
    } else {                            // Not exist, add to hash table
        entry = (CallSignature*) malloc(sizeof(CallSignature));
        entry->key = key;
        entry->key_len = key_len;
        entry->rank = 0;
        entry->terminal_id = current_cfg_terminal++;
        entry->count = 1;
        HASH_ADD_KEYPTR(hh, cst, entry->key, entry->key_len, entry);
    }

    append_terminal(cfg, entry->terminal_id, 1);
}

/**
 * Function that processes one record at a time
 * The pointer of this function needs to be
 * passed to the recorder_decode_records() call.
 *
 * in this function:
 * 1. we apply the filters
 * 2. then build the cst and cfg
 */
void iterate_record(Record* record, void* arg) {

    IterArg *ia = (IterArg*) arg;

    bool user_func = (record->func_id == RECORDER_USER_FUNCTION);

    const char* func_name = recorder_get_func_name(ia->reader, record);

    fprintf(stdout, formatting_record, record->tstart, record->tend, // record->tid
                func_name, record->call_depth, recorder_get_func_type(ia->reader, record));

    for(int arg_id = 0; !user_func && arg_id < record->arg_count; arg_id++) {
        char *arg = record->args[arg_id];
        fprintf(stdout, " %s", arg);
    }
    fprintf(stdout, " )\n");

    // apply fiter to the record
    // then add it to the cst and cfg.
    apply_filter_to_record(record, ia->reader, ia->filters);
    grow_cst_cfg(ia->local_cfg, ia->global_cst, record);
}


int main(int argc, char* argv[]) {
    std::vector<Filter<int, int>> filters;

    // Recorder trace directory
    // TODO: read it from command line argument
    std::string trace_dir = "/g/g90/zhu22/iopattern/recorder-20241007/170016.899-ruby22-zhu22-ior-1614057/";

    // filter file path
    // TODO: read it from command line argument
    std::string filter_path = "/g/g90/zhu22/repos/Recorder-CFG/tools/filters.txt";
    read_filters(filter_path, &filters);

    RecorderReader reader;
    CallSignature  global_cst;

    // For debug purpose, can delete later
    int decimal =  log10(1 / reader.metadata.time_resolution);
    sprintf(formatting_record, "%%.%df %%.%df %%s %%d %%d (", decimal, decimal);

    // Go through each rank's records
    recorder_init_reader(trace_dir.c_str(), &reader);
    for(int rank = 0; rank < reader.metadata.total_ranks; rank++) {
        Grammar local_cfg;

        IterArg arg;
        arg.rank       = rank;
        arg.reader     = &reader;
        arg.local_cfg  = &local_cfg;
        arg.global_cst = &global_cst;
        arg.filters    = &filters;

        // this call iterates through all records of one rank
        // each record is processed by the iterate_record() function
        recorder_decode_records(&reader, rank, iterate_record, &arg);
    }
    recorder_free_reader(&reader);
}
