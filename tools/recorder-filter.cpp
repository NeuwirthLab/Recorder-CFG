#include <iostream>
#include <fstream>
#include <vector>
#include <algorithm>
#include <string>
#include <map>
#include <filesystem>
#include <regex>
#include <getopt.h>
#include <math.h>
extern "C" {
#include "reader.h"
#include "recorder-sequitur.h"
}

static char formatting_record[32];
static CallSignature*  global_cst = NULL;


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
class Filter {
public:
    std::string func_name;
    MultiIndexIntervalTable<KeyType, ValueType> indices;

    Filter(const std::string& name) : func_name(name) {}
    Filter(const std::string& name, const MultiIndexIntervalTable<KeyType, ValueType>& miit)
            : func_name(name), indices(miit) {}
};

template <typename KeyType, typename ValueType>
class Filters {
private:
    std::vector<Filter<KeyType, ValueType>> filters;

public:
    void addFilter(const Filter<KeyType, ValueType>& filter) {
        filters.push_back(filter);
    }
/*
    void addFilter(const std::string& name, const MultiIndexIntervalTable<KeyType, ValueType>& miit) {
        filters.emplace_back(name, miit);
    }
*/
    const Filter<KeyType, ValueType>& getFilter(size_t index) const {
        if (index < filters.size()) {
            return filters[index];
        } else {
            throw std::out_of_range("Index out of range");
        }
    }

    size_t size() const {
        return filters.size();
    }

    // in case for accessing the underlying vector
    const std::vector<Filter<KeyType, ValueType>>& getFilters() const {
        return filters;
    }

    auto begin() {
        return filters.begin();
    }

    auto end() {
        return filters.end();
    }

};


void parseArguments(int argc, char** argv, std::string& trace_dir, std::string& filter_path) {
    const char* const short_opts = "t:f:h";
    const option long_opts[] = {
            {"trace-dir", required_argument, nullptr, 't'},
            {"filter-path", required_argument, nullptr, 'f'},
            {nullptr, no_argument, nullptr, 0}
    };

    while (true) {
        const auto opt = getopt_long(argc, argv, short_opts, long_opts, nullptr);
        if (-1 == opt) break;
        switch (opt) {
            case 't':
                trace_dir = optarg;
                break;

            case 'f':
                filter_path = optarg;
                break;

            default:
                std::cout << "Usage: " << argv[0] << " [OPTIONS]\n"
                          << "  -t, --trace-dir    Set the trace directory\n"
                          << "  -f, --filter-path  Set the filter file path\n"
                          << "  -h, --help         Display this help message\n";
                exit(0);
        }
    }
}


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


void read_filters(std::string &fpath, Filters<int, int> *filters){
    std::ifstream ffile(fpath);
    if (!ffile.is_open()) {
        std::cerr << "Error: Unable to open file at " << fpath << "\n";
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

        filters->addFilter(Filter(func_name, indices));
    }
    std::cout << "Successfully read filters.\n";
}

void apply_filter_to_record(Record* record, Record* new_record, RecorderReader *reader, Filters<int, int> *filters){

    // duplicate the original record and then
    // make modifications to the new record
    memcpy(new_record, record, sizeof(Record));

    std::string func_name = recorder_get_func_name(reader, record);
    for(auto &filter:*filters) {
        if(filter.func_name == func_name) {
            std::vector<std::string> new_args;

            // TODO: should the filters include the same number of incides as the actual call?
            for(auto it = filter.indices.begin(); it != filter.indices.end(); ++it) {
                // for each index in the filter
                int index = stoi(it->first);
                auto &intervalTable = it->second;

                // Clustering
                int arg_modified = 0;
                for(auto &interval : intervalTable.data) {
                    if(atoi(record->args[index]) >= interval.first.lower && atoi(record->args[index]) < interval.first.upper) {
                        new_args.push_back(std::to_string(interval.second));
                        arg_modified = 1;
                        break;
                    }
                }

                if (!arg_modified)
                    new_args.push_back(record->args[index]);
            }

            // Overwrite the orginal record with modified args
            new_record->arg_count = new_args.size();
            new_record->args = (char**) malloc(sizeof(char*) * new_record->arg_count);
            for(int i = 0; i < new_record->arg_count; i++) {
                new_record->args[i] = strdup(new_args[i].c_str());
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
    Grammar         local_cfg;
    Filters<int,int>* filters;
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
 * This is a helper (debug) function that prints out
 * the recorded function call
 */
static void print_record(Record* record, RecorderReader *reader) {
    int decimal =  log10(1 / reader->metadata.time_resolution);
    sprintf(formatting_record, "%%.%df %%.%df %%s %%d %%d (", decimal, decimal);

    bool user_func = (record->func_id == RECORDER_USER_FUNCTION);
    const char* func_name = recorder_get_func_name(reader, record);

    fprintf(stdout, formatting_record, record->tstart, record->tend, // record->tid
                func_name, record->call_depth);

    for(int arg_id = 0; !user_func && arg_id < record->arg_count; arg_id++) {
        char *arg = record->args[arg_id];
        fprintf(stdout, " %s", arg);
    }
    fprintf(stdout, " )\n");
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

    // debug purpose; print out the original record
    printf("old:");
    print_record(record, ia->reader);

    // apply fiter to the record
    // then add it to the cst and cfg.
    Record new_record;
    apply_filter_to_record(record, &new_record, ia->reader, ia->filters);

    // debug purpose; print out the modified record
    printf("new:");
    print_record(&new_record, ia->reader);

    grow_cst_cfg(&ia->local_cfg, global_cst, &new_record);
}


int main(int argc, char** argv) {
    // Recorder trace directory
    std::string trace_dir = "/p/lustre2/wang116/corona/sources/Recorder-CFG/test/recorder-20241223/135530.813-corona171-wang116-a.out-756755";
    // filter file path
    std::string filter_path = "/p/lustre2/wang116/corona/sources/Recorder-CFG/test/recorder-20241223/135530.813-corona171-wang116-a.out-756755/filter.txt";
    parseArguments(argc, argv, trace_dir, filter_path);

    Filters<int, int> filters;
    read_filters(filter_path, &filters);

    RecorderReader reader;
    recorder_init_reader(trace_dir.c_str(), &reader);


    // Prepare the arguments to pass to each rank
    // when iterating local records
    IterArg *iter_args = (IterArg*) malloc(sizeof(IterArg));
    for(int rank = 0; rank < reader.metadata.total_ranks; rank++) {
        iter_args[rank].rank       = rank;
        iter_args[rank].reader     = &reader;
        iter_args[rank].filters    = &filters;
        // initialize local CFG
        sequitur_init(&(iter_args[rank].local_cfg));
    }

    // Go through each rank's records
    for(int rank = 0; rank < reader.metadata.total_ranks; rank++) {

        // this call iterates through all records of one rank
        // each record is processed by the iterate_record() function
        recorder_decode_records(&reader, rank, iterate_record, &(iter_args[rank]));
    }

    // At this point we should have built the global cst and each
    // rank's local cfg. Now let's write them out.



    recorder_free_reader(&reader);
}
