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
#include <zlib.h>
#include <sys/stat.h>
#include <sys/types.h>
extern "C" {
#include "reader.h"
#include "recorder-sequitur.h"
}

#include <sstream>
#include <set>
#include <iterator>

#include <unordered_map>


static char formatting_record[32];
static char filtered_trace_dir[1024];
static CallSignature* global_cst = NULL;


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

// class for statistics

struct RankContribution {
    int rank;
    int total_size_r = 0;
    int total_size_w = 0;
    int num_r = 0;
    int num_w = 0;
    double avg_size_r = 0;
    double avg_size_w = 0;

    RankContribution() = default;

    RankContribution(int r) : rank(r) {}

    void updateContribution(int size_r, int size_w) {
        if (size_r > 0) {
            total_size_r += size_r;
            num_r++;
        }
        if (size_w > 0) {
            total_size_w += size_w;
            num_w++;
        }
    }

    // Compute averages after all operations
    void computeAverages() {
        if (num_r > 0) avg_size_r = static_cast<double>(total_size_r) / num_r;
        if (num_w > 0) avg_size_w = static_cast<double>(total_size_w) / num_w;
    }
};


class File {
public:
    std::string file_name;
    std::unordered_map<int, RankContribution> rank_contributions;

    File(const std::string& name) : file_name(name) {}

    void updateRankContribution(int rank, int size_r, int size_w) {
        if (rank_contributions.find(rank) == rank_contributions.end()) {
            rank_contributions[rank] = RankContribution(rank);
        }
        rank_contributions[rank].updateContribution(size_r, size_w);
    }

    void computeFinalAverages() {
        for (auto& [rank, rc] : rank_contributions) {
            rc.computeAverages();
        }
    }
};




void read_filters(char* filter_path, Filters<int, int> *filters){
    std::string fpath(filter_path);
    std::ifstream ffile(fpath);
    if (!ffile.is_open()) {
        std::cerr << "Error: Unable to open file at " << fpath<< "\n";
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

bool apply_filter_to_record(Record* record, Record* new_record, RecorderReader *reader, Filters<int, int> *filters){

    // duplicate the original record and then
    // make modifications to the new record
    memcpy(new_record, record, sizeof(Record));

    std::string func_name = recorder_get_func_name(reader, record);
    int match = 0;
    for(auto &filter:*filters) {
        if(filter.func_name == func_name) {
            match =1;
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
    return match;
}

void sequitur_write_rules(Grammar *grammar, std::map<int, std::string> *output, int rank) {
    Symbol *rule, *sym;
    int rules_count = 0, symbols_count = 0;
    DL_COUNT(grammar->rules, rule, rules_count);

    DL_FOREACH(grammar->rules, rule) {
        int count;
        DL_COUNT(rule->rule_body, sym, count);
        symbols_count += count;

        std::ostringstream rule_stream;
        // rule_stream << "Rule " << rule->val << " :-> ";

        DL_FOREACH(rule->rule_body, sym) {
            if (sym->exp > 1)
                rule_stream << sym->val << "^" << sym->exp << " ";
            else
                rule_stream << sym->val << " ";
        }

        (*output)[rank] = rule_stream.str();
    }
}


void sequitur_print_rules(Grammar *grammar) {
    Symbol *rule, *sym;
    int rules_count = 0, symbols_count = 0;
    DL_COUNT(grammar->rules, rule, rules_count);

    DL_FOREACH(grammar->rules, rule) {
        int count;
        DL_COUNT(rule->rule_body, sym, count);
        symbols_count += count;

        printf("Rule %d :-> ", rule->val);

        DL_FOREACH(rule->rule_body, sym) {
            if (sym->exp > 1)
                printf("%d^%d ", sym->val, sym->exp);
            else
                printf("%d ", sym->val);
        }
        printf("\n");
        //#endif
    }
}

// Function to split a string by spaces
std::vector<std::string> split(const std::string& str) {
    std::istringstream iss(str);
    return {std::istream_iterator<std::string>{iss}, std::istream_iterator<std::string>{}};
}

// Function to find the longest common subsequence (LCS)
std::pair<std::set<std::string>, std::map<int, std::vector<std::string>>> findGlobalSubsequence(const std::map<int, std::string>& ranks) {
    std::vector<std::vector<std::string>> sequences;
    for (const auto& [_, cfg] : ranks) {
        sequences.push_back(split(cfg));
    }

    // Find intersection of all sequences
    std::set<std::string> globalOps(sequences[0].begin(), sequences[0].end());
    for (const auto& seq : sequences) {
        std::set<std::string> seqSet(seq.begin(), seq.end());
        std::set<std::string> temp;
        std::set_intersection(globalOps.begin(), globalOps.end(), seqSet.begin(), seqSet.end(),
                              std::inserter(temp, temp.begin()));
        globalOps = temp;
    }

    // Extract local operations
    std::map<int, std::vector<std::string>> localOps;
    int rankIndex = 0;
    for (const auto& seq : sequences) {
        for (const auto& op : seq) {
            if (globalOps.find(op) == globalOps.end()) {
                localOps[rankIndex].push_back(op);
            }
        }
        rankIndex++;
    }

    return {globalOps, localOps};
}

// Function to parse CFGs into their structural components
std::map<int, std::map<std::string, std::string>> parseCFGs(const std::map<int, std::string>& cfgs) {
    std::map<int, std::map<std::string, std::string>> parsedCFGs;

    for (const auto& [rank, cfg] : cfgs) {
        std::vector<std::string> parts = split(cfg);
        std::map<std::string, std::string> structure;

        for (size_t i = 0; i < parts.size(); ++i) {
            structure["C_" + std::to_string(i + 1)] = parts[i];
        }

        parsedCFGs[rank] = structure;
    }

    return parsedCFGs;
}

// Function to substitute symbols with component names
std::map<int, std::string> substituteSymbolsWithComponents(const std::map<int, std::map<std::string, std::string>>& parsedCFGs) {
    std::map<int, std::string> substitutedCFGs;

    for (const auto& [rank, components] : parsedCFGs) {
        std::ostringstream substitutedStream;

        for (const auto& [key, value] : components) {
            if (value.find('^') != std::string::npos) {
                size_t exp_pos = value.find('^');
                substitutedStream << key << "^" << value.substr(exp_pos + 1) << " ";
            } else {
                substitutedStream << key << " ";
            }
        }

        substitutedCFGs[rank] = substitutedStream.str();
    }

    return substitutedCFGs;
}

// Function to find the longest common subsequence (LCS)
std::vector<std::string> findLCS(const std::vector<std::vector<std::string>>& sequences) {
    if (sequences.empty()) return {};

    std::vector<std::string> lcs = sequences[0];

    for (size_t i = 1; i < sequences.size(); ++i) {
        std::vector<std::string> currentLCS;
        std::set_intersection(
                lcs.begin(), lcs.end(),
                sequences[i].begin(), sequences[i].end(),
                std::back_inserter(currentLCS)
        );
        lcs = currentLCS;
    }

    return lcs;
}


// Function to analyze CFGs
void analyzeCFGs(const std::map<int, std::string>& ranks) {
    // Step 1: Check for global patterns
    auto [globalOps, localOps] = findGlobalSubsequence(ranks);

    if (!globalOps.empty()) {
        std::cout << "Global Operations Found:\n";
        std::cout << "Global: ";
        for (const auto& op : globalOps) {
            std::cout << op << " ";
        }
        std::cout << "\nLocal:\n";
        for (const auto& [rank, ops] : localOps) {
            std::cout << "  Rank " << rank << ": ";
            for (const auto& op : ops) {
                std::cout << op << " ";
            }
            std::cout << "\n";
        }
    } else {
        std::cout << "No Global Operations Found. Checking for Structural Similarities...\n";

        // Step 2: Parse CFGs for structural similarities
        auto parsedCFGs = parseCFGs(ranks);

        if (!parsedCFGs.empty()) {
            std::cout << "Substituting Symbols with Component Names...\n";
            auto substitutedCFGs = substituteSymbolsWithComponents(parsedCFGs);

            // Convert substituted CFGs into sequences for LCS
            std::vector<std::vector<std::string>> sequences;
            for (const auto& [rank, cfg] : substitutedCFGs) {
                sequences.push_back(split(cfg));
            }

            // Print substituted CFGs
            std::cout << "Substituted CFGs:\n";
            for (const auto& [rank, cfg] : substitutedCFGs) {
                std::cout << "Rank " << rank << ": " << cfg << "\n";
            }

            auto lcs = findLCS(sequences);
            // Output LCS
            std::cout << "Longest Common Subsequence:\n";
            for (const auto& component : lcs) {
                std::cout << component << " ";
            }
            std::cout << "\n";


        } else {
            std::cout << "No Structural Similarities Found.\n";
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
 * this function is directly copied from recorder/lib/recorder-cst-cfg.c
 * to avoid adding dependency to the entire recorder library.
 * TODO: think a better way to reuse this code
 */
char* serialize_cst(CallSignature *cst, size_t *len) {
    *len = sizeof(int);

    CallSignature *entry, *tmp;
    HASH_ITER(hh, cst, entry, tmp) {
        *len = *len + entry->key_len + sizeof(int)*3 + sizeof(unsigned);
    }

    int entries = HASH_COUNT(cst);
    char *res = (char*) malloc(*len);
    char *ptr = res;

    memcpy(ptr, &entries, sizeof(int));
    ptr += sizeof(int);

    HASH_ITER(hh, cst, entry, tmp) {
        memcpy(ptr, &entry->terminal_id, sizeof(int));
        ptr = ptr + sizeof(int);
        memcpy(ptr, &entry->rank, sizeof(int));
        ptr = ptr + sizeof(int);
        memcpy(ptr, &entry->key_len, sizeof(int));
        ptr = ptr + sizeof(int);
        memcpy(ptr, &entry->count, sizeof(unsigned));
        ptr = ptr + sizeof(unsigned);
        memcpy(ptr, entry->key, entry->key_len);
        ptr = ptr + entry->key_len;
    }

    return res;
}

/**
 * this function is directly copied from recorder/lib/recorder-utils.c
 * to avoid adding dependency to the entire recorder library.
 * TODO: think a better way to reuse this code
 */
void recorder_write_zlib(unsigned char* buf, size_t buf_size, FILE* out_file) {
    // Always write two size_t (compressed_size and decopmressed_size)
    // before writting the the compressed data.
    // This allows easier post-processing.
    long off = ftell(out_file);
    size_t compressed_size   = 0;
    size_t decompressed_size = buf_size;
    fwrite(&compressed_size, sizeof(size_t), 1, out_file);
    fwrite(&decompressed_size, sizeof(size_t), 1, out_file);

    int ret;
    unsigned have;
    z_stream strm;

    unsigned char out[buf_size];

    /* allocate deflate state */
    strm.zalloc = Z_NULL;
    strm.zfree  = Z_NULL;
    strm.opaque = Z_NULL;
    ret = deflateInit(&strm, Z_DEFAULT_COMPRESSION);
    // ret = deflateInit(&strm, Z_BEST_COMPRESSION);
    if (ret != Z_OK) {
        printf("[recorder-filter] fatal error: can't initialize zlib.\n");
        return;
    }

    strm.avail_in = buf_size;
    strm.next_in  = buf;
    /* run deflate() on input until output buffer not full, finish
       compression if all of source has been read in */
    do {
        strm.avail_out = buf_size;
        strm.next_out = out;
        ret = deflate(&strm, Z_FINISH);    /* no bad return value */
        assert(ret != Z_STREAM_ERROR);  /* state not clobbered */
        have = buf_size - strm.avail_out;
        compressed_size += have;
        if (fwrite(out, 1, have, out_file) != have) {
            printf("[recorder-filter] fatal error: zlib write out error.");
            (void)deflateEnd(&strm);
            return;
        }
    } while (strm.avail_out == 0);
    assert(strm.avail_in == 0);         /* all input will be used */

    /* clean up and return */
    (void)deflateEnd(&strm);

    fseek(out_file, off, SEEK_SET);
    fwrite(&compressed_size, sizeof(size_t), 1, out_file);
    fwrite(&decompressed_size, sizeof(size_t), 1, out_file);
    fseek(out_file, compressed_size, SEEK_CUR);
}

void save_updated_metadata(RecorderReader* reader) {
    char old_metadata_filename[2048] = {0};
    char new_metadata_filename[2048] = {0};
    FILE* srcfh;
    FILE* dstfh;
    void* fhdata;

    sprintf(old_metadata_filename, "%s/recorder.mt", reader->logs_dir);
    sprintf(new_metadata_filename, "%s/recorder.mt", filtered_trace_dir);

    srcfh = fopen(old_metadata_filename, "rb");
    dstfh = fopen(new_metadata_filename, "wb");

    // first copy the entire old meatdata file to the new metadata file
    size_t res = 0;
    fseek(srcfh, 0, SEEK_END);
    long metafh_size = ftell(srcfh);
    fhdata = malloc(metafh_size);
    fseek(srcfh, 0, SEEK_SET);
    res = fread(fhdata , 1, metafh_size, srcfh);
    if (ferror(srcfh)) {
        perror("Error reading metadata file\n");
        exit(1);
    }
    res = fwrite(fhdata, 1, metafh_size, dstfh);

    // then update the inter-process compression flag.
    int oldval = reader->metadata.interprocess_compression;
    reader->metadata.interprocess_compression = 0;
    fseek(dstfh, 0, SEEK_SET);
    fwrite(&reader->metadata, sizeof(RecorderMetadata), 1, dstfh);
    reader->metadata.interprocess_compression = oldval;

    fclose(srcfh);
    fclose(dstfh);
    free(fhdata);
}

void save_filtered_trace(RecorderReader* reader, IterArg* iter_args, std::map<int, std::string>* cfg_strings) {

    size_t cst_data_len;
    char* cst_data = serialize_cst(global_cst, &cst_data_len);

    for(int rank = 0; rank < reader->metadata.total_ranks; rank++) {
        char filename[1024] = {0};
        sprintf(filename, "%s/%d.cfg", filtered_trace_dir, rank);
        FILE* f = fopen(filename, "wb");
        int integers;
        int* cfg_data = serialize_grammar(&(iter_args[rank].local_cfg), &integers);
        recorder_write_zlib((unsigned char*)cfg_data, sizeof(int)*integers, f);
        fclose(f);
        free(cfg_data);

        // write out global cst, all ranks have the same copy
        sprintf(filename, "%s/%d.cst", filtered_trace_dir, rank);
        f = fopen(filename, "wb");
        recorder_write_zlib((unsigned char*)cst_data, cst_data_len, f);
        fclose(f);
        sequitur_print_rules(&(iter_args[rank].local_cfg));
        sequitur_write_rules(&(iter_args[rank].local_cfg), cfg_strings, rank);

    }

    free(cst_data);

    // Update metadata and write out
    save_updated_metadata(reader);

    // Save timestamps
    // for now, we simply copy the timestamp files from
    // the original trace folder, if we want to cut some
    // records, we also need to cut timestamps
    char cmd[1024];
    sprintf(cmd, "cp %s/recorder.ts %s/recorder.ts", reader->logs_dir, filtered_trace_dir);
    system(cmd);

    // Similarly, simply copy the version file from the
    // original trace folder.
    sprintf(cmd, "cp %s/VERSION %s/VERSION", reader->logs_dir, filtered_trace_dir);
    system(cmd);
}

/**
 * This function addes one record to the CFG and CST
 * the implementation is identical to that of the
 * recorder-logger.c
 */
static int current_cfg_terminal = 0;
void grow_cst_cfg(Grammar* cfg, Record* record) {
    int key_len;
    char* key = compose_cs_key(record, &key_len);

    CallSignature *entry = NULL;
    HASH_FIND(hh, global_cst, key, key_len, entry);
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
        HASH_ADD_KEYPTR(hh, global_cst, entry->key, entry->key_len, entry);
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
 * The pointer of this function is passed
 * to the recorder_decode_records() call.
 *
 * In this function:
 * 1. we apply the filters
 * 2. then build the cst and cfg
 */
void iterate_record(Record* record, void* arg) {

    IterArg *ia = (IterArg*) arg;

    // debug purpose; print out the original record
    // printf("old:");
    // print_record(record, ia->reader);

    // apply fiter to the record
    // then add it to the cst and cfg.
    Record new_record;
    bool match = apply_filter_to_record(record, &new_record, ia->reader, ia->filters);
    if (match){
        // debug purpose; print out the modified record
        // printf("new:");
        // print_record(&new_record, ia->reader);

        grow_cst_cfg(&ia->local_cfg, &new_record);
    }
    // debug purpose; print out the original record is ignored
    // printf("ignored:");
    // print_record(record, ia->reader);
}


#define TERMINAL_START_ID 0

void print_cfg(CFG* cfg, int rule_id) {
    RuleHash *rule = NULL;
    HASH_FIND_INT(cfg->cfg_head, &rule_id, rule);
    assert(rule != NULL);

    for(int i = 0; i < rule->symbols; i++) {
        int sym_val = rule->rule_body[2*i+0];
        int sym_exp = rule->rule_body[2*i+1];

        if (sym_val >= TERMINAL_START_ID) { // terminal
            printf("%d^%d\n", sym_val, sym_exp);
        } else {                            // non-terminal (i.e., rule)
            for(int j = 0; j < sym_exp; j++)
                print_cfg(cfg, sym_val);
        }
    }
}



void generate_config_file(RecorderReader* reader, IterArg* iter_args){
//    for(int i = 0; i < reader->metadata.total_ranks; i++){
//        IterArg* current_iter_arg = &iter_args[i];
//        Grammar* current_grammar = &current_iter_arg->local_cfg;
//        if (!current_grammar->rules) {
//            continue;
//        }
//        Symbol* current_rule = current_grammar->rules;
//        while (current_grammar != NULL){
//            // Traverse the rule body if it's a rule head
//            if (current_rule->rule_body != NULL) {
//                Symbol* body_symbol = current_rule->rule_body;
//                if (body_symbol !=){}
//            }
//        }
//
//    }
}



int main(int argc, char** argv) {

    if (argc != 3) {
        printf("usage: recorder-filter /path/to/trace-folder /path/to/filter-file\n");
        exit(1);
    }

    char* trace_dir = argv[1];
    char* filter_path = argv[2];

    Filters<int, int> filters;
    read_filters(filter_path, &filters);

    RecorderReader reader;
    recorder_init_reader(trace_dir, &reader);

    // create a new folder to store the filtered trace files
    sprintf(filtered_trace_dir, "%s/_filtered", reader.logs_dir);
    if(access(filtered_trace_dir, F_OK) != -1)
        rmdir(filtered_trace_dir);
    mkdir(filtered_trace_dir, S_IRWXU|S_IRWXG|S_IROTH|S_IXOTH);

    // Prepare the arguments to pass to each rank
    // when iterating local records
    IterArg *iter_args = (IterArg*) malloc(sizeof(IterArg) * reader.metadata.total_ranks);
    if (!iter_args) {
        perror("Memory allocation failed");
        exit(EXIT_FAILURE);
    }
    for(int rank = 0; rank < reader.metadata.total_ranks; rank++) {
        iter_args[rank].rank       = rank;
        iter_args[rank].reader     = &reader;
        iter_args[rank].filters    = &filters;
        // initialize local CFG
        sequitur_init(&(iter_args[rank].local_cfg));
        if (!iter_args[rank].local_cfg.rules) {
            fprintf(stderr, "Failed to initialize rules for rank %d\n", rank);
            exit(EXIT_FAILURE);
        }
    }

    // Go through each rank's records
    for(int rank = 0; rank < reader.metadata.total_ranks; rank++) {
        // this call iterates through all records of one rank
        // each record is processed by the iterate_record() function
        recorder_decode_records(&reader, rank, iterate_record, &(iter_args[rank]));
    }

    generate_config_file(&reader, iter_args);
    // At this point we should have built the global cst and each
    // rank's local cfg. Now let's write them out.

    std::map<int, std::string> cfg_strings;
    //std::vector<std::string> cfg_strings;

    save_filtered_trace(&reader, iter_args, &cfg_strings);
    analyzeCFGs(cfg_strings);

    // clean up everything
    cleanup_cst(global_cst);
    for(int rank = 0; rank < reader.metadata.total_ranks; rank++) {
        sequitur_cleanup(&iter_args[rank].local_cfg);
    }
    recorder_free_reader(&reader);

}

