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
#include "json.hpp"
using json = nlohmann::json;


static char formatting_record[32];
static CallSignature* global_cst = NULL;


struct RankContribution {
    int rank;
    int total_size = 0;
    int num_ops = 0;
    double avg_size = 0;

    RankContribution() = default;
    RankContribution(int r) : rank(r) {}

    void updateContribution(int size_r) {
        total_size += size_r;
        num_ops++;
    }

    void computeAverages() {
        if (num_ops > 0) avg_size = static_cast<double>(total_size) / num_ops;
    }
};


struct Operation {
    std::string name;
    std::unordered_map<int, RankContribution> rank_contributions;

    Operation() = default;
    Operation(const std::string& op_name) : name(op_name) {}

    void updateRankContribution(int rank, int size) {
        if (rank_contributions.find(rank) == rank_contributions.end()) {
            rank_contributions[rank] = RankContribution(rank);
        }
        rank_contributions[rank].updateContribution(size);
    }

    void computeAverages() {
        for (auto& [rank, rc] : rank_contributions) {
            rc.computeAverages();
        }
    }

    bool  is_shared () const {
        if (rank_contributions.size() > 1) {
            return true;
        } else {
            return false;
        }
    }
};


struct File {
    std::string file_name;
    double total_size_r = 0.0;
    double total_size_w = 0.0;

    std::unordered_map<std::string, Operation> operations;

    File() = default;
    File(const std::string& name) : file_name(name) {}

    void updateOperationContribution(const std::string& operation_name, int rank, int size) {
        if (operations.find(operation_name) == operations.end()) {
            operations[operation_name] = Operation(operation_name);
        }
        operations[operation_name].updateRankContribution(rank, size);
    }

    void computeFinalAverages() {
        for (auto& [op_name, op] : operations) {
            op.computeAverages();
        }
    }

    void computeFinalFileContribution() {
        total_size_r = 0.0;
        total_size_w = 0.0;
        for (auto& [op_name, op] : operations) {
            for (auto& [rank, rc] : op.rank_contributions) {
                if (op.name.find("read") != std::string::npos) {
                    total_size_r += rc.total_size;
                }
                if (op.name.find("write") != std::string::npos){
                    total_size_w += rc.total_size;
                }
            }
        }
    }
};

struct Files {
    std::unordered_map<std::string, File> file_map;

    // Add or update a file's operation contribution
    void addOrUpdateFile(const std::string& file_name, const std::string& operation_name, int rank, int size) {
        if (file_map.find(file_name) == file_map.end()) {
            file_map[file_name] = File(file_name);
        }
        file_map[file_name].updateOperationContribution(operation_name, rank, size);
    }

    void computeStatistics() {
        for (auto& [file_name, file] : file_map) {
            file.computeFinalAverages();
        }
    }

    // Print information
    void printSharedFilesInfo() {
        for (auto& [file_name, file] : file_map) {
            std::cout << "File: " << file_name << " \n";
            file.computeFinalFileContribution();
            std::cout << "Total Read Size: " << file.total_size_r / (1024 * 1024) << " MB\n";
            std::cout << "Total Write Size: " << file.total_size_w / (1024 * 1024) << " MB\n";
            for (const auto& [op_name, op] : file.operations) {
                std::cout << ", is_shared: " << op.is_shared() << std::endl;
                for (const auto& [rank, rc] : op.rank_contributions) {
                    std::cout << "  Operation: " << op.name
                              << ", Rank: " << rank
                              << ", Avg Size: " << rc.avg_size
                              << ", Num Ops: " << rc.num_ops << "\n";
                }
            }
        }
    }
};

void get_per_file_stats(Record* record, RecorderReader *reader, Files *files, int rank) {
    std::string func_name = recorder_get_func_name(reader, record);

/*    if (func_name == "open" || func_name == "fopen") {
        // Directly use the file name from args[0] to ensure it exists in the map
        std::string file_name = record->args[0];
        files->addOrUpdateFile(file_name, func_name, rank, 0);
    } else */

    if (func_name.find("write") != std::string::npos) {
        std::string file_name;
        int size_w = 0;

        if (func_name == "write" || func_name == "pwrite") {
            // Handle is the file name (args[0]), size is args[2]
            file_name = record->args[0];
            size_w = std::stoi(record->args[2]);
        } else if (func_name == "fwrite") {
            // Handle is the file name (args[3]), size = args[1] * args[2]
            file_name = record->args[3];
            int size = std::stoi(record->args[1]);
            int count = std::stoi(record->args[2]);
            size_w = size * count;
        } else {
            return; // Unsupported write variant
        }

        files->addOrUpdateFile(file_name, func_name, rank, size_w);
    } else if (func_name.find("read") != std::string::npos) {
        std::string file_name;
        int size_r = 0;

        if (func_name == "read" || func_name == "pread") {
            // Handle is the file name (args[0]), size is args[2]
            file_name = record->args[0];
            size_r = std::stoi(record->args[2]);
        } else if (func_name == "fread") {
            // Handle is the file name (args[3]), size = args[1] * args[2]
            file_name = record->args[3];
            int size = std::stoi(record->args[1]);
            int count = std::stoi(record->args[2]);
            size_r = size * count;
        } else {
            return; // Unsupported read variant
        }

        files->addOrUpdateFile(file_name, func_name, rank, size_r);
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
    Files* files;
} IterArg;


void iterate_record(Record* record, void* arg) {
    IterArg *ia = (IterArg*) arg;
    get_per_file_stats(record, ia->reader, ia->files, ia->rank);
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


int main(int argc, char** argv) {

    if (argc != 2) {
        printf("usage: recorder-filter /path/to/trace-folder /path/to/filter-file\n");
        exit(1);
    }

    char* trace_dir = argv[1];
    RecorderReader reader;
    recorder_init_reader(trace_dir, &reader);
    Files files;

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
        iter_args[rank].files    = &files;
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
    files.computeStatistics();
    files.printSharedFilesInfo();
    //json file_configs = files.generateFileConfigsJson();

    // Print the JSON object
    //std::cout << file_configs.dump(4) << std::endl;

    /*
    generate_config_file(&reader, iter_args);
    // At this point we should have built the global cst and each
    // rank's local cfg. Now let's write them out.

    std::map<int, std::string> cfg_strings;
    //std::vector<std::string> cfg_strings;

    save_filtered_trace(&reader, iter_args, &cfg_strings);
    analyzeCFGs(cfg_strings);
    */
    // clean up everything
    cleanup_cst(global_cst);
    for(int rank = 0; rank < reader.metadata.total_ranks; rank++) {
        sequitur_cleanup(&iter_args[rank].local_cfg);
    }
    recorder_free_reader(&reader);

}

