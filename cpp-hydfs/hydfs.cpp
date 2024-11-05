#include <iostream>
#include <filesystem>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <string>
#include<vector>
#include <sstream>
#include <fstream>
#include <memory>
#include <shared_mutex>

 constexpr uint64_t MAX_LOG_SIZE_IN_MEMORY = 1024;
 constexpr int REPLICATE_NUM = 3;

class Log {
public:
    Log(std::string source_host, int64_t source_req_id, uint64_t seq_num, 
                    std::vector<uint8_t> content, const std::string& content_dir, 
                    const std::string& dfs_file)  {}
    // Retrieve the content from a log 
    // return (content, successful) 
    std::string getContent() const {}
    uint64_t get_seq_num() const { return seq_num; }
    uint64_t get_log_size() const { return log_size; }

private:
    int64_t seq_num;
    int64_t log_size;
    std::string source_host;
    int64_t source_req_id;

    std::vector<uint8_t> content;
    std::string content_filename;
};

Log::Log(std::string source_host, int64_t source_req_id, uint64_t seq_num, 
                    std::vector<uint8_t> content, const std::string& content_dir, 
                    const std::string& dfs_file) 
                    : seq_num(seq_num), log_size(content.size()), source_host(std::move(source_host)), source_req_id(source_req_id) {

    std::string content_filename; 

    if (log_size <= MAX_LOG_SIZE_IN_MEMORY) {
        // small enough to store in memory 
        this->content = content;
    } else {
        // need to store as a file
        std::ostringstream oss;
        oss << content_dir << "/" << dfs_file << "-" << seq_num << ".txt";
        content_filename = oss.str();

        try {
            std::ofstream file(content_filename, std::ios::binary);
            if (!file) {
                throw std::ios_base::failure("Failed to open file for writing");
            }
            file.write(reinterpret_cast<const char*>(content.data()), content.size());
        } catch (const std::ios_base::failure& e) {
            std::cerr << "Error in writing log content into " << content_filename << ": " << e.what() << std::endl;
            // [TODO] when error is detected
            exit(1);
        }
        
    }
}

std::string Log::getContent() const {
    if (!content.empty()) {
            return std::string(content.begin(), content.end());
        }

        if (!content_filename.empty()) {
            try {
                std::ifstream file(content_filename, std::ios::binary);
                if (!file) {
                    throw std::ios_base::failure("Failed to open file for reading");
                }
                std::ostringstream oss;
                oss << file.rdbuf();
                return oss.str();
            } catch(const std::ios_base::failure& e) {
                std::cerr << "Error in writing log content into " << content_filename << ": " << e.what() << std::endl;
                // [TODO] figure out what to do when a fauiler is detected 
                exit(1);
            }
        }
        return "";
}


class File {
public:
    File(std::string dfs_filename) : filename(std::move(dfs_filename)), filesize(0) {}
    void addLog(Log log) {
        std::unique_lock(rw_lock);
        filesize += log.get_log_size();
        logs.push_back(std::make_shared<Log>(std::move(log)));
    }

    bool appendFileWithLog(std::shared_ptr<Log> log, uint64_t seq_num) {
        std::unique_lock lock(rw_lock);

        if (seq_num != logs.size()) {
            std::cerr << "Failed to append log with seq_num " << log->get_seq_num()
                      << " at position " << seq_num << ", " << filename
                      << " has " << logs.size() << " logs" << std::endl;
            return false;
        }

        filesize += log->get_log_size();
        logs.push_back(log);
        return true;
    }

private:
    std::string filename;
    uint64_t filesize;
    std::vector<std::shared_ptr<Log>> logs;
    mutable std::shared_mutex rw_lock;
}