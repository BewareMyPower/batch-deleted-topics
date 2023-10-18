#include <iostream>
#include <vector>

#include "CurlWrapper.h"
#include "nlohmann/json.hpp"

using namespace pulsar;
using json = nlohmann::json;

std::string stripTopic(const std::string& topic) {
    static const std::string ns = "public/default/";
    static const std::string partition = "-partition-";

    size_t start = topic.find(ns);
    if (start != std::string::npos) {
        start += ns.size();
    } else {
        start = 0;
    }
    size_t end = topic.find(partition, start);
    if (end == std::string::npos) {
        end = topic.size();
    }
    return topic.substr(start, end - start);
}

int main(int argc, char* argv[]) {
    if (argc < 3) {
        std::cerr << "Usage: " << argv[0] << " url token" << std::endl;
        return 1;
    }
    std::string base_url = argv[1];
    std::string token = argv[2];
    pulsar::CurlWrapper curl;
    if (!curl.init()) {
        std::cerr << "Failed to init curl" << std::endl;
        return 2;
    }

    if (base_url.back() == '/') {
        base_url.pop_back();
    }
    base_url += "/admin/v2/persistent/public/default";
    std::string header = "Authorization: Bearer " + token;
    std::vector<std::string> topics;
    if (auto result = curl.run(base_url, header, {}, nullptr); result.error.empty()) {
        auto topics_json = json::parse(result.responseData);
        if (!topics_json.is_array()) {
            std::cerr << "Unexpected topic list JSON: " << topics_json.dump(4) << std::endl;
            return 3;
        }
        for (const auto& topic_json : topics_json) {
            auto topic_str = topic_json.get<std::string>();
            if (topic_str.find("test-topic") != std::string::npos) {
                topics.emplace_back(std::move(topic_str));
            }
        }
    } else {
        std::cerr << "Failed to get list: " << result.error << std::endl;
        return 3;
    }
    std::cout << "There are " << topics.size() << " topics to be delete" << std::endl;
    CurlWrapper::Options options;
    options.method = "DELETE";
    for (const auto& topic : topics) {
        auto url = base_url + "/" + stripTopic(topic) + "/partitions";
        curl.run(url, header, options, nullptr);
        std::cout << topic << " is deleted" << std::endl;
    }

    return 0;
}
