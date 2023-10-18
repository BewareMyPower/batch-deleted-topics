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

    auto curlm = curl_multi_init();
    assert(curlm);

    std::vector<CurlWrapper> handles(topics.size());
    for (auto& handle : handles) {
        if (!handle.init()) {
            std::cerr << "Failed to init curl" << std::endl;
            return 2;
        }
    }
    CurlWrapper::Options options;
    options.method = "DELETE";
    for (size_t i = 0; i < topics.size(); i++) {
        auto url = base_url + "/" + stripTopic(topics[i]) + "/partitions";
        handles[i].run(url, header, options, nullptr, curlm);
    }

    int running_handles = 0;
    do {
        CURLMcode code = curl_multi_perform(curlm, &running_handles);
        std::cout << "running_handles: " << running_handles << std::endl;
        if (code == CURLM_OK) {
            int ret;
            code = curl_multi_poll(curlm, nullptr, 0, 5000, &ret);
            std::cout << "code: " << code << ", ret: " << ret << std::endl;
        }
        if (code != CURLM_OK) {
            std::cerr << "curl_multi failed: " << code << std::endl;
            break;
        }
    } while (running_handles > 0);
    for (const auto& handle : handles) {
        curl_multi_remove_handle(curlm, handle.handle());
    }

    handles.clear();
    curl_multi_cleanup(curlm);
    return 0;
}
