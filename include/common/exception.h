//
// Created by zwx on 23-8-30.
//

//
// Created by peng on 10/18/22.
//

#pragma once

#include <exception>
#include <string>

namespace utils {
    class WorkloadException : public std::exception {
    public:
        explicit WorkloadException(std::string message) : m(std::move(message)) { }
        [[nodiscard]] const char* what() const noexcept override {
            return m.c_str();
        }

    private:
        std::string m;
    };
}