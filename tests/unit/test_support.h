#ifndef TEST_SUPPORT_H
#define TEST_SUPPORT_H

#include <exception>
#include <iostream>
#include <stdexcept>
#include <string>

class TestFailure : public std::runtime_error {
  public:
    TestFailure(const char *expression, const char *file, int line)
        : std::runtime_error(std::string(file) + ":" + std::to_string(line) + ": check failed: " + expression) {}
};

#define CHECK(expression)                                                                                              \
    do {                                                                                                               \
        if (!(expression)) {                                                                                           \
            throw TestFailure(#expression, __FILE__, __LINE__);                                                        \
        }                                                                                                              \
    } while (false)

template <typename Function> int run_test(const char *name, Function function) {
    try {
        function();
        std::cout << "[PASS] " << name << '\n';
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "[FAIL] " << name << ": " << error.what() << '\n';
        return 1;
    } catch (...) {
        std::cerr << "[FAIL] " << name << ": unknown exception\n";
        return 1;
    }
}

#endif // TEST_SUPPORT_H
