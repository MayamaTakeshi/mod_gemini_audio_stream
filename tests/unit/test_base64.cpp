#include "base64.h"
#include "test_support.h"

#include <stdexcept>
#include <string>

namespace {

void test_known_vectors() {
    CHECK(base64_encode("") == "");
    CHECK(base64_encode("f") == "Zg==");
    CHECK(base64_encode("fo") == "Zm8=");
    CHECK(base64_encode("foo") == "Zm9v");
    CHECK(base64_decode("Zm9v") == "foo");
}

void test_binary_round_trip() {
    const std::string binary("\x00\x01\x7f\x80\xff\x00", 6);
    CHECK(base64_decode(base64_encode(binary)) == binary);
}

void test_url_safe_encoding() {
    const std::string binary("\xfb\xff", 2);
    CHECK(base64_encode(binary, true) == "-_8.");
    CHECK(base64_decode("-_8.") == binary);
}

void test_invalid_input_throws() {
    bool threw = false;
    try {
        static_cast<void>(base64_decode("%%%="));
    } catch (const std::runtime_error&) {
        threw = true;
    }
    CHECK(threw);
}

} // namespace

int main() {
    int failures = 0;
    failures += run_test("known Base64 vectors", test_known_vectors);
    failures += run_test("binary Base64 round trip", test_binary_round_trip);
    failures += run_test("URL-safe Base64", test_url_safe_encoding);
    failures += run_test("invalid Base64 input", test_invalid_input_throws);
    return failures == 0 ? 0 : 1;
}
