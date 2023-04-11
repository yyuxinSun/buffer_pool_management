#include <iostream>

int main() {
    const size_t size = 16 * 1024 * 1024 * 1024;
    // char* ptr = new char[size];
    char* ptr = (char*) malloc(size);
    std::cout << ptr[0] << ptr[size - 10] << std::endl;

    delete[] ptr;
    return 0;
}