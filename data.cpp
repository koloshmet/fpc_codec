#include <iostream>
#include <bit>
#include <span>
#include <vector>
#include <cassert>

#include <filesystem>
#include <fstream>

#include "fpc_codec.h"

void PrintBytes(std::span<const std::byte> bytes) {
    for (auto byte : bytes) {
        std::printf("%02X ", static_cast<unsigned char>(byte));
    }
    std::puts("");
}

template <typename Float>
void DataTest(const std::filesystem::path& dataFile) {
    std::ifstream file(dataFile, std::ios::binary);
    std::vector<char> data(std::istreambuf_iterator<char>(file), std::istreambuf_iterator<char>{});

    DB::CompressionCodecFPC codec(8, 12);
    std::vector<std::byte> encoded;
    encoded.resize(codec.getMaxCompressedDataSize(data.size()));
    std::vector<char> decoded(data.size(), '\0');

    auto start = std::chrono::steady_clock::now();

    auto compressed = codec.doCompressData(data.data(), data.size(), (char*)encoded.data());
    auto compr = std::chrono::steady_clock::now();
    codec.doDecompressData((char*)encoded.data(), compressed, decoded.data(), decoded.size());

    auto fin = std::chrono::steady_clock::now();

    auto enc = std::chrono::duration_cast<std::chrono::milliseconds>(compr - start).count();
    auto dec = std::chrono::duration_cast<std::chrono::milliseconds>(fin - compr).count();

    std::cout << "Enc: " << enc << "ms Dec: " << dec << "ms\n";
    std::cout << "In:" << data.size() / sizeof(Float) << " Src:" << data.size();
    std::cout << " Compr:" << compressed  << '\n';
    std::cout << "Rate:" << (double)data.size() / compressed;
    std::cout << " Enc Speed:" << data.size() * 1000 / (1024 * 1024) / enc  << "MB/s ";
    std::cout << " Dec Speed:" << data.size() * 1000 / (1024 * 1024) / dec  << "MB/s " << std::endl;

    for (std::size_t i = 0; i < data.size(); ++i) {
        if (decoded[i] != data[i]) {
            std::cout << i << std::endl;
            break;
        }
    }
}

int main(int argc, char* argv[]) {
    assert(argc > 1);
    std::span args(argv, argc);
    DataTest<Float64>(args[1]);
    return 0;
}
