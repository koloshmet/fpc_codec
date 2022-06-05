#include <iostream>
#include <bit>
#include <span>
#include <vector>
#include <random>

#include "fpc_codec.h"

void PrintBytes(std::span<const std::byte> bytes) {
    for (auto byte : bytes) {
        std::printf("%02X ", static_cast<unsigned char>(byte));
    }
    std::puts("");
}

void SingleTest() {
    using Float = double;

    std::random_device dev{};
    auto seed = dev();
    std::cout << "Seed: " << seed << std::endl;
    std::mt19937_64 rnd{seed};
    std::uniform_int_distribution size_dist(50'000'000, 100'000'000);
    std::uniform_real_distribution<Float> val_dist(std::numeric_limits<Float>::min(), std::numeric_limits<Float>::max());
    auto count = size_dist(rnd);
    std::vector<Float> inp;
    inp.reserve(count);
    for (int i = 0; i < count; ++i) {
        inp.push_back(val_dist(rnd));
    }

    DB::CompressionCodecFPC codec(8, 8);
    std::vector<std::byte> encoded;
    encoded.resize(codec.getMaxCompressedDataSize(inp.size() * sizeof(Float)));
    std::vector<Float> decoded(count, 0);

    auto start = std::chrono::steady_clock::now();

    auto compressed = codec.doCompressData((char*)inp.data(), inp.size() * sizeof(Float), (char*)encoded.data());
    auto compr = std::chrono::steady_clock::now();
    codec.doDecompressData((char*)encoded.data(), compressed, (char*)decoded.data(), decoded.size() * sizeof(Float));

    auto fin = std::chrono::steady_clock::now();

    auto enc = std::chrono::duration_cast<std::chrono::milliseconds>(compr - start).count();
    auto dec = std::chrono::duration_cast<std::chrono::milliseconds>(fin - compr).count();

    std::cout << "Enc: " << enc << "ms Dec: " << dec << "ms\n";
    std::cout << "In:" << inp.size() << " Src:" << inp.size() * sizeof(Float);
    std::cout << " Compr:" << compressed  << '\n';
    std::cout << "Rate:" << (double)inp.size() * sizeof(Float) / compressed;
    std::cout << " Enc Speed:" << inp.size() * sizeof(Float) * 1000 / (1024 * 1024) / enc  << "MB/s ";
    std::cout << " Dec Speed:" << inp.size() * sizeof(Float) * 1000 / (1024 * 1024) / dec  << "MB/s " << std::endl;

    for (int i = 0; i < count; ++i) {
        if (decoded[i] != inp[i]) {
            std::cout << i << std::endl;
            break;
        }
    }
}

void DebugTest(std::size_t count) {
    std::random_device dev{};
    std::mt19937_64 rnd{1418139277};
    std::uniform_real_distribution val_dist(1.0, 10.0);
    std::vector<double> inp;
    inp.reserve(count);
    for (std::size_t i = 0; i < count; ++i) {
        inp.push_back(val_dist(rnd));
    }

    DB::CompressionCodecFPC codec(8, 10);
    std::vector<std::byte> encoded;
    encoded.resize(codec.getMaxCompressedDataSize(inp.size() * 8));
    std::vector<double> decoded(count, 0);

    PrintBytes(std::as_bytes(std::span(inp)));
    auto compressed = codec.doCompressData((char*)inp.data(), inp.size() * 8, (char*)encoded.data());
    PrintBytes(encoded);
    codec.doDecompressData((char*)encoded.data(), compressed, (char*)decoded.data(), decoded.size() * 8);
    PrintBytes(std::as_bytes(std::span(decoded)));

    for (std::size_t i = 0; i < count; ++i) {
        std::cout << inp[i] << ' ' << decoded[i] << std::endl;
    }
}

int main() {
    while (true) {
        SingleTest();
    }
}
