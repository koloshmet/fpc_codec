#include <span>
#include <bit>
#include <concepts>
#include <cstring>
#include <climits>
#include <vector>
#include <random>
#include <iostream>
#include <chrono>

using UInt8 = std::uint8_t;
using UInt32 = std::uint32_t;
using UInt64 = std::uint64_t;
using Float32 = float;
using Float64 = double;

template <typename... Args>
std::runtime_error Exception(Args&&...) {
    return std::runtime_error{"Shit happens"};
}

namespace DB {

template <std::size_t CHUNK_SIZE>
class CompressionCodecFPC {
public:
    CompressionCodecFPC(UInt8 float_size, UInt8 compression_level);

    UInt32 doCompressData(const char* source, UInt32 source_size, char* dest) const;

    void doDecompressData(const char* source, UInt32 source_size, char* dest, UInt32 uncompressed_size) const;

    UInt32 getMaxCompressedDataSize(UInt32 uncompressed_size) const;

    static constexpr UInt32 HEADER_SIZE{3};

private:
    UInt8 float_width;
    UInt8 level;
};


namespace ErrorCodes {
const int CANNOT_COMPRESS = 1;
const int CANNOT_DECOMPRESS = 1;
const int ILLEGAL_CODEC_PARAMETER = 1;
const int ILLEGAL_SYNTAX_FOR_CODEC_TYPE = 1;
const int BAD_ARGUMENTS = 1;
}

template <std::size_t CHUNK_SIZE>
UInt32 CompressionCodecFPC<CHUNK_SIZE>::getMaxCompressedDataSize(UInt32 uncompressed_size) const {
    auto float_count = (uncompressed_size + float_width - 1) / float_width;
    if (float_count % 2 != 0) {
        ++float_count;
    }
    return HEADER_SIZE + (float_count + float_count / 2) * float_width;
}

template <std::size_t CHUNK_SIZE>
CompressionCodecFPC<CHUNK_SIZE>::CompressionCodecFPC(UInt8 float_size, UInt8 compression_level)
    : float_width{float_size}, level{compression_level}
{
}

namespace {

UInt8 encodeEndianness(std::endian endian) {
    switch (endian) {
        case std::endian::little:
            return 0;
        case std::endian::big:
            return 1;
    }
    throw Exception("Unsupported endianness", ErrorCodes::BAD_ARGUMENTS);
}

std::endian decodeEndianness(UInt8 endian) {
    switch (endian) {
        case 0:
            return std::endian::little;
        case 1:
            return std::endian::big;
    }
    throw Exception("Unsupported endianness", ErrorCodes::BAD_ARGUMENTS);
}

}

namespace {

template <std::unsigned_integral TUint> requires (sizeof(TUint) >= 4)
class DfcmPredictor {
public:
    explicit DfcmPredictor(std::size_t table_size)
        : table(table_size, 0)
        , prev_value{0}
        , hash{0} {
    }

    [[nodiscard]]
    TUint predict() const noexcept {
        return table[hash] + prev_value;
    }

    void add(TUint value) noexcept {
        table[hash] = value - prev_value;
        recalculateHash();
        prev_value = value;
    }

private:
    void recalculateHash() noexcept {
        auto value = table[hash];
        if constexpr (sizeof(TUint) >= 8) {
            hash = ((hash << 2) ^ static_cast<std::size_t>(value >> 40)) & (table.size() - 1);
        } else {
            hash = ((hash << 4) ^ static_cast<std::size_t>(value >> 23)) & (table.size() - 1);
        }
    }

    std::vector<TUint> table;
    TUint prev_value{0};
    std::size_t hash{0};
};

template <std::unsigned_integral TUint> requires (sizeof(TUint) >= 4)
class FcmPredictor {
public:
    explicit FcmPredictor(std::size_t table_size)
        : table(table_size, 0)
        , hash{0} {
    }

    [[nodiscard]]
    TUint predict() const noexcept {
        return table[hash];
    }

    void add(TUint value) noexcept {
        table[hash] = value;
        recalculateHash();
    }

private:
    void recalculateHash() noexcept {
        auto value = table[hash];
        if constexpr (sizeof(TUint) >= 8) {
            hash = ((hash << 6) ^ static_cast<std::size_t>(value >> 48)) & (table.size() - 1);
        } else {
            hash = ((hash << 1) ^ static_cast<std::size_t>(value >> 22)) & (table.size() - 1);
        }
    }

    std::vector<TUint> table;
    std::size_t hash{0};
};

template <std::unsigned_integral TUint, std::size_t CHUNK_SIZE, std::endian Endian = std::endian::native> requires (
    Endian == std::endian::little || Endian == std::endian::big)
class FPCOperation {
    static constexpr auto VALUE_SIZE = sizeof(TUint);
    static constexpr std::byte DFCM_BIT_1{1u << 7};
    static constexpr std::byte DFCM_BIT_2{1u << 3};
    static constexpr unsigned MAX_COMPRESSED_SIZE{0b111u};

public:
    explicit FPCOperation(std::span<std::byte> destination, UInt8 compression_level)
        : dfcm_predictor(1 << compression_level)
        , fcm_predictor(1 << compression_level)
        , chunk{}
        , result{destination} {
    }

    std::size_t encode(std::span<const std::byte> data)&& {
        auto initial_size = result.size();

        std::span chunk_view(chunk);
        for (std::size_t i = 0; i < data.size(); i += chunk_view.size_bytes()) {
            auto written_values = importChunk(data.subspan(i), chunk_view);
            encodeChunk(chunk_view.subspan(0, written_values));
        }

        return initial_size - result.size();
    }

    void decode(std::span<const std::byte> values, std::size_t decoded_size)&& {
        std::size_t read_bytes{0};

        std::span<TUint> chunk_view(chunk);
        for (std::size_t i = 0; i < decoded_size; i += chunk_view.size_bytes()) {
            if (i + chunk_view.size_bytes() > decoded_size)
                chunk_view = chunk_view.first(ceilBytesToEvenValues(decoded_size - i));
            read_bytes += decodeChunk(values.subspan(read_bytes), chunk_view);
            exportChunk(chunk_view);
        }
    }

private:
    static std::size_t ceilBytesToEvenValues(std::size_t bytes_count) {
        auto values_count = (bytes_count + VALUE_SIZE - 1) / VALUE_SIZE;
        return values_count % 2 == 0 ? values_count : values_count + 1;
    }

    std::size_t importChunk(std::span<const std::byte> values, std::span<TUint> chnk) {
        if (auto chunk_view = std::as_writable_bytes(chnk); chunk_view.size() <= values.size()) {
            std::memcpy(chunk_view.data(), values.data(), chunk_view.size());
            return chunk_view.size() / VALUE_SIZE;
        } else {
            std::memset(chunk_view.data(), 0, chunk_view.size());
            std::memcpy(chunk_view.data(), values.data(), values.size());
            return ceilBytesToEvenValues(values.size());
        }
    }

    void exportChunk(std::span<const TUint> chnk) {
        auto chunk_view = std::as_bytes(chnk).first(std::min(result.size(), chnk.size_bytes()));
        std::memcpy(result.data(), chunk_view.data(), chunk_view.size());
        result = result.subspan(chunk_view.size());
    }

    void encodeChunk(std::span<const TUint> seq) {
        for (std::size_t i = 0; i < seq.size(); i += 2) {
            encodePair(seq[i], seq[i + 1]);
        }
    }

    struct CompressedValue {
        TUint value;
        unsigned compressed_size;
        bool is_dfcm_predictor;
    };

    unsigned encodeCompressedSize(int compressed) {
        if constexpr (VALUE_SIZE > MAX_COMPRESSED_SIZE) {
            if (compressed >= 4)
                --compressed;
        }
        return std::min(static_cast<unsigned>(compressed), MAX_COMPRESSED_SIZE);
    }

    unsigned decodeCompressedSize(unsigned encoded_size) {
        if constexpr (VALUE_SIZE > MAX_COMPRESSED_SIZE) {
            if (encoded_size > 3)
                ++encoded_size;
        }
        return encoded_size;
    }

    CompressedValue compressValue(TUint value) noexcept {
        TUint compressed_dfcm = dfcm_predictor.predict() ^ value;
        TUint compressed_fcm = fcm_predictor.predict() ^ value;
        dfcm_predictor.add(value);
        fcm_predictor.add(value);
        auto zeroes_dfcm = std::countl_zero(compressed_dfcm);
        auto zeroes_fcm = std::countl_zero(compressed_fcm);
        if (zeroes_dfcm > zeroes_fcm)
            return {compressed_dfcm, encodeCompressedSize(zeroes_dfcm / CHAR_BIT), true};
        return {compressed_fcm, encodeCompressedSize(zeroes_fcm / CHAR_BIT), false};
    }

    void encodePair(TUint first, TUint second) {
        auto[value1, compressed_size1, is_dfcm_predictor1] = compressValue(first);
        auto[value2, compressed_size2, is_dfcm_predictor2] = compressValue(second);
        std::byte header{0x0};
        if (is_dfcm_predictor1)
            header |= DFCM_BIT_1;
        if (is_dfcm_predictor2)
            header |= DFCM_BIT_2;
        header |= static_cast<std::byte>((compressed_size1 << 4) | compressed_size2);
        result.front() = header;

        compressed_size1 = decodeCompressedSize(compressed_size1);
        compressed_size2 = decodeCompressedSize(compressed_size2);
        auto tail_size1 = VALUE_SIZE - compressed_size1;
        auto tail_size2 = VALUE_SIZE - compressed_size2;

        std::memcpy(result.data() + 1, valueTail(value1, compressed_size1), tail_size1);
        std::memcpy(result.data() + 1 + tail_size1, valueTail(value2, compressed_size2), tail_size2);
        result = result.subspan(1 + tail_size1 + tail_size2);
    }

    std::size_t decodeChunk(std::span<const std::byte> values, std::span<TUint> seq) {
        std::size_t read_bytes{0};
        for (std::size_t i = 0; i < seq.size(); i += 2) {
            read_bytes += decodePair(values.subspan(read_bytes), seq[i], seq[i + 1]);
        }
        return read_bytes;
    }

    TUint decompressValue(TUint value, bool isDfcmPredictor) {
        TUint decompressed;
        if (isDfcmPredictor) {
            decompressed = dfcm_predictor.predict() ^ value;
        } else {
            decompressed = fcm_predictor.predict() ^ value;
        }
        dfcm_predictor.add(decompressed);
        fcm_predictor.add(decompressed);
        return decompressed;
    }

    std::size_t decodePair(std::span<const std::byte> bytes, TUint& first, TUint& second) {
        if (bytes.empty())
            throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Unexpected end of encoded sequence");

        auto compressed_size1 = decodeCompressedSize(static_cast<unsigned>(bytes.front() >> 4) & MAX_COMPRESSED_SIZE);
        auto compressed_size2 = decodeCompressedSize(static_cast<unsigned>(bytes.front()) & MAX_COMPRESSED_SIZE);

        auto tail_size1 = VALUE_SIZE - compressed_size1;
        auto tail_size2 = VALUE_SIZE - compressed_size2;

        if (bytes.size() < 1 + tail_size1 + tail_size2)
            throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Unexpected end of encoded sequence");

        TUint value1{0};
        TUint value2{0};

        std::memcpy(valueTail(value1, compressed_size1), bytes.data() + 1, tail_size1);
        std::memcpy(valueTail(value2, compressed_size2), bytes.data() + 1 + tail_size1, tail_size2);

        auto is_dfcm_predictor1 = static_cast<unsigned char>(bytes.front() & DFCM_BIT_1);
        auto is_dfcm_predictor2 = static_cast<unsigned char>(bytes.front() & DFCM_BIT_2);
        first = decompressValue(value1, is_dfcm_predictor1 != 0);
        second = decompressValue(value2, is_dfcm_predictor2 != 0);

        return 1 + tail_size1 + tail_size2;
    }

    static void* valueTail(TUint& value, unsigned compressed_size) {
        if constexpr (Endian == std::endian::little) {
            return &value;
        } else {
            return reinterpret_cast<std::byte*>(&value) + compressed_size;
        }
    }

    DfcmPredictor<TUint> dfcm_predictor;
    FcmPredictor<TUint> fcm_predictor;
    std::array<TUint, CHUNK_SIZE> chunk{};
    std::span<std::byte> result{};
};

}

template <std::size_t CHUNK_SIZE>
UInt32 CompressionCodecFPC<CHUNK_SIZE>::doCompressData(const char* source, UInt32 source_size, char* dest) const {
    dest[0] = static_cast<char>(float_width);
    dest[1] = static_cast<char>(level);
    dest[2] = static_cast<char>(encodeEndianness(std::endian::native));

    auto destination = std::as_writable_bytes(std::span(dest, source_size).subspan(HEADER_SIZE));
    auto src = std::as_bytes(std::span(source, source_size));
    switch (float_width) {
        case sizeof(Float64):
            return HEADER_SIZE + FPCOperation<UInt64, CHUNK_SIZE>(destination, level).encode(src);
        case sizeof(Float32):
            return HEADER_SIZE + FPCOperation<UInt32, CHUNK_SIZE>(destination, level).encode(src);
        default:
            break;
    }
    throw Exception("Cannot decompress. File has incorrect float width", ErrorCodes::CANNOT_DECOMPRESS);
}

template <std::size_t CHUNK_SIZE>
void CompressionCodecFPC<CHUNK_SIZE>::doDecompressData(
    const char* source,
    UInt32 source_size,
    char* dest,
    UInt32 uncompressed_size) const {
    if (source_size < HEADER_SIZE)
        throw Exception("Cannot decompress. File has wrong header", ErrorCodes::CANNOT_DECOMPRESS);

    auto compressed_data = std::span(source, source_size);
    if (static_cast<UInt8>(compressed_data[0]) != float_width)
        throw Exception("Cannot decompress. File has incorrect float width", ErrorCodes::CANNOT_DECOMPRESS);
    if (static_cast<UInt8>(compressed_data[1]) != level)
        throw Exception("Cannot decompress. File has incorrect compression level", ErrorCodes::CANNOT_DECOMPRESS);
    if (decodeEndianness(static_cast<UInt8>(compressed_data[2])) != std::endian::native)
        throw Exception("Cannot decompress. File has incorrect endianness", ErrorCodes::CANNOT_DECOMPRESS);

    auto destination = std::as_writable_bytes(std::span(dest, uncompressed_size));
    auto src = std::as_bytes(compressed_data.subspan(HEADER_SIZE));
    switch (float_width) {
        case sizeof(Float64):
            FPCOperation<UInt64, CHUNK_SIZE>(destination, level).decode(src, uncompressed_size);
            break;
        case sizeof(Float32):
            FPCOperation<UInt32, CHUNK_SIZE>(destination, level).decode(src, uncompressed_size);
            break;
        default:
            break;
    }
}
}

using Float = double;

std::vector<Float> GenTest() {
    std::mt19937_64 rnd{1488};
    std::uniform_int_distribution size_dist(1'000'000, 1'000'000);
    std::uniform_real_distribution<Float> val_dist(std::numeric_limits<Float>::min(), std::numeric_limits<Float>::max());
    auto count = size_dist(rnd);
    std::vector<Float> inp;
    inp.reserve(count);
    for (int i = 0; i < count; ++i) {
        inp.push_back(val_dist(rnd));
    }
    return inp;
}

static void Level2(benchmark::State& state) {
    auto inp = GenTest();
    DB::CompressionCodecFPC<64> codec(sizeof(Float), 2);
    std::vector<std::byte> encoded;
    encoded.resize(codec.getMaxCompressedDataSize(inp.size() * sizeof(Float)));
    std::vector<Float> decoded(inp.size(), 0);
    for (auto _ : state) {
        auto compressed = codec.doCompressData((char*)inp.data(), inp.size() * sizeof(Float), (char*)encoded.data());
    }
}
// Register the function as a benchmark
BENCHMARK(Level2);

static void Level4(benchmark::State& state) {
    auto inp = GenTest();
    DB::CompressionCodecFPC<64> codec(sizeof(Float), 4);
    std::vector<std::byte> encoded;
    encoded.resize(codec.getMaxCompressedDataSize(inp.size() * sizeof(Float)));
    std::vector<Float> decoded(inp.size(), 0);
    for (auto _ : state) {
        auto compressed = codec.doCompressData((char*)inp.data(), inp.size() * sizeof(Float), (char*)encoded.data());
    }
}
// Register the function as a benchmark
BENCHMARK(Level4);

static void Level8(benchmark::State& state) {
    auto inp = GenTest();
    DB::CompressionCodecFPC<64> codec(sizeof(Float), 8);
    std::vector<std::byte> encoded;
    encoded.resize(codec.getMaxCompressedDataSize(inp.size() * sizeof(Float)));
    std::vector<Float> decoded(inp.size(), 0);
    for (auto _ : state) {
        auto compressed = codec.doCompressData((char*)inp.data(), inp.size() * sizeof(Float), (char*)encoded.data());
    }
}
// Register the function as a benchmark
BENCHMARK(Level8);

static void Level12(benchmark::State& state) {
    auto inp = GenTest();
    DB::CompressionCodecFPC<64> codec(sizeof(Float), 12);
    std::vector<std::byte> encoded;
    encoded.resize(codec.getMaxCompressedDataSize(inp.size() * sizeof(Float)));
    std::vector<Float> decoded(inp.size(), 0);
    for (auto _ : state) {
        auto compressed = codec.doCompressData((char*)inp.data(), inp.size() * sizeof(Float), (char*)encoded.data());
    }
}
// Register the function as a benchmark
BENCHMARK(Level12);

static void Level14(benchmark::State& state) {
    auto inp = GenTest();
    DB::CompressionCodecFPC<64> codec(sizeof(Float), 14);
    std::vector<std::byte> encoded;
    encoded.resize(codec.getMaxCompressedDataSize(inp.size() * sizeof(Float)));
    std::vector<Float> decoded(inp.size(), 0);
    for (auto _ : state) {
        auto compressed = codec.doCompressData((char*)inp.data(), inp.size() * sizeof(Float), (char*)encoded.data());
    }
}
BENCHMARK(Level14);

static void Level16(benchmark::State& state) {
    auto inp = GenTest();
    DB::CompressionCodecFPC<64> codec(sizeof(Float), 16);
    std::vector<std::byte> encoded;
    encoded.resize(codec.getMaxCompressedDataSize(inp.size() * sizeof(Float)));
    std::vector<Float> decoded(inp.size(), 0);
    for (auto _ : state) {
        auto compressed = codec.doCompressData((char*)inp.data(), inp.size() * sizeof(Float), (char*)encoded.data());
    }
}
BENCHMARK(Level16);
