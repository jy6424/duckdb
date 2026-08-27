//===----------------------------------------------------------------------===//
//                         DuckDB
//
// thrift_tools.hpp
//
//
//===----------------------------------------------------------------------===/

#pragma once

#include <condition_variable>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <exception>
#include <list>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <vector>
#include "thrift/protocol/TCompactProtocol.h"
#include "thrift/transport/TBufferTransports.h"

#include "duckdb.hpp"
#include "duckdb/storage/caching_file_system.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/allocator.hpp"

namespace duckdb {

// A ReadHead for prefetching data in a specific range
struct ReadHead {
	ReadHead(idx_t location, uint64_t size) : location(location), size(size) {};
	// Hint info
	idx_t location;
	uint64_t size;

	// Current info
	BufferHandle buffer_handle;
	data_ptr_t buffer_ptr;
	bool data_isset = false;
	bool read_complete = false;
	bool read_scheduled = false;
	std::exception_ptr read_error;

	idx_t GetEnd() const {
		return size + location;
	}
};

// Comparator for ReadHeads that are either overlapping, adjacent, or within ALLOW_GAP bytes from each other
struct ReadHeadComparator {
	static constexpr uint64_t ALLOW_GAP = 1 << 14; // 16 KiB
	bool operator()(const ReadHead *a, const ReadHead *b) const {
		auto a_start = a->location;
		auto a_end = a->location + a->size;
		auto b_start = b->location;

		if (a_end <= NumericLimits<idx_t>::Maximum() - ALLOW_GAP) {
			a_end += ALLOW_GAP;
		}

		return a_start < b_start && a_end < b_start;
	}
};

// Two-step read ahead buffer
// 1: register all ranges that will be read, merging ranges that are consecutive
// 2: prefetch all registered ranges
struct ReadAheadBuffer {
	explicit ReadAheadBuffer(CachingFileHandle &file_handle_p) : file_handle(file_handle_p) {
	}

	~ReadAheadBuffer() {
		WaitForPrefetch();
	}

	// The list of read heads
	std::list<ReadHead> read_heads;
	// Set for merging consecutive ranges
	std::set<ReadHead *, ReadHeadComparator> merge_set;

	CachingFileHandle &file_handle;

	idx_t total_size = 0;

	// Add a read head to the prefetching list
	void AddReadHead(idx_t pos, uint64_t len, bool merge_buffers = true) {
		// Attempt to merge with existing
		if (merge_buffers) {
			ReadHead new_read_head {pos, len};
			auto lookup_set = merge_set.find(&new_read_head);
			if (lookup_set != merge_set.end()) {
				auto existing_head = *lookup_set;
				auto new_start = MinValue<idx_t>(existing_head->location, new_read_head.location);
				auto new_length = MaxValue<idx_t>(existing_head->GetEnd(), new_read_head.GetEnd()) - new_start;
				existing_head->location = new_start;
				existing_head->size = new_length;
				return;
			}
		}

		read_heads.emplace_front(ReadHead(pos, len));
		total_size += len;
		auto &read_head = read_heads.front();

		if (merge_buffers) {
			merge_set.insert(&read_head);
		}

		if (read_head.GetEnd() > file_handle.GetFileSize()) {
			throw std::runtime_error("Prefetch registered for bytes outside file: " + file_handle.GetPath() +
			                         ", attempted range: [" + std::to_string(pos) + ", " +
			                         std::to_string(read_head.GetEnd()) +
			                         "), file size: " + std::to_string(file_handle.GetFileSize()));
		}
	}

	// Returns the relevant read head
	ReadHead *GetReadHead(idx_t pos) {
		for (auto &read_head : read_heads) {
			if (pos >= read_head.location && pos < read_head.GetEnd()) {
				return &read_head;
			}
		}
		return nullptr;
	}

	// Prefetch all read heads
	void Prefetch() {
		WaitForPrefetch();
		StartPrefetch(false);
	}

	void PrefetchPipeline() {
		{
			std::lock_guard<std::mutex> guard(prefetch_lock);
			if (!pipeline_started) {
				pipeline_started = true;
				pipeline_stop = false;
				auto worker_count = AsyncPrefetchWorkers();
				for (idx_t worker_idx = 0; worker_idx < worker_count; worker_idx++) {
					prefetch_threads.emplace_back([this]() { PipelinePrefetchWorker(); });
				}
			}
			for (auto &read_head : read_heads) {
				if (read_head.read_complete || read_head.read_scheduled) {
					continue;
				}
				ValidateReadHead(read_head);
				read_head.read_error = std::exception_ptr();
				read_head.data_isset = false;
				read_head.read_scheduled = true;
				pipeline_queue.push_back(&read_head);
			}
		}
		prefetch_cv.notify_all();
	}

	bool TryClearCompleted() {
		{
			std::lock_guard<std::mutex> guard(prefetch_lock);
			if (!pipeline_queue.empty() || active_pipeline_reads > 0) {
				return false;
			}
			for (auto &read_head : read_heads) {
				if (!read_head.read_complete) {
					return false;
				}
			}
		}
		{
			std::lock_guard<std::mutex> guard(prefetch_lock);
			read_heads.clear();
			merge_set.clear();
			total_size = 0;
		}
		return true;
	}

	void StartPrefetch(bool non_blocking) {
		{
			std::lock_guard<std::mutex> guard(prefetch_lock);
			for (auto &read_head : read_heads) {
				if (non_blocking && read_head.read_complete) {
					continue;
				}
				ValidateReadHead(read_head);
				read_head.read_complete = false;
				read_head.read_scheduled = false;
				read_head.read_error = std::exception_ptr();
				read_head.data_isset = false;
			}
		}
		if (!read_heads.empty() && AsyncPrefetchEnabled() && (read_heads.size() > 1 || AsyncSinglePrefetchEnabled())) {
			next_prefetch = read_heads.begin();
			auto worker_count = MinValue<idx_t>(AsyncPrefetchWorkers(), read_heads.size());
			if (!non_blocking) {
				prefetch_threads.clear();
			}
			for (idx_t worker_idx = 0; worker_idx < worker_count; worker_idx++) {
				prefetch_threads.emplace_back([this]() { PrefetchWorker(); });
			}
			return;
		}
		for (auto &read_head : read_heads) {
			ReadInto(read_head);
		}
	}

	void WaitForReadHead(ReadHead &read_head) {
		if (prefetch_threads.empty() && !read_head.read_complete) {
			ReadInto(read_head);
			return;
		}
		{
			std::unique_lock<std::mutex> guard(prefetch_lock);
			prefetch_cv.wait(guard, [&]() { return read_head.read_complete; });
		}
		if (read_head.read_error) {
			std::rethrow_exception(read_head.read_error);
		}
	}

	bool ReadHeadComplete(ReadHead &read_head) {
		std::lock_guard<std::mutex> guard(prefetch_lock);
		return read_head.read_complete;
	}

	void Clear() {
		WaitForPrefetch();
		read_heads.clear();
		merge_set.clear();
		total_size = 0;
	}

private:
	static bool ReadAheadEnvFlag(const char *name) {
		auto value = std::getenv(name);
		if (!value || !value[0]) {
			return false;
		}
		return strcmp(value, "1") == 0 || strcmp(value, "true") == 0 || strcmp(value, "TRUE") == 0 ||
		       strcmp(value, "yes") == 0 || strcmp(value, "YES") == 0 || strcmp(value, "on") == 0 ||
		       strcmp(value, "ON") == 0;
	}

	static bool AsyncPrefetchEnabled() {
		if (ReadAheadEnvFlag("DUCKDB_PARQUET_PIPELINED_PAGE_READ")) {
			return true;
		}
		auto value = std::getenv("DUCKDB_PARQUET_ASYNC_PREFETCH");
		if (!value || !value[0]) {
			return false;
		}
		return ReadAheadEnvFlag("DUCKDB_PARQUET_ASYNC_PREFETCH");
	}

	static bool AsyncSinglePrefetchEnabled() {
		if (ReadAheadEnvFlag("DUCKDB_PARQUET_PIPELINED_PAGE_READ")) {
			return true;
		}
		auto value = std::getenv("DUCKDB_PARQUET_ASYNC_SINGLE_PREFETCH");
		if (!value || !value[0]) {
			return false;
		}
		return ReadAheadEnvFlag("DUCKDB_PARQUET_ASYNC_SINGLE_PREFETCH");
	}

	static idx_t AsyncPrefetchWorkers() {
		auto value = std::getenv("DUCKDB_PARQUET_ASYNC_PREFETCH_WORKERS");
		if (!value || !value[0]) {
			return 1;
		}
		char *end = nullptr;
		auto parsed = std::strtoull(value, &end, 10);
		if (!end || *end != '\0' || parsed == 0) {
			return 1;
		}
		return MinValue<idx_t>(UnsafeNumericCast<idx_t>(parsed), 16);
	}

	void ValidateReadHead(ReadHead &read_head) {
		if (read_head.GetEnd() > file_handle.GetFileSize()) {
			throw std::runtime_error("Prefetch registered requested for bytes outside file");
		}
	}

	void ReadInto(ReadHead &read_head) {
		try {
			read_head.buffer_handle = file_handle.Read(read_head.buffer_ptr, read_head.size, read_head.location);
			D_ASSERT(read_head.buffer_handle.IsValid());
			{
				std::lock_guard<std::mutex> guard(prefetch_lock);
				read_head.data_isset = true;
				read_head.read_complete = true;
				read_head.read_scheduled = false;
			}
		} catch (...) {
			std::lock_guard<std::mutex> guard(prefetch_lock);
			read_head.read_error = std::current_exception();
			read_head.read_complete = true;
			read_head.read_scheduled = false;
		}
		prefetch_cv.notify_all();
	}

	void PrefetchWorker() {
		while (true) {
			ReadHead *read_head = nullptr;
			{
				std::lock_guard<std::mutex> guard(prefetch_worker_lock);
				if (next_prefetch == read_heads.end()) {
					return;
				}
				read_head = &*next_prefetch;
				next_prefetch++;
			}
			ReadInto(*read_head);
		}
	}

	void PipelinePrefetchWorker() {
		while (true) {
			ReadHead *read_head = nullptr;
			{
				std::unique_lock<std::mutex> guard(prefetch_lock);
				prefetch_cv.wait(guard, [&]() { return pipeline_stop || !pipeline_queue.empty(); });
				if (pipeline_stop && pipeline_queue.empty()) {
					return;
				}
				read_head = pipeline_queue.front();
				pipeline_queue.pop_front();
				active_pipeline_reads++;
			}
			ReadInto(*read_head);
			{
				std::lock_guard<std::mutex> guard(prefetch_lock);
				active_pipeline_reads--;
			}
			prefetch_cv.notify_all();
		}
	}

	void WaitForPrefetch() {
		if (pipeline_started) {
			{
				std::lock_guard<std::mutex> guard(prefetch_lock);
				pipeline_stop = true;
			}
			prefetch_cv.notify_all();
		}
		for (auto &thread : prefetch_threads) {
			if (thread.joinable()) {
				thread.join();
			}
		}
		prefetch_threads.clear();
		pipeline_queue.clear();
		pipeline_started = false;
		pipeline_stop = false;
		active_pipeline_reads = 0;
	}

	std::mutex prefetch_lock;
	std::mutex prefetch_worker_lock;
	std::condition_variable prefetch_cv;
	std::vector<std::thread> prefetch_threads;
	std::list<ReadHead>::iterator next_prefetch;
	std::deque<ReadHead *> pipeline_queue;
	bool pipeline_started = false;
	bool pipeline_stop = false;
	idx_t active_pipeline_reads = 0;
};

class ThriftFileTransport : public duckdb_apache::thrift::transport::TVirtualTransport<ThriftFileTransport> {
public:
	static constexpr uint64_t PREFETCH_FALLBACK_BUFFERSIZE = 1000000;

	ThriftFileTransport(CachingFileHandle &file_handle_p, bool prefetch_mode_p)
	    : file_handle(file_handle_p), location(0), size(file_handle.GetFileSize()),
	      ra_buffer(file_handle), prefetch_mode(prefetch_mode_p) {
	}

	uint32_t read(uint8_t *buf, uint32_t len) {
		auto prefetch_buffer = ra_buffer.GetReadHead(location);
		if (prefetch_buffer != nullptr && location - prefetch_buffer->location + len <= prefetch_buffer->size) {
			D_ASSERT(location - prefetch_buffer->location + len <= prefetch_buffer->size);

			if (ShouldBypassPendingPrefetch(len) && !ra_buffer.ReadHeadComplete(*prefetch_buffer)) {
				file_handle.GetFileHandle().Read(context, buf, len, location);
			} else {
				ra_buffer.WaitForReadHead(*prefetch_buffer);
				D_ASSERT(prefetch_buffer->buffer_handle.IsValid());
				memcpy(buf, prefetch_buffer->buffer_ptr + location - prefetch_buffer->location, len);
			}
		} else if (prefetch_mode && len < PREFETCH_FALLBACK_BUFFERSIZE && len > 0) {
			Prefetch(location, MinValue<uint64_t>(PREFETCH_FALLBACK_BUFFERSIZE, file_handle.GetFileSize() - location));
			auto prefetch_buffer_fallback = ra_buffer.GetReadHead(location);
			D_ASSERT(location - prefetch_buffer_fallback->location + len <= prefetch_buffer_fallback->size);
			ra_buffer.WaitForReadHead(*prefetch_buffer_fallback);
			memcpy(buf, prefetch_buffer_fallback->buffer_ptr + location - prefetch_buffer_fallback->location, len);
		} else {
			// No prefetch, do a regular (non-caching) read
			file_handle.GetFileHandle().Read(context, buf, len, location);
		}

		location += len;
		return len;
	}

	// Prefetch a single buffer
	void Prefetch(idx_t pos, uint64_t len) {
		RegisterPrefetch(pos, len, false);
		FinalizeRegistration();
		PrefetchRegistered();
	}

	// Register a buffer for prefixing
	void RegisterPrefetch(idx_t pos, uint64_t len, bool can_merge = true) {
		ra_buffer.AddReadHead(pos, len, can_merge);
	}

	// Prevents any further merges, should be called before PrefetchRegistered
	void FinalizeRegistration() {
		ra_buffer.merge_set.clear();
	}

	// Prefetch all previously registered ranges
	void PrefetchRegistered() {
		ra_buffer.Prefetch();
	}

	void PrefetchRegisteredPipeline() {
		ra_buffer.PrefetchPipeline();
	}

	bool TryClearCompletedPrefetch() {
		return ra_buffer.TryClearCompleted();
	}

	void ClearPrefetch() {
		ra_buffer.Clear();
	}

	void Skip(idx_t skip_count) {
		location += skip_count;
	}

	bool HasPrefetch() const {
		return !ra_buffer.read_heads.empty() || !ra_buffer.merge_set.empty();
	}

	void SetLocation(idx_t location_p) {
		location = location_p;
	}

	idx_t GetLocation() const {
		return location;
	}

	optional_ptr<ReadHead> GetReadHead(idx_t pos) {
		return ra_buffer.GetReadHead(pos);
	}

	idx_t GetSize() const {
		return size;
	}

private:
	QueryContext context;

	static bool EnvFlag(const char *name) {
		auto value = std::getenv(name);
		if (!value || !value[0]) {
			return false;
		}
		return strcmp(value, "1") == 0 || strcmp(value, "true") == 0 || strcmp(value, "TRUE") == 0 ||
		       strcmp(value, "yes") == 0 || strcmp(value, "YES") == 0 || strcmp(value, "on") == 0 ||
		       strcmp(value, "ON") == 0;
	}

	static uint32_t HeaderBypassBytes() {
		auto value = std::getenv("DUCKDB_PARQUET_PREFETCH_HEADER_BYPASS_BYTES");
		if (!value || !value[0]) {
			return 4096;
		}
		char *end = nullptr;
		auto parsed = std::strtoull(value, &end, 10);
		if (!end || *end != '\0' || parsed == 0) {
			return 4096;
		}
		return UnsafeNumericCast<uint32_t>(MinValue<uint64_t>(parsed, 1ULL << 20));
	}

	static bool ShouldBypassPendingPrefetch(uint32_t len) {
		return EnvFlag("DUCKDB_PARQUET_COLUMN_CHUNK_PREFETCH") && len <= HeaderBypassBytes();
	}

	CachingFileHandle &file_handle;
	idx_t location;
	idx_t size;

	// Multi-buffer prefetch
	ReadAheadBuffer ra_buffer;

	// Whether the prefetch mode is enabled. In this mode the DirectIO flag of the handle will be set and the parquet
	// reader will manage the read buffering.
	bool prefetch_mode;
};

} // namespace duckdb
