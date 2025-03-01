#include "buffer.h"

std::list<Buffer *> Buffer::instances_;
std::mutex Buffer::mutex_;

Buffer::Buffer(const std::string &filename, size_t limit)
    : buffer_limit(limit) {
  output_file.open(filename, std::ios::out | std::ios::trunc);
  if (!output_file.is_open()) {
    throw std::runtime_error("Failed to open output file: " + filename);
  }

  register_instance();
  register_signals();
}

Buffer::~Buffer() {
  flush();
  if (output_file.is_open()) {
    output_file.close();
  }

  unregister_instance();
}

Buffer &Buffer::operator<<(std::ostream &(*manip)(std::ostream &)) {
  buffer << manip;
  if (buffer.tellp() >= static_cast<std::streampos>(buffer_limit)) {
    flush();
  }
  return *this;
}

void Buffer::flush() {
  if (buffer.tellp() > 0) {
    output_file << buffer.str();
    output_file.flush();
    buffer.str("");
    buffer.clear();
  }
}

void Buffer::register_instance() {
  std::lock_guard<std::mutex> lock(mutex_);
  instances_.push_back(this);
}

void Buffer::unregister_instance() {
  std::lock_guard<std::mutex> lock(mutex_);
  instances_.remove(this);
}

void Buffer::register_signals() {
  std::signal(SIGTERM, handle_signal);
  std::signal(SIGINT, handle_signal);
}

void Buffer::handle_signal(int signal) {
  std::lock_guard<std::mutex> lock(mutex_);

  for (Buffer *instance : instances_) {
    try {
      instance->flush();
      std::cerr << "Flushed buffer due to signal: " << signal << std::endl;
    } catch (const std::exception &e) {
      std::cerr << "Failed to flush buffer during signal handling: " << e.what()
                << std::endl;
    }
  }
  std::exit(signal);
}