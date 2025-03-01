#ifndef STATS_BUFFER_H_
#define STATS_BUFFER_H_

#include <csignal>
#include <fstream>
#include <iostream>
#include <list>
#include <mutex>
#include <sstream>
#include <string>

class Buffer {
private:
  std::stringstream buffer;
  std::ofstream output_file;
  size_t buffer_limit;

  static std::list<Buffer *> instances_;
  static std::mutex mutex_;

  static void handle_signal(int signal);

public:
  Buffer(const std::string &filename, size_t limit = 10 * 1024 * 1024);

  ~Buffer();

  template <typename T> Buffer &operator<<(const T &data);

  Buffer &operator<<(std::ostream &(*manip)(std::ostream &));

  void flush();

  void register_instance();
  void unregister_instance();
  void register_signals();
};

template <typename T> Buffer &Buffer::operator<<(const T &data) {
  buffer << data;
  if (buffer.tellp() >= static_cast<std::streampos>(buffer_limit)) {
    flush();
  }
  return *this;
}

#endif // STATS_BUFFER_H_