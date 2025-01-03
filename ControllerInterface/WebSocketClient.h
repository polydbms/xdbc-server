#ifndef WEBSOCKETCLIENT_H
#define WEBSOCKETCLIENT_H

#include <nlohmann/json.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/core/buffers_to_string.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/steady_timer.hpp>
#include "spdlog/spdlog.h"
#include "spdlog/stopwatch.h"
#include "spdlog/sinks/stdout_color_sinks.h"
#include <thread>
#include <atomic>
#include <functional>
#include <string>

namespace beast = boost::beast;
namespace asio = boost::asio;
namespace websocket = beast::websocket;
using json = nlohmann::json;

class WebSocketClient {
public:
    // Constructor: Accept host and port for connection setup
    WebSocketClient(const std::string& host, const std::string& port);
    
    // Start method: Accept function parameters to store them in class members
    void start();

    // Stop the client and cleanup
    void stop();

    // Run io_context and handle periodic communication
    void run(std::function<json()> metrics_convert,
               std::function<void(const json&)> env_convert);

    // Get the status of the WebSocket client
    bool is_active() const;

private:
    // Periodic communication method that runs in a separate thread
    void periodic_communication();

    // Connection setup details
    std::string host_;
    std::string port_;
    asio::io_context ioc_;
    asio::ip::tcp::resolver resolver_;
    websocket::stream<asio::ip::tcp::socket> ws_;

    asio::steady_timer timer_;  // Timer for periodic communication

    // Function objects for metrics and environment conversion
    std::function<json()> metrics_convert_;
    std::function<void(const json&)> env_convert_;

    // Flag to control periodic communication thread
    std::atomic<bool> stop_thread_;
    std::atomic<bool> operation_started_;  // Indicates if the operation has started after acknowledgment
    std::thread periodic_thread_;  // Thread for periodic communication

    // Status variable: Indicates if the WebSocketClient is active
    std::atomic<bool> active_;
};

#endif // WEBSOCKETCLIENT_H
