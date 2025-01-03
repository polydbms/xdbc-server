#include "WebSocketClient.h"
#include <iostream>
#include <thread>

WebSocketClient::WebSocketClient(const std::string& host, const std::string& port)
    : host_(host), port_(port), resolver_(ioc_), ws_(ioc_), timer_(ioc_), active_(false), stop_thread_(false), operation_started_(false) {}

void WebSocketClient::start() {
    try {
        // Resolve host and port
        auto results = resolver_.resolve(host_, port_);
        // Connect to the first resolved endpoint
        beast::get_lowest_layer(ws_).connect(*results.begin());
        // Perform WebSocket handshake
        ws_.handshake(host_, "/xdbc-client");

        // Send request to start the routine operation
        json start_request = {{"operation", "start_transfer"}};
        ws_.write(asio::buffer(start_request.dump()));
        spdlog::info("Sent start request: {}", start_request.dump());

        // Wait for acknowledgment from the server
        beast::flat_buffer buffer;
        bool acknowledged = false; // Track acknowledgment status
        auto start_time = std::chrono::steady_clock::now();
        const std::chrono::seconds timeout(10); // Set a timeout duration

        while (!acknowledged) {
            // Check for timeout
            auto elapsed = std::chrono::steady_clock::now() - start_time;
            if (elapsed > timeout) {
                spdlog::error("Timeout waiting for server acknowledgment.");
                throw std::runtime_error("Server acknowledgment timeout");
            }

            // Attempt to read the acknowledgment
            try {
                ws_.read(buffer);
                std::string ack_response = beast::buffers_to_string(buffer.data());
                spdlog::info("Received acknowledgment: {}", ack_response);

                // Parse and check acknowledgment
                json ack_json = json::parse(ack_response);
                if (ack_json["operation"] == "acknowledged") {
                    acknowledged = true;
                    operation_started_ = true; // Set flag indicating acknowledgment received
                    spdlog::info("Server acknowledged the start request.");
                } else {
                    spdlog::warn("Server response does not acknowledge start: {}", ack_json.dump());
                    //throw std::runtime_error("Server rejected start request");
                }
            } catch (const std::exception& e) {
                spdlog::error("Error while waiting for acknowledgment: {}", e.what());
                // Optional: Retry after a short delay
                std::this_thread::sleep_for(std::chrono::milliseconds(500));
            }
        }
    } catch (const std::exception& e) {
        spdlog::error("WebSocket Client Error during start: {}", e.what());
        throw; // Rethrow the exception to notify the caller
    }
}

void WebSocketClient::periodic_communication() {
    try {
        while (!stop_thread_) {
            // Convert metrics to JSON and send it
            json metrics_json = metrics_convert_();
            //json metrics_json = {{"waiting_time", "100ms"}};
            json request_json = {
                {"operation", "get_environment"},
                {"payload", metrics_json}  // Include metrics in the payload
            };
            ws_.write(asio::buffer(request_json.dump()));
            std::string metrics_dump = metrics_json.dump();
            ////spdlog::info("Sending JSON: {}", metrics_dump);  // Log the JSON being sent

            // Read response from server
            beast::flat_buffer buffer;
            ws_.read(buffer);
            std::string env_response = beast::buffers_to_string(buffer.data());
            ////spdlog::info("Received JSON: {}", env_response);  // Log the received JSON

            // Parse and process the response
            json env_json = json::parse(env_response);
            if (env_json["operation"] == "set_environment") {
                json payload = env_json["payload"];
                env_convert_(payload);  // Process environment data from payload
            } else {
                spdlog::warn("Unexpected operation received: {}", env_json["operation"]);
            }

            // Wait for 1 second before next communication
            std::this_thread::sleep_for(std::chrono::seconds(1));
            active_ = true;
        }
    } catch (const std::exception& e) {
        std::cerr << "Error in periodic communication: " << e.what() << std::endl;
    }
}

void WebSocketClient::run(std::function<json()> metrics_convert,
                          std::function<void(const json&)> env_convert) {
    try {
        // Wait until the operation has started and acknowledgment is received
        while (!operation_started_) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));  // Wait briefly before checking again
        }

        // Store the conversion functions to be used in periodic communication
        metrics_convert_ = metrics_convert;
        env_convert_ = env_convert;

        // Once acknowledgment is received, start periodic communication in the same thread
        periodic_communication();

        ioc_.run();  // Start processing asynchronous operations
    } catch (const std::exception& e) {
        std::cerr << "Error in io_context run: " << e.what() << std::endl;
    }
}

void WebSocketClient::stop() {
    stop_thread_ = true;  // Signal thread to stop

    try {
        if (ws_.is_open()) {
            // Send the "finished" message to the server
            json stop_message = {{"operation", "finished"}};
            ws_.write(asio::buffer(stop_message.dump()));
            spdlog::info("Sent stop message: {}", stop_message.dump());

            // Gracefully close WebSocket connection
            ws_.close(beast::websocket::close_code::normal);
        }
        ioc_.stop();  // Stop the io_context loop
    } catch (const std::exception& e) {
        std::cerr << "Error during stop: " << e.what() << std::endl;
    }
}

// Check if the WebSocket client is active
bool WebSocketClient::is_active() const {
    return active_.load();
}
