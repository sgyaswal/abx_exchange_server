#include <iostream>
#include <boost/asio.hpp>
#include <nlohmann/json.hpp>
#include <vector>
#include <fstream>
#include <cstdint>

using boost::asio::ip::tcp;
using json = nlohmann::json;

// Structure to represent the stock packet
struct StockPacket {
    std::string symbol;
    char buysellindicator;
    int32_t quantity;
    int32_t price;
    int32_t packetSequence;
};

// Helper function to convert a received packet to StockPacket
StockPacket parsePacket(const std::vector<uint8_t>& packet) {
    StockPacket stockPacket;
    stockPacket.symbol = std::string(packet.begin(), packet.begin() + 4); // Symbol (4 bytes)
    stockPacket.buysellindicator = packet[4]; // Buy/Sell Indicator (1 byte)
    stockPacket.quantity = ntohl(*(reinterpret_cast<const int32_t*>(&packet[5]))); // Quantity (4 bytes)
    stockPacket.price = ntohl(*(reinterpret_cast<const int32_t*>(&packet[9]))); // Price (4 bytes)
    stockPacket.packetSequence = ntohl(*(reinterpret_cast<const int32_t*>(&packet[13]))); // Sequence (4 bytes)
    return stockPacket;
}

// Function to request missing sequences from the server
void requestMissingPacket(tcp::socket& socket, int32_t seq) {
    std::vector<uint8_t> request(2);
    request[0] = 2; // CallType 2 for resending packet
    request[1] = static_cast<uint8_t>(seq);
    
    // Ensure the socket is still open before writing
    if (socket.is_open()) {
        boost::asio::write(socket, boost::asio::buffer(request));
    }
}

// Main function to connect to the server and retrieve data
int main() {
    try {
        boost::asio::io_service io_service;

        // Connect to the ABX Exchange Server
        tcp::resolver resolver(io_service);
        tcp::resolver::query query("127.0.0.1", "3000");
        tcp::resolver::iterator endpoint_iterator = resolver.resolve(query);

        tcp::socket socket(io_service);
        boost::asio::connect(socket, endpoint_iterator);

        // Request to stream all packets
        std::vector<uint8_t> request(2);
        request[0] = 1; // CallType 1 for streaming all packets
        
        // Ensure the socket is still open before writing
        if (socket.is_open()) {
            boost::asio::write(socket, boost::asio::buffer(request));
        }

        std::vector<StockPacket> packets;
        std::vector<int32_t> receivedSequences;

        // Receive data from the server
        while (socket.is_open()) {
            std::vector<uint8_t> response(17); // Each packet is 17 bytes
            boost::system::error_code error;
            size_t len = socket.read_some(boost::asio::buffer(response), error);

            if (error == boost::asio::error::eof) {
                break; // Connection closed cleanly
            }
            if (error) {
                throw boost::system::system_error(error); // Other errors
            }

            StockPacket packet = parsePacket(response);
            packets.push_back(packet);
            receivedSequences.push_back(packet.packetSequence);
        }

        // Find missing sequences
        std::vector<int32_t> missingSequences;
        for (int32_t seq = 1; seq <= packets.back().packetSequence; ++seq) {
            if (std::find(receivedSequences.begin(), receivedSequences.end(), seq) == receivedSequences.end()) {
                missingSequences.push_back(seq);
            }
        }

        // Request and receive missing sequences
        for (int32_t seq : missingSequences) {
            requestMissingPacket(socket, seq);

            std::vector<uint8_t> response(17); // Each packet is 17 bytes
            boost::system::error_code error;
            size_t len = socket.read_some(boost::asio::buffer(response), error);

            if (!error) {
                StockPacket packet = parsePacket(response);
                packets.push_back(packet);
            }
        }

        // Close the socket
        if (socket.is_open()) {
            socket.close();
        }

        // Create JSON output
        json output = json::array();
        for (const auto& packet : packets) {
            json entry = {
                {"symbol", packet.symbol},
                {"buysellindicator", std::string(1, packet.buysellindicator)},
                {"quantity", packet.quantity},
                {"price", packet.price},
                {"packetSequence", packet.packetSequence}
            };
            output.push_back(entry);
        }

        // Save to JSON file
        std::ofstream file("output.json");
        file << output.dump(4); // Pretty print with 4 spaces indentation
        file.close();

        std::cout << "Data successfully written to output.json" << std::endl;
    } catch (std::exception& e) {
        std::cerr << "Exception: " << e.what() << "\n";
    }

    return 0;
}
