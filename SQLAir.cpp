/*
 * A very lightweight (light as air) implementation of a simple CSV-based
 * database system that uses SQL-like syntax for querying and updating the
 * CSV files.
 *
 * Copyright 2023 yurj@miamioh.edu
 */

#include "SQLAir.h"

#include <algorithm>
#include <fstream>
#include <memory>
#include <string>
#include <tuple>

#include "HTTPFile.h"

/**
 * A fixed HTTP response header that is used by the runServer method below.
 * Note that this a constant (and not a global variable)
 */
const std::string HTTPRespHeader =
    "HTTP/1.1 200 OK\r\n"
    "Server: localhost\r\n"
    "Connection: Close\r\n"
    "Content-Type: text/plain\r\n"
    "Content-Length: ";

/**
 * A helper method called by the selectQuery function to print the values in
 * specified columns (by colNames) for a row in a CSV.
 *
 * @param row The row of the CSV whose values will be printed.
 *
 * @param colNames A vector of strings containing column names in the CSV from
 * where we will print values.
 *
 * @param csv The CSV object that contains the row, and is used to get the
 * correct indices for values in the row.
 *
 * @param os The output stream used to print the values from the row.
 */
void display(const CSVRow& row, const StrVec& colNames, const CSV& csv,
             std::ostream& os, int numRows) {
    if (numRows == 1) {  // Print the column names if we select a row
        os << colNames << std::endl;
    }
    std::string delim = "";
    for (const auto& colName : colNames) {
        os << delim << row.at(csv.getColumnIndex(colName));
        delim = "\t";
    }
    os << std::endl;
}

// API method to perform operations associated with a "select" statement
// to print columns that match an optional condition.
void SQLAir::selectQuery(CSV& csv, bool mustWait, StrVec colNames,
                         const int whereColIdx, const std::string& cond,
                         const std::string& value, std::ostream& os) {
    // Convert any "*" to suitable column names
    colNames = (colNames[0] == "*") ? csv.getColumnNames() : colNames;
    int numRows = 0;
    CSVRow selRow;
    // Print each row that matches an optional condition.
    for (auto& row : csv) {
        bool rowChosen = false;
        {
            std::scoped_lock<std::mutex> lock(row.rowMutex);  // begin CS
            rowChosen = (whereColIdx == -1 ||
                         matches(row.at(whereColIdx), cond, value));
            selRow = row;
        }                 // end CS
        if (rowChosen) {  // make sure I/O is outside of CS
            display(selRow, colNames, csv, os, ++numRows);
        }
    }
    if (mustWait && numRows == 0) {  // we have to wait and no rows selected
        std::unique_lock<std::mutex> lock(csv.csvMutex);
        csv.csvCondVar.wait(lock);
        // rerun the selectQuery method with the same arguments after waiting
        selectQuery(csv, mustWait, colNames, whereColIdx, cond, value, os);
    } else {
        os << numRows << " row(s) selected." << std::endl;
    }
}

void SQLAir::updateQuery(CSV& csv, bool mustWait, StrVec colNames,
                         StrVec values, const int whereColIdx,
                         const std::string& cond, const std::string& value,
                         std::ostream& os) {
    int numRows = 0;
    // Update each row that matches an optional condition.
    for (auto& row : csv) {
        std::scoped_lock<std::mutex> lock(row.rowMutex);  // begin CS
        // Determine if this row matches "where" clause condition, if any
        // see SQLAirBase::matches() helper method.
        if (whereColIdx == -1 || matches(row.at(whereColIdx), cond, value)) {
            numRows++;
            for (size_t i = 0; i < colNames.size(); i++) {
                row[csv.getColumnIndex(colNames[i])] = values[i];
            }
        }
    }  // end CS

    if (mustWait && numRows == 0) {  // we have to wait and no rows updated
        std::unique_lock<std::mutex> lock(csv.csvMutex);
        csv.csvCondVar.wait(lock);
        // after waiting run updateQuery method again with same arguments
        updateQuery(csv, mustWait, colNames, values, whereColIdx, cond, value,
                    os);
    } else {
        os << numRows << " row(s) updated." << std::endl;
        if (numRows > 0) {  // notify threads if a row was updated
            csv.csvCondVar.notify_all();
        }
    }
}

void SQLAir::insertQuery(CSV& csv, bool mustWait, StrVec colNames,
                         StrVec values, std::ostream& os) {
    throw Exp("insert is not yet implemented.");
}

void SQLAir::deleteQuery(CSV& csv, bool mustWait, const int whereColIdx,
                         const std::string& cond, const std::string& value,
                         std::ostream& os) {
    throw Exp("delete is not yet implemented.");
}

// The method to process GET requests when the program is run as a web server.
void SQLAir::clientThread(TcpStreamPtr client) {
    // Read the HTTP request from the client, process it, and send an
    // HTTP response back to the client.
    std::string request, response, hdr;
    *client >> request >> request;

    while (getline(*client, hdr) && !hdr.empty() && hdr != "\r") {
    }

    if (request.find("/sql-air?query=") == 0) {  // run sql-air query
        request = request.substr(15);            // remove "/sql-air?query="
        request = Helper::url_decode(request);
        std::ostringstream os;
        try {  // process the requested query
            process(request, os);
        } catch (const std::exception& exp) {
            os << "Error: " << exp.what() << std::endl;
        }
        *client << HTTPRespHeader << os.str().size() << "\r\n\r\n" << os.str();
    } else if (!request.empty()) {    // request from a file
        request = request.substr(1);  // remove initial / from request string
        *client << http::file(request);
    }
    numThreads--;          // decrement the number of threads
    thrCond.notify_one();  // notify a thread that one has finished running
}

// The method to have this class run as a web-server.
void SQLAir::runServer(boost::asio::ip::tcp::acceptor& server,
                       const int maxThr) {
    // Process client connections one-by-one...forever
    while (true) {
        // Check the number of background threads running
        std::unique_lock<std::mutex> lock(thrMutex);
        thrCond.wait(lock, [this, maxThr]() { return numThreads < maxThr; });

        // Creates garbage-collected connection on heap
        TcpStreamPtr client =
            std::make_shared<boost::asio::ip::tcp::iostream>();
        server.accept(*client->rdbuf());  // wait for client to connect
        // Now we have a I/O stream to talk to the client. Have a
        // conversation using the protocol.
        std::thread thr([this, client] { SQLAir::clientThread(client); });
        numThreads++;  // increment the number of threads
        thr.detach();  // Process transaction independently
    }
}

/**
 * Helper method to setup a TCP stream for downloading data from an
 * web-server. The basis for this method was taken from the LoginSentry.cpp file
 * from HW3.
 *
 * @param host The host name of the web-server. Host names can be of
 * the form "www.miamioh.edu" or "ceclnx01.cec.miamioh.edu".  This
 * information is typically extracted from a given URL.
 *
 * @param path The path to the file being download.  An example of
 * this value is "/~raodm/ssh_logs/full_logs.txt". This information is typically
 * extracted from a given URL.
 *
 * @param socket The TCP stream (aka socket) to be setup by this
 * method.  After a successful call to this method, this stream will
 * contain the response from the web-server to be processed.
 *
 * @param port An optional port number. The default port number is "80".
 *
 */
void setupDownload(const std::string& hostName, const std::string& path,
                   boost::asio::ip::tcp::iostream& data,
                   const std::string& port = "80") {
    // Create a boost socket and request the log file from the server.
    data.connect(hostName, port);

    // check to make sure connection is good
    if (!data.good()) {
        throw Exp("Unable to connect to " + hostName + " at port " + port);
    }

    // send HTTP request to server
    data << "GET " << path << " HTTP/1.1\r\n"
         << "Host: " << hostName << "\r\n"
         << "Connection: Close\r\n\r\n";

    // read the first line and make sure it contains "200 OK"
    std::string status;
    getline(data, status);
    if (status.find("200 OK") == std::string::npos) {
        status = Helper::trim(status);
        throw Exp("Error (" + status + ") getting " + path + " from " +
                  hostName + " at port " + port);
    }

    // read the rest of the header lines
    for (std::string hdr; getline(data, hdr) && !hdr.empty() && hdr != "\r";) {
    }
}

CSV& SQLAir::loadAndGet(std::string fileOrURL) {
    // Check if the specified fileOrURL is already loaded in a thread-safe
    // manner to avoid race conditions on the unordered_map
    {
        std::scoped_lock<std::mutex> guard(recentCSVMutex);
        // Use recent CSV if parameter was empty string.
        fileOrURL = (fileOrURL.empty() ? recentCSV : fileOrURL);
        // Update the most recently used CSV for the next round
        recentCSV = fileOrURL;
        if (inMemoryCSV.find(fileOrURL) != inMemoryCSV.end()) {
            // Requested CSV is already in memory. Just return it.
            return inMemoryCSV.at(fileOrURL);
        }
    }
    // When control drops here, we need to load the CSV into memory.
    // Loading or I/O is being done outside critical sections
    CSV csv;  // Load data into this csv
    if (fileOrURL.find("http://") == 0) {
        // This is an URL. We have to get the stream from a web-server
        // Implement this feature.
        std::string host, port, path;
        std::tie(host, port, path) = Helper::breakDownURL(fileOrURL);
        boost::asio::ip::tcp::iostream data;
        setupDownload(host, path, data, port);
        csv.load(data);
    } else {
        // We assume it is a local file on the server. Load that file.
        std::ifstream data(fileOrURL);
        // This method may throw exceptions on errors.
        csv.load(data);
    }

    // We get to this line of code only if the above if-else to load the
    // CSV did not throw any exceptions. In this case we have a valid CSV
    // to add to our inMemoryCSV list. We need to do that in a thread-safe
    // manner.
    std::scoped_lock<std::mutex> guard(recentCSVMutex);
    // Move (instead of copy) the CSV data into our in-memory CSVs
    inMemoryCSV[fileOrURL].move(csv);
    // Return a reference to the in-memory CSV (not temporary one)
    return inMemoryCSV.at(fileOrURL);
}

// Save the currently loaded CSV file to a local file.
void SQLAir::saveQuery(std::ostream& os) {
    if (recentCSV.empty() || recentCSV.find("http://") == 0) {
        throw Exp("Saving CSV to an URL using POST is not implemented");
    }
    // Create a local file and have the CSV write itself.
    std::ofstream csvData(recentCSV);
    inMemoryCSV.at(recentCSV).save(csvData);
    os << recentCSV << " saved.\n";
}
