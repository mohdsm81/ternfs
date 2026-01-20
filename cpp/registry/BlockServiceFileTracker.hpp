// Copyright 2025 XTX Markets Technologies Limited
//
// SPDX-License-Identifier: GPL-2.0-or-later

#pragma once

#include "Loop.hpp"
#include "Msgs.hpp"
#include "MsgsGen.hpp"
#include "RegistryDB.hpp"
#include "RegistryServer.hpp"
#include "RegistryWriter.hpp"
#include "SPSC.hpp"

struct BlockServiceStatus {
    bool hadFiles;
    ShardId endShardId;
};

struct ShardRequestStatus {
    ShardId shardId;
    TernTime lastSent;
    BlockServiceId blockServiceId;
};

class BlockServiceFileTracker : public Loop {
public:
    static constexpr uint32_t MAX_BLOCK_SERVICE_SCAN = 128;
    static constexpr uint32_t MAX_SHARD_RESPONSES = 2*MAX_BLOCK_SERVICE_SCAN;
    static constexpr Duration SHARD_RETRY_TIMEOUT = 1_sec;

    BlockServiceFileTracker(Logger& logger, std::shared_ptr<XmonAgent>& xmon, const RegistryOptions& options, RegistryDB& db, RegistryServer& server, RegistryWriter& writer);

    virtual void step() override;

    void pushShardResponses(std::vector<ShardResponse>& responses);

    virtual void sendStop() override;

private:
    RegistryDB& _registryDB;
    RegistryServer& _server;
    RegistryWriter& _writer;

    SPSC<ShardResponse> _queue;
    std::vector<ShardResponse> _responses;

    std::unordered_map<uint64_t, ShardRequestStatus> _inflightRequests;
    TernTime _lastRequestBatchTime;

    std::unordered_map<BlockServiceId, BlockServiceStatus> _blockServiceState;
    std::vector<FullBlockServiceInfo> _pendingBlockServices;
    ShardId _currentShardIndex = 0;

    void _processResponses();
    void _scanBlockServices();
    void _refillBlockServicesCache();
    void _sendRequests();

    uint64_t _nextRequestId;
};
