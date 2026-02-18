// Copyright 2024 Halimjon Juraev
// Nexus Technologies, LLC
// Licensed under the Apache License, Version 2.0

import Foundation

/// Configuration for creating a Key-Value bucket
public struct KeyValueConfig: Sendable {
    /// Bucket name (must match [a-zA-Z0-9_-]+)
    public var bucket: String

    /// Description of the bucket
    public var description: String?

    /// Number of history entries per key (1-64, default 1)
    public var history: Int

    /// Time-to-live for entries
    public var ttl: Duration?

    /// Maximum size of a value in bytes (-1 for unlimited)
    public var maxValueSize: Int32

    /// Maximum total bytes for the bucket (-1 for unlimited)
    public var maxBytes: Int64

    /// Storage backend type
    public var storage: StorageType

    /// Number of replicas
    public var replicas: Int

    /// Placement preferences
    public var placement: Placement?

    /// Re-publish configuration
    public var republish: Republish?

    /// Mirror configuration (for mirrored buckets)
    public var mirror: StreamSource?

    /// Source configurations (for sourced buckets)
    public var sources: [StreamSource]?

    /// Compression algorithm
    public var compression: StoreCompression?

    /// User-provided metadata
    public var metadata: [String: String]?

    public init(
        bucket: String,
        description: String? = nil,
        history: Int = 1,
        ttl: Duration? = nil,
        maxValueSize: Int32 = -1,
        maxBytes: Int64 = -1,
        storage: StorageType = .file,
        replicas: Int = 1,
        placement: Placement? = nil,
        republish: Republish? = nil,
        mirror: StreamSource? = nil,
        sources: [StreamSource]? = nil,
        compression: StoreCompression? = nil,
        metadata: [String: String]? = nil
    ) {
        self.bucket = bucket
        self.description = description
        self.history = history
        self.ttl = ttl
        self.maxValueSize = maxValueSize
        self.maxBytes = maxBytes
        self.storage = storage
        self.replicas = replicas
        self.placement = placement
        self.republish = republish
        self.mirror = mirror
        self.sources = sources
        self.compression = compression
        self.metadata = metadata
    }

    /// Convert to the underlying StreamConfig
    func toStreamConfig() -> StreamConfig {
        let streamName = "KV_\(bucket)"

        // Calculate maxAge in nanoseconds
        let maxAgeNanos: Int64
        if let ttl = ttl {
            maxAgeNanos = Int64(ttl.components.seconds) * 1_000_000_000
                + Int64(ttl.components.attoseconds / 1_000_000_000)
        } else {
            maxAgeNanos = 0
        }

        // Calculate duplicate window
        let twoMinutesNanos: Int64 = 120 * 1_000_000_000
        let duplicateWindowNanos: Int64
        if maxAgeNanos > 0 {
            duplicateWindowNanos = min(maxAgeNanos, twoMinutesNanos)
        } else {
            duplicateWindowNanos = twoMinutesNanos
        }

        var subjects = ["$KV.\(bucket).>"]

        // Mirror and source buckets don't set subjects
        if mirror != nil {
            subjects = []
        }
        if let srcs = sources, !srcs.isEmpty {
            subjects = []
        }

        var config = StreamConfig(
            name: streamName,
            subjects: subjects,
            retention: .limits,
            maxConsumers: -1,
            maxMsgs: -1,
            maxBytes: maxBytes,
            maxMsgSize: maxValueSize,
            storage: storage,
            replicas: replicas,
            discard: .new,
            maxMsgsPerSubject: Int64(history),
            allowRollupHdrs: true,
            denyDelete: true,
            denyPurge: false,
            allowDirect: true,
            mirror: mirror,
            sources: sources,
            description: description,
            placement: placement,
            republish: republish,
            compression: compression,
            metadata: metadata
        )

        // Set raw nanos values directly (bypassing Duration init)
        config.maxAge = maxAgeNanos
        config.duplicateWindow = duplicateWindowNanos

        return config
    }
}

// MARK: - Validation

/// Validate a bucket name (alphanumeric, dash, underscore)
func validateBucketName(_ name: String) throws(JetStreamError) {
    guard !name.isEmpty else {
        throw .invalidBucketName("bucket name cannot be empty")
    }

    let pattern = "^[a-zA-Z0-9_-]+$"
    guard name.range(of: pattern, options: .regularExpression) != nil else {
        throw .invalidBucketName(name)
    }
}

/// Validate a key for put/get/delete operations
func validateKey(_ key: String) throws(JetStreamError) {
    guard !key.isEmpty else {
        throw .invalidKey("key cannot be empty")
    }

    let pattern = "^[-/_=\\.a-zA-Z0-9]+$"
    guard key.range(of: pattern, options: .regularExpression) != nil else {
        throw .invalidKey(key)
    }

    guard !key.hasPrefix(".") && !key.hasSuffix(".") else {
        throw .invalidKey(key)
    }
}

/// Validate a key pattern for watch operations (allows wildcards)
func validateWatchKey(_ key: String) throws(JetStreamError) {
    guard !key.isEmpty else {
        throw .invalidKey("key cannot be empty")
    }

    // Allow * and > wildcards in addition to normal key chars
    let pattern = "^[-/_=\\.a-zA-Z0-9*>]+$"
    guard key.range(of: pattern, options: .regularExpression) != nil else {
        throw .invalidKey(key)
    }
}
