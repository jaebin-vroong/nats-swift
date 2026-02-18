// Copyright 2024 Halimjon Juraev
// Nexus Technologies, LLC
// Licensed under the Apache License, Version 2.0

import Foundation
import NIOCore

extension JetStreamContext {

    /// Create a new Key-Value bucket.
    public func createKeyValue(_ config: KeyValueConfig) async throws(JetStreamError) -> KeyValueBucket {
        try validateBucketName(config.bucket)

        if config.history < 1 || config.history > 64 {
            throw .historyExceeded(max: 64)
        }

        let streamConfig = config.toStreamConfig()
        let stream = try await createStream(streamConfig)

        return KeyValueBucket(context: self, bucket: config.bucket, stream: stream)
    }

    /// Bind to an existing Key-Value bucket by name.
    public func keyValue(_ bucket: String) async throws(JetStreamError) -> KeyValueBucket {
        try validateBucketName(bucket)

        let streamName = "KV_\(bucket)"

        do {
            let stream = try await self.stream(streamName)

            // Validate this is actually a KV bucket by checking subjects
            let info = await stream.info
            let expectedSubject = "$KV.\(bucket).>"
            let subjects = info.config.subjects

            // KV bucket should have the expected subject, unless it's a mirror/source
            let isMirrorOrSource = info.config.mirror != nil
                || (info.config.sources != nil && !info.config.sources!.isEmpty)

            if !isMirrorOrSource && !subjects.contains(expectedSubject) {
                throw JetStreamError.bucketNotFound(bucket)
            }

            return KeyValueBucket(context: self, bucket: bucket, stream: stream)
        } catch {
            if case JetStreamError.streamNotFound = error {
                throw .bucketNotFound(bucket)
            }
            if case JetStreamError.apiError(let code, _, _) = error, code == 404 {
                throw .bucketNotFound(bucket)
            }
            if let jsError = error as? JetStreamError {
                throw jsError
            }
            throw .bucketNotFound(bucket)
        }
    }

    /// Delete a Key-Value bucket.
    public func deleteKeyValue(_ bucket: String) async throws(JetStreamError) {
        try validateBucketName(bucket)

        let streamName = "KV_\(bucket)"
        try await deleteStream(streamName)
    }

    /// List all Key-Value bucket names.
    public func keyValueBucketNames() async throws -> [String] {
        var names: [String] = []

        for try await stream in streams() {
            let streamName = await stream.name
            if streamName.hasPrefix("KV_") {
                names.append(String(streamName.dropFirst(3)))
            }
        }

        return names
    }

    /// List all Key-Value buckets with status information.
    public func keyValueBuckets() async throws -> [KeyValueBucketStatus] {
        var statuses: [KeyValueBucketStatus] = []

        for try await stream in streams() {
            let streamName = await stream.name
            if streamName.hasPrefix("KV_") {
                let info = await stream.info
                statuses.append(KeyValueBucketStatus.from(info))
            }
        }

        return statuses
    }
}
