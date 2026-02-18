// Copyright 2024 Halimjon Juraev
// Nexus Technologies, LLC
// Licensed under the Apache License, Version 2.0

import Foundation
import NIOCore

/// A handle to a Key-Value bucket backed by a JetStream stream
public actor KeyValueBucket {
    let context: JetStreamContext
    let bucket: String
    var stream: Stream

    init(context: JetStreamContext, bucket: String, stream: Stream) {
        self.context = context
        self.bucket = bucket
        self.stream = stream
    }

    // MARK: - Read Operations

    /// Get the latest value for a key. Returns nil if the key doesn't exist or is deleted/purged.
    public func get(_ key: String) async throws(JetStreamError) -> KeyValueEntry? {
        try validateKey(key)

        let streamName = "KV_\(bucket)"
        let getSubject = "$KV.\(bucket).\(key)"

        // Use Direct Get with subject shortcut
        let subject = "$JS.API.DIRECT.GET.\(streamName).\(getSubject)"

        do {
            let response = try await context.rawRequest(subject, payload: ByteBuffer())

            // Check for 404
            if let headers = response.headers, headers.isNoMessages {
                return nil
            }
            if let status = response.headers?.status, status == 404 {
                return nil
            }

            guard let entry = KeyValueEntry.fromDirectGet(response, bucket: bucket) else {
                return nil
            }

            // If the entry is a delete/purge marker, return nil
            if entry.operation == .delete || entry.operation == .purge {
                return nil
            }

            return entry
        } catch {
            // 404 from API error means key not found
            if case .apiError(let code, _, _) = error, code == 404 {
                return nil
            }
            throw error
        }
    }

    /// Get a specific revision of a key.
    public func get(_ key: String, revision: UInt64) async throws(JetStreamError) -> KeyValueEntry? {
        try validateKey(key)

        let streamName = "KV_\(bucket)"
        let subject = "$JS.API.DIRECT.GET.\(streamName)"

        let request = DirectGetRequest(seq: revision)
        let payload: ByteBuffer
        do {
            let data = try JSONEncoder().encode(request)
            var buffer = ByteBuffer()
            buffer.writeBytes(data)
            payload = buffer
        } catch {
            throw JetStreamError.kvOperationFailed(error.localizedDescription)
        }

        do {
            let response = try await context.rawRequest(subject, payload: payload)

            if let headers = response.headers, headers.isNoMessages {
                return nil
            }
            if let status = response.headers?.status, status == 404 {
                return nil
            }

            guard let entry = KeyValueEntry.fromDirectGet(response, bucket: bucket) else {
                return nil
            }

            // Verify the returned entry matches the requested key
            let expectedSubject = "$KV.\(bucket).\(key)"
            if let responseSubject = response.headers?[NatsHeaders.Keys.natsSubject],
               responseSubject != expectedSubject {
                return nil
            }

            return entry
        } catch let error as JetStreamError {
            if case .apiError(let code, _, _) = error, code == 404 {
                return nil
            }
            throw error
        }
    }

    // MARK: - Write Operations

    /// Put a value for a key, returning the revision number.
    @discardableResult
    public func put(_ key: String, value: ByteBuffer) async throws(JetStreamError) -> UInt64 {
        try validateKey(key)

        let subject = "$KV.\(bucket).\(key)"
        let ack = try await context.publish(subject, payload: value)
        return ack.seq
    }

    /// Put a string value for a key, returning the revision number.
    @discardableResult
    public func put(_ key: String, value: String) async throws(JetStreamError) -> UInt64 {
        var buffer = ByteBuffer()
        buffer.writeString(value)
        return try await put(key, value: buffer)
    }

    /// Create a key only if it does not already exist. Returns the revision.
    /// Throws `keyExists` if the key already has a value.
    @discardableResult
    public func create(_ key: String, value: ByteBuffer) async throws(JetStreamError) -> UInt64 {
        try validateKey(key)

        let subject = "$KV.\(bucket).\(key)"
        var headers = NatsHeaders()
        headers[NatsHeaders.Keys.expectedLastSubjectSequence] = "0"

        do {
            let ack = try await context.publishWithHeaders(subject, payload: value, headers: headers)
            return ack.seq
        } catch {
            // Error code 10071 = wrong last subject sequence
            if case .apiError(_, let errorCode, _) = error, errorCode == 10071 {
                // Check if existing entry is a delete/purge marker - if so, retry with that revision
                if let existing = try await getEntry(key) {
                    if existing.operation == .delete || existing.operation == .purge {
                        return try await update(key, value: value, revision: existing.revision)
                    }
                    throw JetStreamError.keyExists(key: key, revision: existing.revision)
                }
                throw JetStreamError.keyExists(key: key, revision: 0)
            }
            throw error
        }
    }

    /// Create a key with a string value only if it does not already exist.
    @discardableResult
    public func create(_ key: String, value: String) async throws(JetStreamError) -> UInt64 {
        var buffer = ByteBuffer()
        buffer.writeString(value)
        return try await create(key, value: buffer)
    }

    /// Update a key only if the current revision matches. Returns the new revision.
    @discardableResult
    public func update(_ key: String, value: ByteBuffer, revision: UInt64) async throws(JetStreamError) -> UInt64 {
        try validateKey(key)

        let subject = "$KV.\(bucket).\(key)"
        var headers = NatsHeaders()
        headers[NatsHeaders.Keys.expectedLastSubjectSequence] = String(revision)

        let ack = try await context.publishWithHeaders(subject, payload: value, headers: headers)
        return ack.seq
    }

    /// Update a key with a string value only if the current revision matches.
    @discardableResult
    public func update(_ key: String, value: String, revision: UInt64) async throws(JetStreamError) -> UInt64 {
        var buffer = ByteBuffer()
        buffer.writeString(value)
        return try await update(key, value: buffer, revision: revision)
    }

    // MARK: - Delete Operations

    /// Soft-delete a key by placing a delete marker.
    public func delete(_ key: String) async throws(JetStreamError) {
        try validateKey(key)

        let subject = "$KV.\(bucket).\(key)"
        var headers = NatsHeaders()
        headers[NatsHeaders.Keys.kvOperation] = KeyValueOperation.delete.rawValue

        _ = try await context.publishWithHeaders(subject, payload: ByteBuffer(), headers: headers)
    }

    /// Soft-delete a key with CAS (only if revision matches).
    public func delete(_ key: String, revision: UInt64) async throws(JetStreamError) {
        try validateKey(key)

        let subject = "$KV.\(bucket).\(key)"
        var headers = NatsHeaders()
        headers[NatsHeaders.Keys.kvOperation] = KeyValueOperation.delete.rawValue
        headers[NatsHeaders.Keys.expectedLastSubjectSequence] = String(revision)

        _ = try await context.publishWithHeaders(subject, payload: ByteBuffer(), headers: headers)
    }

    /// Purge a key, removing all revisions.
    public func purge(_ key: String) async throws(JetStreamError) {
        try validateKey(key)

        let subject = "$KV.\(bucket).\(key)"
        var headers = NatsHeaders()
        headers[NatsHeaders.Keys.kvOperation] = KeyValueOperation.purge.rawValue
        headers[NatsHeaders.Keys.rollup] = "sub"

        _ = try await context.publishWithHeaders(subject, payload: ByteBuffer(), headers: headers)
    }

    /// Purge a key with CAS (only if revision matches), removing all revisions.
    public func purge(_ key: String, revision: UInt64) async throws(JetStreamError) {
        try validateKey(key)

        let subject = "$KV.\(bucket).\(key)"
        var headers = NatsHeaders()
        headers[NatsHeaders.Keys.kvOperation] = KeyValueOperation.purge.rawValue
        headers[NatsHeaders.Keys.rollup] = "sub"
        headers[NatsHeaders.Keys.expectedLastSubjectSequence] = String(revision)

        _ = try await context.publishWithHeaders(subject, payload: ByteBuffer(), headers: headers)
    }

    // MARK: - Enumeration

    /// List all active keys in the bucket, optionally filtered by a subject pattern.
    public func keys(filter: String? = nil) async throws(JetStreamError) -> [String] {
        let streamName = "KV_\(bucket)"
        let filterSubject = filter.map { "$KV.\(bucket).\($0)" } ?? "$KV.\(bucket).>"

        let consumer = try await context.createConsumer(
            stream: streamName,
            config: ConsumerConfig(
                deliverPolicy: .lastPerSubject,
                ackPolicy: .none,
                filterSubject: filterSubject,
                inactiveThreshold: .seconds(300),
                memStorage: true,
                headersOnly: true
            )
        )

        var keys: [String] = []
        let consumerName = await consumer.name
        let keyPrefix = "$KV.\(bucket)."

        defer {
            Task {
                try? await context.deleteConsumer(stream: streamName, consumer: consumerName)
            }
        }

        // Fetch all pending messages
        let info = await consumer.info
        var remaining = Int(info.numPending)
        if remaining == 0 {
            return keys
        }

        while remaining > 0 {
            let batchSize = min(remaining, 256)
            let messages = try await consumer.fetch(batch: batchSize, maxWait: .seconds(5))

            if messages.isEmpty {
                break
            }

            for msg in messages {
                let subject = msg.subject
                guard subject.hasPrefix(keyPrefix) else { continue }
                let key = String(subject.dropFirst(keyPrefix.count))

                // Skip delete/purge markers
                if let opStr = msg.headers?[NatsHeaders.Keys.kvOperation] {
                    if opStr == "DEL" || opStr == "PURGE" {
                        continue
                    }
                }

                keys.append(key)
            }

            remaining -= messages.count
        }

        return keys
    }

    /// Get all history entries for a key.
    public func history(_ key: String) async throws(JetStreamError) -> [KeyValueEntry] {
        try validateKey(key)

        let streamName = "KV_\(bucket)"
        let filterSubject = "$KV.\(bucket).\(key)"

        let consumer = try await context.createConsumer(
            stream: streamName,
            config: ConsumerConfig(
                deliverPolicy: .all,
                ackPolicy: .none,
                filterSubject: filterSubject,
                inactiveThreshold: .seconds(300),
                memStorage: true
            )
        )

        var entries: [KeyValueEntry] = []
        let consumerName = await consumer.name

        defer {
            Task {
                try? await context.deleteConsumer(stream: streamName, consumer: consumerName)
            }
        }

        let info = await consumer.info
        var remaining = Int(info.numPending)
        if remaining == 0 {
            return entries
        }

        while remaining > 0 {
            let batchSize = min(remaining, 256)
            let messages = try await consumer.fetch(batch: batchSize, maxWait: .seconds(5))

            if messages.isEmpty {
                break
            }

            for msg in messages {
                if let entry = KeyValueEntry.fromConsumerMessage(msg, bucket: bucket) {
                    entries.append(entry)
                }
            }

            remaining -= messages.count
        }

        return entries
    }

    // MARK: - Watch

    /// Watch for changes on keys matching the given pattern.
    /// Pass ">" to watch all keys.
    public func watch(
        _ keyPattern: String = ">",
        options: KeyValueWatchOptions = KeyValueWatchOptions()
    ) async throws(JetStreamError) -> KeyValueWatcher {
        try validateWatchKey(keyPattern)
        return try await KeyValueWatcher.create(
            context: context,
            bucket: bucket,
            keyPattern: keyPattern,
            options: options
        )
    }

    /// Watch all keys for changes.
    public func watchAll(
        options: KeyValueWatchOptions = KeyValueWatchOptions()
    ) async throws(JetStreamError) -> KeyValueWatcher {
        try await watch(">", options: options)
    }

    // MARK: - Maintenance

    /// Remove old delete/purge markers. Markers older than the given duration are fully purged;
    /// recent markers are kept (1 entry) to preserve tombstone semantics.
    public func purgeDeletes(olderThan: Duration? = nil) async throws(JetStreamError) {
        let streamName = "KV_\(bucket)"

        // Create a consumer to find all delete/purge markers
        let consumer = try await context.createConsumer(
            stream: streamName,
            config: ConsumerConfig(
                deliverPolicy: .lastPerSubject,
                ackPolicy: .none,
                filterSubject: "$KV.\(bucket).>",
                inactiveThreshold: .seconds(300),
                memStorage: true,
                headersOnly: true
            )
        )

        let consumerName = await consumer.name

        defer {
            Task {
                try? await context.deleteConsumer(stream: streamName, consumer: consumerName)
            }
        }

        let info = await consumer.info
        var remaining = Int(info.numPending)

        var deleteSubjects: [(subject: String, isOld: Bool)] = []
        let now = Date()

        while remaining > 0 {
            let batchSize = min(remaining, 256)
            let messages = try await consumer.fetch(batch: batchSize, maxWait: .seconds(5))

            if messages.isEmpty {
                break
            }

            for msg in messages {
                if let opStr = msg.headers?[NatsHeaders.Keys.kvOperation],
                   (opStr == "DEL" || opStr == "PURGE") {

                    let isOld: Bool
                    if let threshold = olderThan {
                        let age = now.timeIntervalSince(msg.metadata.timestamp)
                        isOld = age > threshold.timeInterval
                    } else {
                        isOld = true
                    }

                    deleteSubjects.append((subject: msg.subject, isOld: isOld))
                }
            }

            remaining -= messages.count
        }

        // Purge each delete marker subject
        for item in deleteSubjects {
            if item.isOld {
                // Fully purge (remove all including the marker)
                _ = try await stream.purge(subject: item.subject)
            } else {
                // Keep the latest marker but remove history
                _ = try await stream.purge(subject: item.subject, keep: 1)
            }
        }
    }

    /// Get the current status of this bucket.
    public func status() async throws(JetStreamError) -> KeyValueBucketStatus {
        try await stream.refresh()
        let info = await stream.info
        return KeyValueBucketStatus.from(info)
    }

    // MARK: - Internal Helpers

    /// Get entry including delete/purge markers (for internal use by create)
    private func getEntry(_ key: String) async throws(JetStreamError) -> KeyValueEntry? {
        try validateKey(key)

        let streamName = "KV_\(bucket)"
        let getSubject = "$KV.\(bucket).\(key)"
        let subject = "$JS.API.DIRECT.GET.\(streamName).\(getSubject)"

        do {
            let response = try await context.rawRequest(subject, payload: ByteBuffer())

            if let headers = response.headers, headers.isNoMessages {
                return nil
            }
            if let status = response.headers?.status, status == 404 {
                return nil
            }

            return KeyValueEntry.fromDirectGet(response, bucket: bucket)
        } catch let error as JetStreamError {
            if case .apiError(let code, _, _) = error, code == 404 {
                return nil
            }
            throw error
        }
    }
}

// MARK: - Duration extension

extension Duration {
    var timeInterval: TimeInterval {
        let (seconds, attoseconds) = self.components
        return TimeInterval(seconds) + TimeInterval(attoseconds) / 1e18
    }
}
