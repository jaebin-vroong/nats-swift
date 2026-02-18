// Copyright 2024 Halimjon Juraev
// Nexus Technologies, LLC
// Licensed under the Apache License, Version 2.0

import Foundation
import NIOCore

/// Options for watching key changes
public struct KeyValueWatchOptions: Sendable {
    /// Include all historical values (deliverPolicy: .all)
    public var includeHistory: Bool

    /// Only receive updates after the watcher is created (deliverPolicy: .new)
    public var updatesOnly: Bool

    /// Only receive headers (no values) — useful for key enumeration
    public var metaOnly: Bool

    /// Resume watching from a specific revision (deliverPolicy: .byStartSequence)
    public var resumeFromRevision: UInt64?

    public init(
        includeHistory: Bool = false,
        updatesOnly: Bool = false,
        metaOnly: Bool = false,
        resumeFromRevision: UInt64? = nil
    ) {
        self.includeHistory = includeHistory
        self.updatesOnly = updatesOnly
        self.metaOnly = metaOnly
        self.resumeFromRevision = resumeFromRevision
    }
}

/// An async sequence that yields KV entry updates.
/// A nil element signals that all initial values have been delivered.
public struct KeyValueWatcher: AsyncSequence, Sendable {
    public typealias Element = KeyValueEntry?

    private let stream: AsyncThrowingStream<KeyValueEntry?, Error>
    private let consumerName: String
    private let streamName: String
    private let context: JetStreamContext

    init(
        stream: AsyncThrowingStream<KeyValueEntry?, Error>,
        consumerName: String,
        streamName: String,
        context: JetStreamContext
    ) {
        self.stream = stream
        self.consumerName = consumerName
        self.streamName = streamName
        self.context = context
    }

    public struct AsyncIterator: AsyncIteratorProtocol {
        var base: AsyncThrowingStream<KeyValueEntry?, Error>.AsyncIterator

        public mutating func next() async throws -> KeyValueEntry?? {
            try await base.next()
        }
    }

    public func makeAsyncIterator() -> AsyncIterator {
        AsyncIterator(base: stream.makeAsyncIterator())
    }

    /// Stop the watcher and clean up the consumer.
    public func stop() async throws(JetStreamError) {
        do {
            try await context.deleteConsumer(stream: streamName, consumer: consumerName)
        } catch {
            // Consumer may already be deleted (ephemeral timeout)
        }
    }

    /// Create a new watcher
    static func create(
        context: JetStreamContext,
        bucket: String,
        keyPattern: String,
        options: KeyValueWatchOptions
    ) async throws(JetStreamError) -> KeyValueWatcher {
        let streamName = "KV_\(bucket)"
        let filterSubject = "$KV.\(bucket).\(keyPattern)"

        // Determine deliver policy
        let deliverPolicy: DeliverPolicy
        var optStartSeq: UInt64? = nil

        if options.updatesOnly {
            deliverPolicy = .new
        } else if let rev = options.resumeFromRevision {
            deliverPolicy = .byStartSequence
            optStartSeq = rev
        } else if options.includeHistory {
            deliverPolicy = .all
        } else {
            deliverPolicy = .lastPerSubject
        }

        let consumer = try await context.createConsumer(
            stream: streamName,
            config: ConsumerConfig(
                deliverPolicy: deliverPolicy,
                optStartSeq: optStartSeq,
                ackPolicy: .none,
                filterSubject: filterSubject,
                inactiveThreshold: .seconds(300),
                memStorage: true,
                headersOnly: options.metaOnly ? true : nil
            )
        )

        let consumerName = await consumer.name
        let initialPending = await consumer.info.numPending

        let asyncStream = AsyncThrowingStream<KeyValueEntry?, Error> { continuation in
            Task {
                var received: UInt64 = 0
                var sentInitialDone = options.updatesOnly  // If updatesOnly, skip initial done sentinel

                while !Task.isCancelled {
                    do {
                        let messages = try await consumer.fetch(batch: 256, maxWait: .seconds(5))

                        for msg in messages {
                            let entry = KeyValueEntry.fromConsumerMessage(msg, bucket: bucket)
                            continuation.yield(entry)
                            received += 1
                        }

                        // After receiving all initial pending messages, send nil sentinel
                        if !sentInitialDone && received >= initialPending {
                            continuation.yield(nil)
                            sentInitialDone = true
                        }

                        if messages.isEmpty && !sentInitialDone {
                            // No messages and no initial pending — send sentinel
                            continuation.yield(nil)
                            sentInitialDone = true
                        }
                    } catch {
                        continuation.finish(throwing: error)
                        return
                    }
                }

                continuation.finish()
            }
        }

        return KeyValueWatcher(
            stream: asyncStream,
            consumerName: consumerName,
            streamName: streamName,
            context: context
        )
    }
}
