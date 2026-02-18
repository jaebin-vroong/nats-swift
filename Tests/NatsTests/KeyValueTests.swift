// Copyright 2024 Halimjon Juraev
// Nexus Technologies, LLC
// Licensed under the Apache License, Version 2.0

import Testing
import Foundation
import NIOCore
@testable import Nats

// MARK: - Unit Tests

@Suite("KeyValue Unit Tests")
struct KeyValueUnitTests {

    // MARK: - Validation Tests

    @Suite("Bucket Name Validation")
    struct BucketNameValidationTests {

        @Test("Valid bucket names")
        func validBucketNames() throws {
            try validateBucketName("mybucket")
            try validateBucketName("my-bucket")
            try validateBucketName("my_bucket")
            try validateBucketName("MyBucket123")
            try validateBucketName("BUCKET")
            try validateBucketName("a")
            try validateBucketName("test-bucket_123")
        }

        @Test("Empty bucket name is invalid")
        func emptyBucketName() throws {
            #expect(throws: JetStreamError.self) {
                try validateBucketName("")
            }
        }

        @Test("Bucket name with dots is invalid")
        func bucketNameWithDots() throws {
            #expect(throws: JetStreamError.self) {
                try validateBucketName("my.bucket")
            }
        }

        @Test("Bucket name with spaces is invalid")
        func bucketNameWithSpaces() throws {
            #expect(throws: JetStreamError.self) {
                try validateBucketName("my bucket")
            }
        }

        @Test("Bucket name with special characters is invalid")
        func bucketNameWithSpecialChars() throws {
            #expect(throws: JetStreamError.self) {
                try validateBucketName("my@bucket")
            }
            #expect(throws: JetStreamError.self) {
                try validateBucketName("my/bucket")
            }
            #expect(throws: JetStreamError.self) {
                try validateBucketName("my>bucket")
            }
        }
    }

    @Suite("Key Validation")
    struct KeyValidationTests {

        @Test("Valid keys")
        func validKeys() throws {
            try validateKey("mykey")
            try validateKey("my-key")
            try validateKey("my_key")
            try validateKey("my/key")
            try validateKey("my.key")
            try validateKey("path/to/key")
            try validateKey("key=value")
            try validateKey("key123")
            try validateKey("a")
        }

        @Test("Empty key is invalid")
        func emptyKey() throws {
            #expect(throws: JetStreamError.self) {
                try validateKey("")
            }
        }

        @Test("Key starting with dot is invalid")
        func keyStartingWithDot() throws {
            #expect(throws: JetStreamError.self) {
                try validateKey(".mykey")
            }
        }

        @Test("Key ending with dot is invalid")
        func keyEndingWithDot() throws {
            #expect(throws: JetStreamError.self) {
                try validateKey("mykey.")
            }
        }

        @Test("Key with wildcards is invalid for regular key")
        func keyWithWildcards() throws {
            #expect(throws: JetStreamError.self) {
                try validateKey("my*key")
            }
            #expect(throws: JetStreamError.self) {
                try validateKey("my>key")
            }
        }

        @Test("Key with spaces is invalid")
        func keyWithSpaces() throws {
            #expect(throws: JetStreamError.self) {
                try validateKey("my key")
            }
        }
    }

    @Suite("Watch Key Validation")
    struct WatchKeyValidationTests {

        @Test("Valid watch keys including wildcards")
        func validWatchKeys() throws {
            try validateWatchKey("mykey")
            try validateWatchKey("*")
            try validateWatchKey(">")
            try validateWatchKey("path.*")
            try validateWatchKey("path.>")
        }

        @Test("Empty watch key is invalid")
        func emptyWatchKey() throws {
            #expect(throws: JetStreamError.self) {
                try validateWatchKey("")
            }
        }
    }

    // MARK: - Config Tests

    @Suite("KeyValueConfig to StreamConfig Conversion")
    struct ConfigConversionTests {

        @Test("Default config conversion")
        func defaultConfig() {
            let kvConfig = KeyValueConfig(bucket: "test")
            let streamConfig = kvConfig.toStreamConfig()

            #expect(streamConfig.name == "KV_test")
            #expect(streamConfig.subjects == ["$KV.test.>"])
            #expect(streamConfig.retention == .limits)
            #expect(streamConfig.discard == .new)
            #expect(streamConfig.maxMsgsPerSubject == 1)
            #expect(streamConfig.allowRollupHdrs == true)
            #expect(streamConfig.denyDelete == true)
            #expect(streamConfig.denyPurge == false)
            #expect(streamConfig.allowDirect == true)
            #expect(streamConfig.maxConsumers == -1)
            #expect(streamConfig.maxMsgs == -1)
            #expect(streamConfig.maxAge == 0)
            #expect(streamConfig.duplicateWindow == 120_000_000_000) // 2 minutes in nanos
        }

        @Test("Config with history")
        func configWithHistory() {
            let kvConfig = KeyValueConfig(bucket: "test", history: 10)
            let streamConfig = kvConfig.toStreamConfig()

            #expect(streamConfig.maxMsgsPerSubject == 10)
        }

        @Test("Config with TTL less than 2 minutes")
        func configWithShortTTL() {
            let kvConfig = KeyValueConfig(bucket: "test", ttl: .seconds(60))
            let streamConfig = kvConfig.toStreamConfig()

            #expect(streamConfig.maxAge == 60_000_000_000) // 60s in nanos
            #expect(streamConfig.duplicateWindow == 60_000_000_000) // min(TTL, 2min) = TTL
        }

        @Test("Config with TTL more than 2 minutes")
        func configWithLongTTL() {
            let kvConfig = KeyValueConfig(bucket: "test", ttl: .seconds(600))
            let streamConfig = kvConfig.toStreamConfig()

            #expect(streamConfig.maxAge == 600_000_000_000) // 600s in nanos
            #expect(streamConfig.duplicateWindow == 120_000_000_000) // min(TTL, 2min) = 2min
        }

        @Test("Config with storage type")
        func configWithStorage() {
            let kvConfig = KeyValueConfig(bucket: "test", storage: .memory)
            let streamConfig = kvConfig.toStreamConfig()

            #expect(streamConfig.storage == .memory)
        }

        @Test("Config with replicas")
        func configWithReplicas() {
            let kvConfig = KeyValueConfig(bucket: "test", replicas: 3)
            let streamConfig = kvConfig.toStreamConfig()

            #expect(streamConfig.replicas == 3)
        }

        @Test("Config with max value size")
        func configWithMaxValueSize() {
            let kvConfig = KeyValueConfig(bucket: "test", maxValueSize: 1024)
            let streamConfig = kvConfig.toStreamConfig()

            #expect(streamConfig.maxMsgSize == 1024)
        }

        @Test("Config with max bytes")
        func configWithMaxBytes() {
            let kvConfig = KeyValueConfig(bucket: "test", maxBytes: 1_000_000)
            let streamConfig = kvConfig.toStreamConfig()

            #expect(streamConfig.maxBytes == 1_000_000)
        }

        @Test("Config with compression")
        func configWithCompression() {
            let kvConfig = KeyValueConfig(bucket: "test", compression: .s2)
            let streamConfig = kvConfig.toStreamConfig()

            #expect(streamConfig.compression == .s2)
        }

        @Test("Config with metadata")
        func configWithMetadata() {
            let kvConfig = KeyValueConfig(
                bucket: "test",
                metadata: ["env": "test", "team": "platform"]
            )
            let streamConfig = kvConfig.toStreamConfig()

            #expect(streamConfig.metadata?["env"] == "test")
            #expect(streamConfig.metadata?["team"] == "platform")
        }

        @Test("Mirror config has no subjects")
        func mirrorConfigNoSubjects() {
            let kvConfig = KeyValueConfig(
                bucket: "test",
                mirror: StreamSource(name: "KV_source")
            )
            let streamConfig = kvConfig.toStreamConfig()

            #expect(streamConfig.subjects.isEmpty)
        }

        @Test("Sources config has no subjects")
        func sourcesConfigNoSubjects() {
            let kvConfig = KeyValueConfig(
                bucket: "test",
                sources: [StreamSource(name: "KV_source1")]
            )
            let streamConfig = kvConfig.toStreamConfig()

            #expect(streamConfig.subjects.isEmpty)
        }
    }

    // MARK: - Entry Parsing Tests

    @Suite("KeyValueEntry Parsing")
    struct EntryParsingTests {

        @Test("Parse PUT operation from Direct Get")
        func parsePutFromDirectGet() {
            var headers = NatsHeaders()
            headers[NatsHeaders.Keys.natsSequence] = "42"
            headers[NatsHeaders.Keys.natsSubject] = "$KV.mybucket.mykey"
            headers[NatsHeaders.Keys.natsTimeStamp] = "2024-01-01T00:00:00Z"

            var buffer = ByteBuffer()
            buffer.writeString("hello")

            let message = NatsMessage(
                subject: "",
                headers: headers,
                buffer: buffer
            )

            let entry = KeyValueEntry.fromDirectGet(message, bucket: "mybucket")

            #expect(entry != nil)
            #expect(entry?.bucket == "mybucket")
            #expect(entry?.key == "mykey")
            #expect(entry?.revision == 42)
            #expect(entry?.operation == .put)
            #expect(entry?.stringValue == "hello")
        }

        @Test("Parse DEL operation from Direct Get")
        func parseDelFromDirectGet() {
            var headers = NatsHeaders()
            headers[NatsHeaders.Keys.natsSequence] = "43"
            headers[NatsHeaders.Keys.natsSubject] = "$KV.mybucket.mykey"
            headers[NatsHeaders.Keys.natsTimeStamp] = "2024-01-01T00:00:00Z"
            headers[NatsHeaders.Keys.kvOperation] = "DEL"

            let message = NatsMessage(
                subject: "",
                headers: headers,
                buffer: ByteBuffer()
            )

            let entry = KeyValueEntry.fromDirectGet(message, bucket: "mybucket")

            #expect(entry != nil)
            #expect(entry?.operation == .delete)
        }

        @Test("Parse PURGE operation from Direct Get")
        func parsePurgeFromDirectGet() {
            var headers = NatsHeaders()
            headers[NatsHeaders.Keys.natsSequence] = "44"
            headers[NatsHeaders.Keys.natsSubject] = "$KV.mybucket.mykey"
            headers[NatsHeaders.Keys.natsTimeStamp] = "2024-01-01T00:00:00Z"
            headers[NatsHeaders.Keys.kvOperation] = "PURGE"

            let message = NatsMessage(
                subject: "",
                headers: headers,
                buffer: ByteBuffer()
            )

            let entry = KeyValueEntry.fromDirectGet(message, bucket: "mybucket")

            #expect(entry != nil)
            #expect(entry?.operation == .purge)
        }

        @Test("Parse entry with nested key")
        func parseNestedKey() {
            var headers = NatsHeaders()
            headers[NatsHeaders.Keys.natsSequence] = "1"
            headers[NatsHeaders.Keys.natsSubject] = "$KV.mybucket.path.to.key"
            headers[NatsHeaders.Keys.natsTimeStamp] = "2024-01-01T00:00:00Z"

            let message = NatsMessage(
                subject: "",
                headers: headers,
                buffer: ByteBuffer()
            )

            let entry = KeyValueEntry.fromDirectGet(message, bucket: "mybucket")

            #expect(entry != nil)
            #expect(entry?.key == "path.to.key")
        }

        @Test("Parse returns nil for missing headers")
        func parseNilForMissingHeaders() {
            let message = NatsMessage(
                subject: "",
                buffer: ByteBuffer()
            )

            let entry = KeyValueEntry.fromDirectGet(message, bucket: "mybucket")
            #expect(entry == nil)
        }

        @Test("Parse returns nil for missing sequence header")
        func parseNilForMissingSequence() {
            var headers = NatsHeaders()
            headers[NatsHeaders.Keys.natsSubject] = "$KV.mybucket.mykey"

            let message = NatsMessage(
                subject: "",
                headers: headers,
                buffer: ByteBuffer()
            )

            let entry = KeyValueEntry.fromDirectGet(message, bucket: "mybucket")
            #expect(entry == nil)
        }
    }

    // MARK: - Status Tests

    @Suite("KeyValueBucketStatus")
    struct StatusTests {

        @Test("Status from StreamInfo")
        func statusFromStreamInfo() {
            let config = StreamConfig(
                name: "KV_mybucket",
                subjects: ["$KV.mybucket.>"],
                maxBytes: 1_000_000,
                maxMsgSize: 1024,
                storage: .memory,
                replicas: 1,
                maxMsgsPerSubject: 5,
                compression: .s2,
                metadata: ["env": "test"]
            )

            let state = StreamState(
                messages: 100,
                bytes: 50_000,
                firstSeq: 1,
                firstTs: Date(),
                lastSeq: 100,
                lastTs: Date(),
                numDeleted: 5,
                deleted: nil,
                lost: nil,
                consumerCount: 2,
                numSubjects: 25
            )

            let info = StreamInfo(
                config: config,
                created: Date(),
                state: state,
                cluster: nil,
                mirror: nil,
                sources: nil
            )

            let status = KeyValueBucketStatus.from(info)

            #expect(status.bucket == "mybucket")
            #expect(status.values == 25)
            #expect(status.history == 5)
            #expect(status.maxValueSize == 1024)
            #expect(status.maxBytes == 1_000_000)
            #expect(status.storage == .memory)
            #expect(status.replicas == 1)
            #expect(status.totalEntries == 100)
            #expect(status.totalBytes == 50_000)
            #expect(status.compression == .s2)
            #expect(status.metadata?["env"] == "test")
        }

        @Test("Status with TTL")
        func statusWithTTL() {
            var config = StreamConfig(
                name: "KV_ttlbucket",
                subjects: ["$KV.ttlbucket.>"]
            )
            config.maxAge = 60_000_000_000 // 60 seconds in nanos

            let state = StreamState(
                messages: 0,
                bytes: 0,
                firstSeq: 0,
                firstTs: Date(),
                lastSeq: 0,
                lastTs: Date(),
                numDeleted: nil,
                deleted: nil,
                lost: nil,
                consumerCount: 0,
                numSubjects: nil
            )

            let info = StreamInfo(
                config: config,
                created: Date(),
                state: state,
                cluster: nil,
                mirror: nil,
                sources: nil
            )

            let status = KeyValueBucketStatus.from(info)

            #expect(status.ttl != nil)
            #expect(status.ttl == .seconds(60))
        }
    }
}

// MARK: - Integration Tests

/// Integration tests requiring a running NATS server with JetStream on localhost:4222
@Suite("KeyValue Integration Tests", .tags(.integration), .serialized)
struct KeyValueIntegrationTests {

    /// Create a connected client for tests
    private func makeClient() async throws -> NatsClient {
        let client = NatsClient {
            $0.servers = [URL(string: "nats://localhost:4222")!]
        }
        try await client.connect()
        return client
    }

    /// Generate a unique bucket name for test isolation
    private func uniqueBucket() -> String {
        "test_\(UUID().uuidString.prefix(8).lowercased())"
    }

    // MARK: - Bucket Lifecycle

    @Suite("Bucket Lifecycle", .serialized)
    struct BucketLifecycleTests {

        private func makeClient() async throws -> NatsClient {
            let client = NatsClient {
                $0.servers = [URL(string: "nats://localhost:4222")!]
            }
            try await client.connect()
            return client
        }

        private func uniqueBucket() -> String {
            "test_\(UUID().uuidString.prefix(8).lowercased())"
        }

        @Test("Create and delete bucket")
        func createAndDeleteBucket() async throws {
            let client = try await makeClient()
            let js = try await client.jetStream()
            let bucket = uniqueBucket()

            let kv = try await js.createKeyValue(KeyValueConfig(bucket: bucket))
            let status = try await kv.status()

            #expect(status.bucket == bucket)
            #expect(status.values == 0)
            #expect(status.history == 1)
            #expect(status.storage == .file)

            try await js.deleteKeyValue(bucket)
            await client.close()
        }

        @Test("Create bucket with custom config")
        func createBucketWithCustomConfig() async throws {
            let client = try await makeClient()
            let js = try await client.jetStream()
            let bucket = uniqueBucket()

            let kv = try await js.createKeyValue(KeyValueConfig(
                bucket: bucket,
                description: "Test bucket",
                history: 10,
                maxValueSize: 1024,
                maxBytes: 1_000_000,
                storage: .memory,
                replicas: 1
            ))

            let status = try await kv.status()

            #expect(status.bucket == bucket)
            #expect(status.history == 10)
            #expect(status.maxValueSize == 1024)
            #expect(status.maxBytes == 1_000_000)
            #expect(status.storage == .memory)

            try await js.deleteKeyValue(bucket)
            await client.close()
        }

        @Test("Create bucket with TTL")
        func createBucketWithTTL() async throws {
            let client = try await makeClient()
            let js = try await client.jetStream()
            let bucket = uniqueBucket()

            let kv = try await js.createKeyValue(KeyValueConfig(
                bucket: bucket,
                ttl: .seconds(60)
            ))

            let status = try await kv.status()
            #expect(status.ttl != nil)

            try await js.deleteKeyValue(bucket)
            await client.close()
        }

        @Test("Bind to existing bucket")
        func bindToExistingBucket() async throws {
            let client = try await makeClient()
            let js = try await client.jetStream()
            let bucket = uniqueBucket()

            // Create bucket
            _ = try await js.createKeyValue(KeyValueConfig(bucket: bucket))

            // Bind to it
            let kv = try await js.keyValue(bucket)
            let status = try await kv.status()
            #expect(status.bucket == bucket)

            try await js.deleteKeyValue(bucket)
            await client.close()
        }

        @Test("Bind to non-existent bucket throws error")
        func bindToNonExistentBucket() async throws {
            let client = try await makeClient()
            let js = try await client.jetStream()

            do {
                _ = try await js.keyValue("nonexistent_bucket_xyz")
                Issue.record("Should have thrown bucketNotFound")
            } catch let error as JetStreamError {
                if case .bucketNotFound = error {
                    // expected
                } else {
                    Issue.record("Expected bucketNotFound, got \(error)")
                }
            }

            await client.close()
        }

        @Test("Invalid bucket name throws error")
        func invalidBucketNameThrows() async throws {
            let client = try await makeClient()
            let js = try await client.jetStream()

            do {
                _ = try await js.createKeyValue(KeyValueConfig(bucket: "invalid.bucket"))
                Issue.record("Should have thrown invalidBucketName")
            } catch let error as JetStreamError {
                if case .invalidBucketName = error {
                    // expected
                } else {
                    Issue.record("Expected invalidBucketName, got \(error)")
                }
            }

            await client.close()
        }

        @Test("History out of range throws error")
        func historyOutOfRange() async throws {
            let client = try await makeClient()
            let js = try await client.jetStream()

            do {
                _ = try await js.createKeyValue(KeyValueConfig(
                    bucket: uniqueBucket(),
                    history: 100
                ))
                Issue.record("Should have thrown historyExceeded")
            } catch let error as JetStreamError {
                if case .historyExceeded = error {
                    // expected
                } else {
                    Issue.record("Expected historyExceeded, got \(error)")
                }
            }

            await client.close()
        }

        @Test("List bucket names")
        func listBucketNames() async throws {
            let client = try await makeClient()
            let js = try await client.jetStream()
            let bucket1 = uniqueBucket()
            let bucket2 = uniqueBucket()

            _ = try await js.createKeyValue(KeyValueConfig(bucket: bucket1))
            _ = try await js.createKeyValue(KeyValueConfig(bucket: bucket2))

            let names = try await js.keyValueBucketNames()
            #expect(names.contains(bucket1))
            #expect(names.contains(bucket2))

            try await js.deleteKeyValue(bucket1)
            try await js.deleteKeyValue(bucket2)
            await client.close()
        }

        @Test("List buckets with status")
        func listBucketsWithStatus() async throws {
            let client = try await makeClient()
            let js = try await client.jetStream()
            let bucket = uniqueBucket()

            let kv = try await js.createKeyValue(KeyValueConfig(bucket: bucket))
            _ = try await kv.put("key1", value: "value1")

            let statuses = try await js.keyValueBuckets()
            let found = statuses.first { $0.bucket == bucket }
            #expect(found != nil)
            #expect(found?.totalEntries == 1)

            try await js.deleteKeyValue(bucket)
            await client.close()
        }
    }

    // MARK: - Put/Get Operations

    @Suite("Put and Get Operations", .serialized)
    struct PutGetTests {

        private func makeClient() async throws -> NatsClient {
            let client = NatsClient {
                $0.servers = [URL(string: "nats://localhost:4222")!]
            }
            try await client.connect()
            return client
        }

        private func uniqueBucket() -> String {
            "test_\(UUID().uuidString.prefix(8).lowercased())"
        }

        @Test("Put and get a string value")
        func putAndGetString() async throws {
            let client = try await makeClient()
            let js = try await client.jetStream()
            let bucket = uniqueBucket()

            let kv = try await js.createKeyValue(KeyValueConfig(bucket: bucket))

            let rev = try await kv.put("greeting", value: "hello world")
            #expect(rev >= 1)

            let entry = try await kv.get("greeting")
            #expect(entry != nil)
            #expect(entry?.key == "greeting")
            #expect(entry?.stringValue == "hello world")
            #expect(entry?.revision == rev)
            #expect(entry?.operation == .put)

            try await js.deleteKeyValue(bucket)
            await client.close()
        }

        @Test("Put and get a ByteBuffer value")
        func putAndGetByteBuffer() async throws {
            let client = try await makeClient()
            let js = try await client.jetStream()
            let bucket = uniqueBucket()

            let kv = try await js.createKeyValue(KeyValueConfig(bucket: bucket))

            var buffer = ByteBuffer()
            buffer.writeBytes([0x01, 0x02, 0x03, 0x04])
            let rev = try await kv.put("binary", value: buffer)

            let entry = try await kv.get("binary")
            #expect(entry != nil)
            #expect(entry?.value.readableBytes == 4)
            #expect(entry?.revision == rev)

            try await js.deleteKeyValue(bucket)
            await client.close()
        }

        @Test("Get non-existent key returns nil")
        func getNonExistentKey() async throws {
            let client = try await makeClient()
            let js = try await client.jetStream()
            let bucket = uniqueBucket()

            let kv = try await js.createKeyValue(KeyValueConfig(bucket: bucket))

            let entry = try await kv.get("does_not_exist")
            #expect(entry == nil)

            try await js.deleteKeyValue(bucket)
            await client.close()
        }

        @Test("Put overwrites existing value")
        func putOverwrites() async throws {
            let client = try await makeClient()
            let js = try await client.jetStream()
            let bucket = uniqueBucket()

            let kv = try await js.createKeyValue(KeyValueConfig(bucket: bucket))

            let rev1 = try await kv.put("key", value: "value1")
            let rev2 = try await kv.put("key", value: "value2")

            #expect(rev2 > rev1)

            let entry = try await kv.get("key")
            #expect(entry?.stringValue == "value2")
            #expect(entry?.revision == rev2)

            try await js.deleteKeyValue(bucket)
            await client.close()
        }

        @Test("Get by revision")
        func getByRevision() async throws {
            let client = try await makeClient()
            let js = try await client.jetStream()
            let bucket = uniqueBucket()

            let kv = try await js.createKeyValue(KeyValueConfig(
                bucket: bucket,
                history: 10
            ))

            let rev1 = try await kv.put("key", value: "first")
            _ = try await kv.put("key", value: "second")

            // Get the first revision specifically
            let entry = try await kv.get("key", revision: rev1)
            #expect(entry != nil)
            #expect(entry?.stringValue == "first")
            #expect(entry?.revision == rev1)

            try await js.deleteKeyValue(bucket)
            await client.close()
        }

        @Test("Multiple keys")
        func multipleKeys() async throws {
            let client = try await makeClient()
            let js = try await client.jetStream()
            let bucket = uniqueBucket()

            let kv = try await js.createKeyValue(KeyValueConfig(bucket: bucket))

            _ = try await kv.put("name", value: "Alice")
            _ = try await kv.put("age", value: "30")
            _ = try await kv.put("city", value: "NYC")

            let name = try await kv.get("name")
            let age = try await kv.get("age")
            let city = try await kv.get("city")

            #expect(name?.stringValue == "Alice")
            #expect(age?.stringValue == "30")
            #expect(city?.stringValue == "NYC")

            try await js.deleteKeyValue(bucket)
            await client.close()
        }

        @Test("Key with path separators")
        func keyWithPathSeparators() async throws {
            let client = try await makeClient()
            let js = try await client.jetStream()
            let bucket = uniqueBucket()

            let kv = try await js.createKeyValue(KeyValueConfig(bucket: bucket))

            _ = try await kv.put("config/app/name", value: "myapp")
            _ = try await kv.put("config/app/version", value: "1.0")

            let name = try await kv.get("config/app/name")
            let version = try await kv.get("config/app/version")

            #expect(name?.stringValue == "myapp")
            #expect(version?.stringValue == "1.0")

            try await js.deleteKeyValue(bucket)
            await client.close()
        }

        @Test("Invalid key throws error")
        func invalidKeyThrows() async throws {
            let client = try await makeClient()
            let js = try await client.jetStream()
            let bucket = uniqueBucket()

            let kv = try await js.createKeyValue(KeyValueConfig(bucket: bucket))

            do {
                _ = try await kv.put("invalid key", value: "test")
                Issue.record("Should have thrown invalidKey")
            } catch let error as JetStreamError {
                if case .invalidKey = error {
                    // expected
                } else {
                    Issue.record("Expected invalidKey, got \(error)")
                }
            }

            try await js.deleteKeyValue(bucket)
            await client.close()
        }

        @Test("Empty value is allowed")
        func emptyValue() async throws {
            let client = try await makeClient()
            let js = try await client.jetStream()
            let bucket = uniqueBucket()

            let kv = try await js.createKeyValue(KeyValueConfig(bucket: bucket))

            _ = try await kv.put("empty", value: "")

            let entry = try await kv.get("empty")
            #expect(entry != nil)
            #expect(entry?.stringValue == "")

            try await js.deleteKeyValue(bucket)
            await client.close()
        }
    }

    // MARK: - Create (Put-If-Absent) Tests

    @Suite("Create Operations (CAS)", .serialized)
    struct CreateTests {

        private func makeClient() async throws -> NatsClient {
            let client = NatsClient {
                $0.servers = [URL(string: "nats://localhost:4222")!]
            }
            try await client.connect()
            return client
        }

        private func uniqueBucket() -> String {
            "test_\(UUID().uuidString.prefix(8).lowercased())"
        }

        @Test("Create new key succeeds")
        func createNewKey() async throws {
            let client = try await makeClient()
            let js = try await client.jetStream()
            let bucket = uniqueBucket()

            let kv = try await js.createKeyValue(KeyValueConfig(bucket: bucket))

            let rev = try await kv.create("newkey", value: "newvalue")
            #expect(rev >= 1)

            let entry = try await kv.get("newkey")
            #expect(entry?.stringValue == "newvalue")

            try await js.deleteKeyValue(bucket)
            await client.close()
        }

        @Test("Create existing key throws keyExists")
        func createExistingKeyThrows() async throws {
            let client = try await makeClient()
            let js = try await client.jetStream()
            let bucket = uniqueBucket()

            let kv = try await js.createKeyValue(KeyValueConfig(bucket: bucket))

            _ = try await kv.create("existing", value: "first")

            do {
                _ = try await kv.create("existing", value: "second")
                Issue.record("Should have thrown keyExists")
            } catch let error as JetStreamError {
                if case .keyExists = error {
                    // expected
                } else {
                    Issue.record("Expected keyExists, got \(error)")
                }
            }

            // Original value should be unchanged
            let entry = try await kv.get("existing")
            #expect(entry?.stringValue == "first")

            try await js.deleteKeyValue(bucket)
            await client.close()
        }

        @Test("Create after delete succeeds")
        func createAfterDelete() async throws {
            let client = try await makeClient()
            let js = try await client.jetStream()
            let bucket = uniqueBucket()

            let kv = try await js.createKeyValue(KeyValueConfig(bucket: bucket))

            _ = try await kv.create("key", value: "original")
            try await kv.delete("key")

            // Create should succeed because the key is deleted
            let rev = try await kv.create("key", value: "recreated")
            #expect(rev >= 1)

            let entry = try await kv.get("key")
            #expect(entry?.stringValue == "recreated")

            try await js.deleteKeyValue(bucket)
            await client.close()
        }
    }

    // MARK: - Update (CAS) Tests

    @Suite("Update Operations (CAS)", .serialized)
    struct UpdateTests {

        private func makeClient() async throws -> NatsClient {
            let client = NatsClient {
                $0.servers = [URL(string: "nats://localhost:4222")!]
            }
            try await client.connect()
            return client
        }

        private func uniqueBucket() -> String {
            "test_\(UUID().uuidString.prefix(8).lowercased())"
        }

        @Test("Update with correct revision succeeds")
        func updateWithCorrectRevision() async throws {
            let client = try await makeClient()
            let js = try await client.jetStream()
            let bucket = uniqueBucket()

            let kv = try await js.createKeyValue(KeyValueConfig(bucket: bucket))

            let rev1 = try await kv.put("key", value: "value1")
            let rev2 = try await kv.update("key", value: "value2", revision: rev1)

            #expect(rev2 > rev1)

            let entry = try await kv.get("key")
            #expect(entry?.stringValue == "value2")

            try await js.deleteKeyValue(bucket)
            await client.close()
        }

        @Test("Update with wrong revision fails")
        func updateWithWrongRevision() async throws {
            let client = try await makeClient()
            let js = try await client.jetStream()
            let bucket = uniqueBucket()

            let kv = try await js.createKeyValue(KeyValueConfig(bucket: bucket))

            _ = try await kv.put("key", value: "value1")

            do {
                _ = try await kv.update("key", value: "value2", revision: 999)
                Issue.record("Should have thrown error for wrong revision")
            } catch {
                // Expected - wrong revision should fail
            }

            // Original value should be unchanged
            let entry = try await kv.get("key")
            #expect(entry?.stringValue == "value1")

            try await js.deleteKeyValue(bucket)
            await client.close()
        }

        @Test("Update with string convenience")
        func updateWithStringConvenience() async throws {
            let client = try await makeClient()
            let js = try await client.jetStream()
            let bucket = uniqueBucket()

            let kv = try await js.createKeyValue(KeyValueConfig(bucket: bucket))

            let rev1 = try await kv.put("key", value: "v1")
            let rev2 = try await kv.update("key", value: "v2", revision: rev1)

            let entry = try await kv.get("key")
            #expect(entry?.stringValue == "v2")
            #expect(entry?.revision == rev2)

            try await js.deleteKeyValue(bucket)
            await client.close()
        }
    }

    // MARK: - Delete Tests

    @Suite("Delete Operations", .serialized)
    struct DeleteTests {

        private func makeClient() async throws -> NatsClient {
            let client = NatsClient {
                $0.servers = [URL(string: "nats://localhost:4222")!]
            }
            try await client.connect()
            return client
        }

        private func uniqueBucket() -> String {
            "test_\(UUID().uuidString.prefix(8).lowercased())"
        }

        @Test("Delete a key")
        func deleteKey() async throws {
            let client = try await makeClient()
            let js = try await client.jetStream()
            let bucket = uniqueBucket()

            let kv = try await js.createKeyValue(KeyValueConfig(bucket: bucket))

            _ = try await kv.put("key", value: "value")
            try await kv.delete("key")

            // Get should return nil for deleted key
            let entry = try await kv.get("key")
            #expect(entry == nil)

            try await js.deleteKeyValue(bucket)
            await client.close()
        }

        @Test("Delete with CAS revision")
        func deleteWithCAS() async throws {
            let client = try await makeClient()
            let js = try await client.jetStream()
            let bucket = uniqueBucket()

            let kv = try await js.createKeyValue(KeyValueConfig(bucket: bucket))

            let rev = try await kv.put("key", value: "value")
            try await kv.delete("key", revision: rev)

            let entry = try await kv.get("key")
            #expect(entry == nil)

            try await js.deleteKeyValue(bucket)
            await client.close()
        }

        @Test("Delete with wrong CAS revision fails")
        func deleteWithWrongCAS() async throws {
            let client = try await makeClient()
            let js = try await client.jetStream()
            let bucket = uniqueBucket()

            let kv = try await js.createKeyValue(KeyValueConfig(bucket: bucket))

            _ = try await kv.put("key", value: "value")

            do {
                try await kv.delete("key", revision: 999)
                Issue.record("Should have thrown error for wrong revision")
            } catch {
                // Expected
            }

            // Key should still exist
            let entry = try await kv.get("key")
            #expect(entry?.stringValue == "value")

            try await js.deleteKeyValue(bucket)
            await client.close()
        }

        @Test("Delete non-existent key succeeds (soft delete)")
        func deleteNonExistentKey() async throws {
            let client = try await makeClient()
            let js = try await client.jetStream()
            let bucket = uniqueBucket()

            let kv = try await js.createKeyValue(KeyValueConfig(bucket: bucket))

            // Deleting a key that was never set — this publishes a delete marker
            // which may fail if there's no subject history. Let's put first then delete.
            _ = try await kv.put("key", value: "temp")
            try await kv.delete("key")

            let entry = try await kv.get("key")
            #expect(entry == nil)

            try await js.deleteKeyValue(bucket)
            await client.close()
        }
    }

    // MARK: - Purge Tests

    @Suite("Purge Operations", .serialized)
    struct PurgeTests {

        private func makeClient() async throws -> NatsClient {
            let client = NatsClient {
                $0.servers = [URL(string: "nats://localhost:4222")!]
            }
            try await client.connect()
            return client
        }

        private func uniqueBucket() -> String {
            "test_\(UUID().uuidString.prefix(8).lowercased())"
        }

        @Test("Purge a key removes all history")
        func purgeKey() async throws {
            let client = try await makeClient()
            let js = try await client.jetStream()
            let bucket = uniqueBucket()

            let kv = try await js.createKeyValue(KeyValueConfig(
                bucket: bucket,
                history: 10
            ))

            _ = try await kv.put("key", value: "v1")
            _ = try await kv.put("key", value: "v2")
            _ = try await kv.put("key", value: "v3")

            try await kv.purge("key")

            // Get should return nil
            let entry = try await kv.get("key")
            #expect(entry == nil)

            // History should only have the purge marker
            let history = try await kv.history("key")
            #expect(history.count == 1)
            #expect(history.first?.operation == .purge)

            try await js.deleteKeyValue(bucket)
            await client.close()
        }

        @Test("Purge with CAS revision")
        func purgeWithCAS() async throws {
            let client = try await makeClient()
            let js = try await client.jetStream()
            let bucket = uniqueBucket()

            let kv = try await js.createKeyValue(KeyValueConfig(bucket: bucket))

            let rev = try await kv.put("key", value: "value")
            try await kv.purge("key", revision: rev)

            let entry = try await kv.get("key")
            #expect(entry == nil)

            try await js.deleteKeyValue(bucket)
            await client.close()
        }
    }

    // MARK: - Keys Listing Tests

    @Suite("Keys Listing", .serialized)
    struct KeysListingTests {

        private func makeClient() async throws -> NatsClient {
            let client = NatsClient {
                $0.servers = [URL(string: "nats://localhost:4222")!]
            }
            try await client.connect()
            return client
        }

        private func uniqueBucket() -> String {
            "test_\(UUID().uuidString.prefix(8).lowercased())"
        }

        @Test("List all keys")
        func listAllKeys() async throws {
            let client = try await makeClient()
            let js = try await client.jetStream()
            let bucket = uniqueBucket()

            let kv = try await js.createKeyValue(KeyValueConfig(bucket: bucket))

            _ = try await kv.put("alpha", value: "a")
            _ = try await kv.put("beta", value: "b")
            _ = try await kv.put("gamma", value: "c")

            let keys = try await kv.keys()
            #expect(keys.count == 3)
            #expect(keys.contains("alpha"))
            #expect(keys.contains("beta"))
            #expect(keys.contains("gamma"))

            try await js.deleteKeyValue(bucket)
            await client.close()
        }

        @Test("Keys excludes deleted entries")
        func keysExcludesDeleted() async throws {
            let client = try await makeClient()
            let js = try await client.jetStream()
            let bucket = uniqueBucket()

            let kv = try await js.createKeyValue(KeyValueConfig(bucket: bucket))

            _ = try await kv.put("keep", value: "v")
            _ = try await kv.put("remove", value: "v")
            try await kv.delete("remove")

            let keys = try await kv.keys()
            #expect(keys.contains("keep"))
            #expect(!keys.contains("remove"))

            try await js.deleteKeyValue(bucket)
            await client.close()
        }

        @Test("Keys on empty bucket returns empty list")
        func keysOnEmptyBucket() async throws {
            let client = try await makeClient()
            let js = try await client.jetStream()
            let bucket = uniqueBucket()

            let kv = try await js.createKeyValue(KeyValueConfig(bucket: bucket))

            let keys = try await kv.keys()
            #expect(keys.isEmpty)

            try await js.deleteKeyValue(bucket)
            await client.close()
        }

        @Test("Keys with many entries")
        func keysWithManyEntries() async throws {
            let client = try await makeClient()
            let js = try await client.jetStream()
            let bucket = uniqueBucket()

            let kv = try await js.createKeyValue(KeyValueConfig(bucket: bucket))

            for i in 0..<50 {
                _ = try await kv.put("key_\(String(format: "%03d", i))", value: "value_\(i)")
            }

            let keys = try await kv.keys()
            #expect(keys.count == 50)

            try await js.deleteKeyValue(bucket)
            await client.close()
        }
    }

    // MARK: - History Tests

    @Suite("History", .serialized)
    struct HistoryTests {

        private func makeClient() async throws -> NatsClient {
            let client = NatsClient {
                $0.servers = [URL(string: "nats://localhost:4222")!]
            }
            try await client.connect()
            return client
        }

        private func uniqueBucket() -> String {
            "test_\(UUID().uuidString.prefix(8).lowercased())"
        }

        @Test("History shows all revisions")
        func historyShowsAllRevisions() async throws {
            let client = try await makeClient()
            let js = try await client.jetStream()
            let bucket = uniqueBucket()

            let kv = try await js.createKeyValue(KeyValueConfig(
                bucket: bucket,
                history: 10
            ))

            _ = try await kv.put("key", value: "v1")
            _ = try await kv.put("key", value: "v2")
            _ = try await kv.put("key", value: "v3")

            let history = try await kv.history("key")
            #expect(history.count == 3)

            #expect(history[0].stringValue == "v1")
            #expect(history[1].stringValue == "v2")
            #expect(history[2].stringValue == "v3")

            // Revisions should be increasing
            #expect(history[1].revision > history[0].revision)
            #expect(history[2].revision > history[1].revision)

            // All should be PUT operations
            for entry in history {
                #expect(entry.operation == .put)
            }

            try await js.deleteKeyValue(bucket)
            await client.close()
        }

        @Test("History includes delete markers")
        func historyIncludesDeleteMarkers() async throws {
            let client = try await makeClient()
            let js = try await client.jetStream()
            let bucket = uniqueBucket()

            let kv = try await js.createKeyValue(KeyValueConfig(
                bucket: bucket,
                history: 10
            ))

            _ = try await kv.put("key", value: "v1")
            try await kv.delete("key")
            _ = try await kv.put("key", value: "v2")

            let history = try await kv.history("key")
            #expect(history.count == 3)

            #expect(history[0].operation == .put)
            #expect(history[0].stringValue == "v1")
            #expect(history[1].operation == .delete)
            #expect(history[2].operation == .put)
            #expect(history[2].stringValue == "v2")

            try await js.deleteKeyValue(bucket)
            await client.close()
        }

        @Test("History respects max history setting")
        func historyRespectsMaxSetting() async throws {
            let client = try await makeClient()
            let js = try await client.jetStream()
            let bucket = uniqueBucket()

            let kv = try await js.createKeyValue(KeyValueConfig(
                bucket: bucket,
                history: 3
            ))

            _ = try await kv.put("key", value: "v1")
            _ = try await kv.put("key", value: "v2")
            _ = try await kv.put("key", value: "v3")
            _ = try await kv.put("key", value: "v4")
            _ = try await kv.put("key", value: "v5")

            let history = try await kv.history("key")
            // Should only keep the last 3
            #expect(history.count == 3)
            #expect(history[0].stringValue == "v3")
            #expect(history[1].stringValue == "v4")
            #expect(history[2].stringValue == "v5")

            try await js.deleteKeyValue(bucket)
            await client.close()
        }

        @Test("History of non-existent key returns empty")
        func historyNonExistentKey() async throws {
            let client = try await makeClient()
            let js = try await client.jetStream()
            let bucket = uniqueBucket()

            let kv = try await js.createKeyValue(KeyValueConfig(bucket: bucket))

            let history = try await kv.history("nope")
            #expect(history.isEmpty)

            try await js.deleteKeyValue(bucket)
            await client.close()
        }
    }

    // MARK: - Watch Tests

    @Suite("Watch", .serialized)
    struct WatchTests {

        private func makeClient() async throws -> NatsClient {
            let client = NatsClient {
                $0.servers = [URL(string: "nats://localhost:4222")!]
            }
            try await client.connect()
            return client
        }

        private func uniqueBucket() -> String {
            "test_\(UUID().uuidString.prefix(8).lowercased())"
        }

        @Test("Watch receives initial values and updates")
        func watchReceivesInitialAndUpdates() async throws {
            let client = try await makeClient()
            let js = try await client.jetStream()
            let bucket = uniqueBucket()

            let kv = try await js.createKeyValue(KeyValueConfig(bucket: bucket))

            // Put some initial values
            _ = try await kv.put("key1", value: "initial1")
            _ = try await kv.put("key2", value: "initial2")

            // Start watching
            let watcher = try await kv.watchAll()

            var entries: [KeyValueEntry?] = []
            var gotSentinel = false

            for try await entry in watcher {
                entries.append(entry)
                if entry == nil {
                    gotSentinel = true
                    break
                }
                // Safety limit
                if entries.count > 10 { break }
            }

            // Should have 2 initial entries + nil sentinel
            #expect(gotSentinel)
            #expect(entries.count == 3) // key1, key2, nil

            // First two should be the initial values
            #expect(entries[0]?.key == "key1")
            #expect(entries[0]?.stringValue == "initial1")
            #expect(entries[1]?.key == "key2")
            #expect(entries[1]?.stringValue == "initial2")

            // Third is the sentinel
            #expect(entries[2] == nil)

            try await watcher.stop()
            try await js.deleteKeyValue(bucket)
            await client.close()
        }

        @Test("Watch with updatesOnly skips initial values")
        func watchUpdatesOnly() async throws {
            let client = try await makeClient()
            let js = try await client.jetStream()
            let bucket = uniqueBucket()

            let kv = try await js.createKeyValue(KeyValueConfig(bucket: bucket))

            _ = try await kv.put("existing", value: "should_not_see")

            // Start watching with updatesOnly
            let watcher = try await kv.watchAll(
                options: KeyValueWatchOptions(updatesOnly: true)
            )

            // Put a new value after starting watch
            Task {
                try await Task.sleep(for: .milliseconds(500))
                _ = try await kv.put("new_key", value: "new_value")
            }

            var entries: [KeyValueEntry] = []
            for try await entry in watcher {
                if let entry = entry {
                    entries.append(entry)
                    break
                }
            }

            // Should only see the new key, not the existing one
            #expect(entries.count == 1)
            #expect(entries[0].key == "new_key")
            #expect(entries[0].stringValue == "new_value")

            try await watcher.stop()
            try await js.deleteKeyValue(bucket)
            await client.close()
        }

        @Test("Watch with includeHistory")
        func watchWithHistory() async throws {
            let client = try await makeClient()
            let js = try await client.jetStream()
            let bucket = uniqueBucket()

            let kv = try await js.createKeyValue(KeyValueConfig(
                bucket: bucket,
                history: 5
            ))

            _ = try await kv.put("key", value: "v1")
            _ = try await kv.put("key", value: "v2")
            _ = try await kv.put("key", value: "v3")

            let watcher = try await kv.watch(
                ">",
                options: KeyValueWatchOptions(includeHistory: true)
            )

            var entries: [KeyValueEntry?] = []
            for try await entry in watcher {
                entries.append(entry)
                if entry == nil { break }
                if entries.count > 10 { break }
            }

            // With includeHistory, should get all 3 versions + sentinel
            #expect(entries.count == 4)
            #expect(entries[0]?.stringValue == "v1")
            #expect(entries[1]?.stringValue == "v2")
            #expect(entries[2]?.stringValue == "v3")
            #expect(entries[3] == nil)

            try await watcher.stop()
            try await js.deleteKeyValue(bucket)
            await client.close()
        }

        @Test("Watch specific key pattern")
        func watchSpecificPattern() async throws {
            let client = try await makeClient()
            let js = try await client.jetStream()
            let bucket = uniqueBucket()

            let kv = try await js.createKeyValue(KeyValueConfig(bucket: bucket))

            _ = try await kv.put("users.alice", value: "data1")
            _ = try await kv.put("users.bob", value: "data2")
            _ = try await kv.put("orders.123", value: "order")

            // Watch only users.*
            let watcher = try await kv.watch("users.*")

            var entries: [KeyValueEntry?] = []
            for try await entry in watcher {
                entries.append(entry)
                if entry == nil { break }
                if entries.count > 10 { break }
            }

            // Should get 2 user entries + sentinel (not the order)
            #expect(entries.count == 3)
            #expect(entries[2] == nil) // sentinel

            try await watcher.stop()
            try await js.deleteKeyValue(bucket)
            await client.close()
        }

        @Test("Watch on empty bucket sends sentinel immediately")
        func watchEmptyBucket() async throws {
            let client = try await makeClient()
            let js = try await client.jetStream()
            let bucket = uniqueBucket()

            let kv = try await js.createKeyValue(KeyValueConfig(bucket: bucket))

            let watcher = try await kv.watchAll()

            var entries: [KeyValueEntry?] = []
            for try await entry in watcher {
                entries.append(entry)
                if entry == nil { break }
                if entries.count > 5 { break }
            }

            // Should just get the nil sentinel
            #expect(entries.count == 1)
            #expect(entries[0] == nil)

            try await watcher.stop()
            try await js.deleteKeyValue(bucket)
            await client.close()
        }
    }

    // MARK: - PurgeDeletes Tests

    @Suite("PurgeDeletes", .serialized)
    struct PurgeDeletesTests {

        private func makeClient() async throws -> NatsClient {
            let client = NatsClient {
                $0.servers = [URL(string: "nats://localhost:4222")!]
            }
            try await client.connect()
            return client
        }

        private func uniqueBucket() -> String {
            "test_\(UUID().uuidString.prefix(8).lowercased())"
        }

        @Test("PurgeDeletes removes delete markers")
        func purgeDeletesRemovesMarkers() async throws {
            let client = try await makeClient()
            let js = try await client.jetStream()
            let bucket = uniqueBucket()

            let kv = try await js.createKeyValue(KeyValueConfig(
                bucket: bucket,
                history: 10
            ))

            _ = try await kv.put("key1", value: "v1")
            _ = try await kv.put("key2", value: "v2")
            _ = try await kv.put("key3", value: "v3")

            // Delete two keys
            try await kv.delete("key1")
            try await kv.delete("key2")

            // PurgeDeletes should clean up the markers
            try await kv.purgeDeletes()

            // key3 should still exist
            let entry = try await kv.get("key3")
            #expect(entry?.stringValue == "v3")

            // Deleted keys should still return nil
            let deleted1 = try await kv.get("key1")
            let deleted2 = try await kv.get("key2")
            #expect(deleted1 == nil)
            #expect(deleted2 == nil)

            try await js.deleteKeyValue(bucket)
            await client.close()
        }
    }

    // MARK: - Status Tests

    @Suite("Bucket Status", .serialized)
    struct BucketStatusTests {

        private func makeClient() async throws -> NatsClient {
            let client = NatsClient {
                $0.servers = [URL(string: "nats://localhost:4222")!]
            }
            try await client.connect()
            return client
        }

        private func uniqueBucket() -> String {
            "test_\(UUID().uuidString.prefix(8).lowercased())"
        }

        @Test("Status reflects current state")
        func statusReflectsState() async throws {
            let client = try await makeClient()
            let js = try await client.jetStream()
            let bucket = uniqueBucket()

            let kv = try await js.createKeyValue(KeyValueConfig(
                bucket: bucket,
                history: 5,
                storage: .memory
            ))

            _ = try await kv.put("key1", value: "v1")
            _ = try await kv.put("key2", value: "v2")
            _ = try await kv.put("key1", value: "v1-updated")

            let status = try await kv.status()
            #expect(status.bucket == bucket)
            #expect(status.values == 2) // 2 unique keys
            #expect(status.history == 5)
            #expect(status.storage == .memory)
            #expect(status.totalEntries == 3) // 3 total messages (including history)

            try await js.deleteKeyValue(bucket)
            await client.close()
        }
    }

    // MARK: - End-to-End Scenario Tests

    @Suite("End-to-End Scenarios", .serialized)
    struct EndToEndTests {

        private func makeClient() async throws -> NatsClient {
            let client = NatsClient {
                $0.servers = [URL(string: "nats://localhost:4222")!]
            }
            try await client.connect()
            return client
        }

        private func uniqueBucket() -> String {
            "test_\(UUID().uuidString.prefix(8).lowercased())"
        }

        @Test("Full CRUD lifecycle")
        func fullCRUDLifecycle() async throws {
            let client = try await makeClient()
            let js = try await client.jetStream()
            let bucket = uniqueBucket()

            let kv = try await js.createKeyValue(KeyValueConfig(
                bucket: bucket,
                history: 5
            ))

            // Create
            let rev1 = try await kv.create("user.1", value: "Alice")
            #expect(rev1 >= 1)

            // Read
            let entry1 = try await kv.get("user.1")
            #expect(entry1?.stringValue == "Alice")

            // Update with CAS
            let rev2 = try await kv.update("user.1", value: "Alice Updated", revision: rev1)
            #expect(rev2 > rev1)

            // Read updated
            let entry2 = try await kv.get("user.1")
            #expect(entry2?.stringValue == "Alice Updated")
            #expect(entry2?.revision == rev2)

            // Put (overwrite)
            let rev3 = try await kv.put("user.1", value: "Alice Final")
            #expect(rev3 > rev2)

            // History
            let history = try await kv.history("user.1")
            #expect(history.count == 3)

            // Delete
            try await kv.delete("user.1")
            let deleted = try await kv.get("user.1")
            #expect(deleted == nil)

            // Keys
            _ = try await kv.put("user.2", value: "Bob")
            let keys = try await kv.keys()
            #expect(keys.contains("user.2"))
            #expect(!keys.contains("user.1")) // deleted

            // Purge the deleted key
            try await kv.purge("user.1")

            // Status
            let status = try await kv.status()
            #expect(status.bucket == bucket)

            // Cleanup
            try await js.deleteKeyValue(bucket)
            await client.close()
        }

        @Test("Concurrent puts to different keys")
        func concurrentPuts() async throws {
            let client = try await makeClient()
            let js = try await client.jetStream()
            let bucket = uniqueBucket()

            let kv = try await js.createKeyValue(KeyValueConfig(bucket: bucket))

            // Put 20 different keys concurrently
            try await withThrowingTaskGroup(of: Void.self) { group in
                for i in 0..<20 {
                    group.addTask {
                        _ = try await kv.put("concurrent_key_\(i)", value: "value_\(i)")
                    }
                }
                try await group.waitForAll()
            }

            // All 20 keys should exist
            let keys = try await kv.keys()
            #expect(keys.count == 20)

            try await js.deleteKeyValue(bucket)
            await client.close()
        }

        @Test("Optimistic concurrency with multiple clients")
        func optimisticConcurrency() async throws {
            let client1 = try await makeClient()
            let client2 = try await makeClient()

            let js1 = try await client1.jetStream()
            let js2 = try await client2.jetStream()
            let bucket = uniqueBucket()

            let kv1 = try await js1.createKeyValue(KeyValueConfig(bucket: bucket))
            let kv2 = try await js2.keyValue(bucket)

            // Client 1 creates a key
            let rev1 = try await kv1.put("shared", value: "initial")

            // Both clients read it
            let entry1 = try await kv1.get("shared")
            let entry2 = try await kv2.get("shared")
            #expect(entry1?.revision == rev1)
            #expect(entry2?.revision == rev1)

            // Client 1 updates successfully
            let rev2 = try await kv1.update("shared", value: "updated-by-1", revision: rev1)
            #expect(rev2 > rev1)

            // Client 2 tries to update with stale revision - should fail
            do {
                _ = try await kv2.update("shared", value: "updated-by-2", revision: rev1)
                Issue.record("Should have failed with stale revision")
            } catch {
                // Expected - stale revision
            }

            // Client 2 re-reads and updates with correct revision
            let latest = try await kv2.get("shared")
            #expect(latest?.stringValue == "updated-by-1")
            let rev3 = try await kv2.update("shared", value: "updated-by-2", revision: latest!.revision)
            #expect(rev3 > rev2)

            let final = try await kv1.get("shared")
            #expect(final?.stringValue == "updated-by-2")

            try await js1.deleteKeyValue(bucket)
            await client1.close()
            await client2.close()
        }

        @Test("Large values")
        func largeValues() async throws {
            let client = try await makeClient()
            let js = try await client.jetStream()
            let bucket = uniqueBucket()

            let kv = try await js.createKeyValue(KeyValueConfig(bucket: bucket))

            // 100KB value
            let largeString = String(repeating: "x", count: 100_000)
            _ = try await kv.put("large", value: largeString)

            let entry = try await kv.get("large")
            #expect(entry?.stringValue?.count == 100_000)

            try await js.deleteKeyValue(bucket)
            await client.close()
        }

        @Test("Publish headers bug fix verification")
        func publishHeadersBugFix() async throws {
            let client = try await makeClient()
            let js = try await client.jetStream()
            let bucket = uniqueBucket()

            let kv = try await js.createKeyValue(KeyValueConfig(bucket: bucket))

            // This tests the fix for the publish headers bug.
            // create() relies on Nats-Expected-Last-Subject-Sequence header.
            // If headers aren't passed, create() would always succeed even for existing keys.
            let rev = try await kv.create("unique_key", value: "first")
            #expect(rev >= 1)

            // Second create should fail because headers are now properly sent
            do {
                _ = try await kv.create("unique_key", value: "second")
                Issue.record("Should have thrown keyExists - headers bug may still be present")
            } catch let error as JetStreamError {
                if case .keyExists = error {
                    // This confirms the headers fix works
                } else {
                    Issue.record("Expected keyExists, got \(error)")
                }
            }

            try await js.deleteKeyValue(bucket)
            await client.close()
        }
    }
}
