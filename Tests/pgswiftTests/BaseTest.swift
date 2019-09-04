//
//  BaseTest.swift
//  pgswiftTests
//
//  Created by Mark Lilback on 9/2/19.
//

import Foundation
import XCTest
@testable import pgswift

class BaseTest: XCTestCase {
	var connection: Connection?
	var dateFormatter: DateFormatter?
	var timestampFormatter = DateFormatter()
	
	let testHost = "localhost"
	let testPort = "5433"
	let testUser = "test"
	let testPassword = "secret"
	let testDb = "test"
	
	override func setUp() {
		guard confirmDockerRunning() else {
			logger.critical("docker container not running")
			fatalError("pgtest docker container not running. See README.md.")
		}
		
		let basicInfo = ConnectInfo(host: testHost, port: testPort, user: testUser, password: testPassword, dbname: testDb, sslMode: .allow)
		dateFormatter = DateFormatter()
		dateFormatter?.locale = Locale(identifier: "en_US_POSIX")
		dateFormatter?.setLocalizedDateFormatFromTemplate("M/d/yyyy")
		timestampFormatter.dateFormat = "yyyy-MM-dd HH:m:s.SSSSx"
		timestampFormatter.locale = Locale(identifier: "en_US_POSIX")
		timestampFormatter.timeZone = TimeZone(secondsFromGMT: 0)
		
		connection = Connection(connectInfo: basicInfo)
		try! connection?.open()
		XCTAssert(connection?.isConnected ?? false)
		let result = try! connection?.execute(query: initialSQL)
		XCTAssert(result!.wasSuccessful)
	}
	
	override func tearDown() {
		if let sql = cleanupSQL {
			try! connection?.execute(query: sql)
		}
		connection?.close()
	}
	
	func whichDocker() -> URL {
		let envPath = ProcessInfo.processInfo.environment["DOCKER_EXE"] ?? "/usr/local/bin/docker"
		return URL(fileURLWithPath: envPath)
	}
	
	func confirmDockerRunning() -> Bool {
		let proc = Process()
		proc.executableURL = whichDocker()
		proc.arguments = ["ps", "--format", "'{{.Names}}'", "--filter", "name=pgtest"]
		let outPipe = Pipe()
		proc.standardOutput = outPipe
		
		do {
			try proc.run()
			proc.waitUntilExit()
			let outputData = outPipe.fileHandleForReading.readDataToEndOfFile()
			let outputStr = String(decoding: outputData, as: UTF8.self)
			return outputStr.contains("pgtest")
		} catch {
			logger.critical("failed to run docker ps")
			return false
		}
	}
	
	var initialSQL: String { return "" }
	var cleanupSQL: String? { return nil }

}
